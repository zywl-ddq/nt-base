"""
Module:    main (nt-base 入口)
Purpose:   Trading Base Service — 持久化运行容器，承载动态注册的交易策略。
           连接 Binance Futures 沙盘(testnet)，管理行情订阅、因子计算、
           Bar分发、策略的动态注册和生命周期。

================================================================================
执行流程 (6步)
================================================================================
  1. assert_required()           — 验证环境变量和密钥完整
  2. get_pool()                  — 连接 TimescaleDB (asyncpg 连接池)
  3. build_trading_node()        — 构建 NT TradingNode (沙盘模式)
  4. DataManageActor             — 订阅 bars/ticks/L2/OI + 持久化到DB
  5. BaseStrategy(NT)            — 挂载策略容器，内部持有 OrderExecutor + RiskLoop
  6. gRPC Server                 — Unix socket + TCP 双端口启动，接收策略注册
  7. Bar 分发（monkey-patch）     — 拦截 dm_actor.on_bar 用于因子计算和信号分发

================================================================================
Bar 分发机制（核心逻辑，位于 monkey-patched dm_actor.on_bar 内）
================================================================================
  每个 bar（1s/5s/1m）:
    - 更新 price — 缓冲 OHLC 数据
  每个 1分钟 bar:
    - 计算因子（通过 grpc_servicer._factor_engine.execute_all）
    - 构建 protobuf Bar 推送给所有已注册的 trading-v2 客户端
    - 对每个策略 slot: strategy.on_bar(bar_data) — signal
    - signal.direction != 0 — OrderExecutor.execute(slot, signal, price)
    - signal.direction == 0 且 reason == "hold" — 继续持有，不做任何操作
    - signal.direction == 0 且 reason != "hold" — 标记 Bar 级退出（pending_bar_exit）

================================================================================
动态注册机制
================================================================================
  策略通过 gRPC Register() 在启动时注册。
  注册仅在内存中完成 — 不需要 DB 持久化。
  当 nt-base 重启后，策略端（trading-v2）会自动重连并重新注册。
  孤儿策略（断连超过宽限期）会自动平仓并清理。

================================================================================
关闭流程
================================================================================
  SIGTERM/SIGINT — gRPC 优雅停止 — flat_all 全部平仓 — 关闭连接池

================================================================================
日志
================================================================================
  双输出: systemd journal (stdout) + /root/nt-base/logs/nt_base.log

Author:    nt-base system
Version:   2.0.0 (dynamic registration)
"""

# —— 导入区 ---------------------------------------------------------------------

from __future__ import annotations          # 启用延迟求值注解（PEP 563）
"""nt-base — trading base service entrypoint."""

import asyncio                              # 异步运行时（async/await 协程）
import sys                                  # 系统参数，用于修改模块搜索路径
import signal                               # Unix 信号处理（SIGTERM/SIGINT）
from pathlib import Path                    # 跨平台路径处理

# 将项目根目录（main.py 所在目录）加入 Python 模块搜索路径
# 这样 base/、shared/、risk/ 等子包可以直接 import
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/root/trading-v2/factors")

# —— 内部模块导入 -----------------------------------------------------------------

from shared.env import cfg, assert_required         # 配置对象 + 环境变量校验
from shared.log import setup_logging                 # 日志配置（stdout + 文件双输出）
from shared.db import get_pool, close_pool           # TimescaleDB asyncpg 连接池管理
from base.data_manage import DataManageActor, DataManageConfig  # 行情管理器（订阅+入库）
from base.trading_node import build_trading_node     # TradingNode 工厂（Binance数据+Sandbox执行）
from base.registry import StrategyRegistry           # 策略注册表（内存中的策略槽管理+因子索引）
from base.executor import OrderExecutor              # 订单执行器（下单、平仓、动态仓位管理）
from risk.loop import RiskLoop                       # 1秒轮询风控循环（止损/止盈/持仓时间/日亏损）
from risk.tick_exit import TickExitManager           # Tick 级退出管理器（Trailing/ToxicFlow/Breakeven）

# —— NautilusTrader SDK 导入 ------------------------------------------------------

from nautilus_trader.trading.strategy import Strategy                   # NT 策略基类
from nautilus_trader.model.identifiers import InstrumentId, Venue       # 品种ID / 交易所标识

import trading_base_pb2 as pb                                            # protobuf 生成的交易基础消息类型

# —— gRPC 服务（策略通信） --------------------------------------------------------

from base.registration import RegistrationManager
from base.grpc_server import TradingBaseServicer, start_grpc_server      # gRPC服务端：接收策略注册+Bar推送

# —— 全局变量 --------------------------------------------------------------------

logger = setup_logging("nt_base")                        # 日志记录器（文件名前缀 "nt_base"）
VENUE_NAME = "BINANCE"                                   # 交易所名称（对应 NautilusTrader 中的 Venue）
SYMBOL = f"{cfg.primary_symbol}-PERP.{VENUE_NAME}"       # 完整品种ID，例如 "SOLUSDT-PERP.BINANCE"


# ================================================================================
# BaseStrategy — NT 策略容器
# ================================================================================
# 这是 NautilusTrader 的 Strategy 子类，作为整个系统的"执行代理人"。
# 它不负责策略逻辑（那是在 trading-v2 进程中计算的），而是持有关键的子系统引用：
#   - OrderExecutor:   下单执行（基于 gRPC 接收到的信号）
#   - RiskLoop:        1秒级风控轮询（止损/止盈/持仓时间限制）
#   - 价格更新通道:    将最新价格喂给 RiskLoop 用于风控评估
#
# 相当于"操作系统内核"——提供基础服务，不包含业务策略逻辑。
# ================================================================================

class BaseStrategy(Strategy):
    """NT Strategy 容器，持有注册表、执行器和风控循环的引用。"""

    def __init__(self, registry: StrategyRegistry):
        """
        初始化策略容器。

        参数:
            registry: 策略注册表，管理所有策略槽（slot）的生命周期

        保存的引用:
            _registry     — 策略注册表（可查询所有活跃策略及其状态）
            _executor     — 订单执行器（由 on_start 创建）
            _risk_loop    — 风控循环（由 on_start 创建，1秒间隔轮询）
            _latest_price — 最新价格缓存（用于风控和gRPC上下文）
        """
        super().__init__()                                 # 调用 NT Strategy 基类初始化
        self._registry = registry                          # 策略注册表引用
        self._executor = None                              # 订单执行器（on_start 时创建）
        self._risk_loop = None                             # 风控循环（on_start 时创建）
        self._latest_price: dict[str, float] = {SYMBOL: 0.0}  # 最新价格缓存，key=品种ID

    def on_start(self):
        """
        NT 生命周期回调 — 策略启动时自动调用。

        这是策略容器初始化的入口，按顺序执行以下操作：
        1. 创建 OrderExecutor（绑定到 SOLUSDT 品种和 BINANCE 交易所）
        2. 创建并启动 RiskLoop（异步任务，1秒间隔）
        3. 将执行上下文注入 gRPC servicer（使其能接收 signal 并执行）
        4. 启动 pending 通知清理循环（每10秒清理超时未确认的 fill 通知）

        OrderExecutor 依赖 NT 提供的多个核心组件：
          - portfolio:     投资组合管理（查询持仓、资金信息）
          - submit_order:  提交订单的 NT 方法
          - cache:         数据缓存（查询订单状态等）
          - order_factory: 订单工厂（创建限价单/市价单等）
        """
        # 从品种字符串解析 InstrumentId 对象（NautilusTrader 的内部标识格式）
        sol_id = InstrumentId.from_str(SYMBOL)
        venue = Venue("BINANCE")

        # 创建订单执行器，传入 NT 核心组件
        # OrderExecutor 是下单的核心类，负责：
        #   - 计算仓位大小（基于账户权益和杠杆）
        #   - 提交市价单入场
        #   - 管理部分成交（IOC订单的剩余部分处理）
        #   - 维护持仓状态（均价、高低点、持仓时间）
        self._executor = OrderExecutor(
            sol_id=sol_id, venue=venue,
            portfolio=self.portfolio,
            submit_order=self.submit_order,
            cache=self.cache,
            order_factory=self.order_factory,
            cancel_order=self.cancel_order,  # [maker] 撤单回调
        )

        # 创建风控循环（1秒间隔），传入注册表和执行器
        # RiskLoop 每秒检查所有有仓位的策略 slot：
        #   - 是否触发硬止损（固定价格止损线）
        #   - 是否达到止盈目标
        #   - 是否超过最大持仓时间
        #   - 是否当日亏损超限
        self._risk_loop = RiskLoop(self._registry, self._executor)

        # 启动风控循环异步任务（daemon 风格，持续运行）
        asyncio.create_task(self._risk_loop.start())
        self.log.info("RiskLoop started")

        # 将执行上下文注入 gRPC servicer
        # 这样 trading-v2 提交的 Signal 能通过 gRPC 通道直接调用 OrderExecutor
        gs = getattr(self, "_grpc_servicer", None)
        if gs:
            gs.set_execution_context(
                executor=self._executor,
                registry=self._registry,
                get_price=lambda: self._latest_price.get(SYMBOL, 0.0),
            )
            self.log.info("gRPC execution context wired")

        # 启动 pending 通知清理循环
        # 当订单填充后，OrderExecutor 会记录 pending 通知等待确认
        # 如果长时间未确认（如 trading-v2 断连），需要自动清理防止内存泄漏
        asyncio.create_task(self._cleanup_pending_loop())
        self.log.info("BaseStrategy started: executor + risk_loop ready")

    async def _cleanup_pending_loop(self):
        """
        定期清理过期的 pending fill 通知。

        OrderExecutor.on_fill() 会向 trading-v2 发送 fill 通知，
        并在 _pending_fills 中记录，等待 trading-v2 确认（ack）。
        如果 trading-v2 断连或响应超时，pending 通知会堆积。
        此循环每10秒清理一次超过超时阈值的 pending 通知。

        这是一个 daemon 任务，随策略启动而开始，随策略停止而结束。
        """
        while True:
            await asyncio.sleep(10)                           # 每10秒执行一次
            if self._executor:
                n = self._executor.cleanup_pending()          # 清理过期的 pending 通知
                if n:
                    self.log.warning(f"Cleaned {n} stale pending notifications")

    def on_order_filled(self, event):
        """
        NT 事件回调 — 订单被部分或全部成交时触发。

        当交易所匹配了订单后，NT 引擎会触发此事件。
        我们需要将成交信息转发给 OrderExecutor，以便：
        1. 更新持仓均价（VWAP 计算）
        2. 发送 fill 通知给 trading-v2（通过 gRPC）
        3. 确认 pending 订单状态

        参数:
            event: NT 的 OrderFilled 事件对象，包含：
              - client_order_id: 客户端订单ID（与提交时一致）
              - last_px:         成交价格
              - last_qty:        本次成交量
              - commission:      手续费
        """
        if self._executor:
            cid = str(event.client_order_id)                          # 客户端订单ID
            commission = float(event.commission.as_decimal()) if event.commission else 0.0
            self._executor.on_fill(cid, float(event.last_px), float(event.last_qty), commission)

    def on_order_canceled(self, event):
        """
        NT 事件回调 — 订单被取消时触发。

        对于 IOC（Immediate-or-Cancel）订单，当市场深度不足以完全成交时，
        未成交部分会被自动取消。我们需要接受已成交部分，
        并清理相应的 pending 记录，避免后续错误引用。

        参数:
            event: NT 的 OrderCanceled 事件对象
        """
        if self._executor:
            cid = str(event.client_order_id)
            self._executor.accept_partial_fill(cid)

    def on_order_expired(self, event):
        """
        NT 事件回调 — 订单过期时触发。

        与 on_order_canceled 类似，用于处理 GTD（Good-Till-Date）或
        其他带过期时间订单的到期未成交部分。接受已成交的数量并清理 pending。

        参数:
            event: NT 的 OrderExpired 事件对象
        """
        if self._executor:
            cid = str(event.client_order_id)
            self._executor.accept_partial_fill(cid)

    def on_stop(self):
        """
        NT 生命周期回调 — 策略停止时自动调用。

        执行有序关闭：
        1. 停止风控循环（1秒安全轮询任务）
        2. 清空所有 pending fill 通知
        3. 全部平仓（flat_all），释放所有持仓
        4. 日志记录

        注意：此方法在 NT 引擎停止流程中被调用，应尽可能快速完成，
        避免阻塞整个关闭流程。
        """
        if self._risk_loop:
            asyncio.create_task(self._risk_loop.stop())          # 停止风控循环
        if self._executor:
            self._executor.flush_pending()                       # 清空 pending 通知
            self._executor.flat_all(self._registry.all_slots(), "on_stop")  # 全部平仓
        self.log.info("BaseStrategy stopped")

    def get_executor(self):
        """返回订单执行器引用，供外部（如 monkey-patch 的 on_bar）使用。"""
        return self._executor

    def get_risk_loop(self):
        """返回风控循环引用，供外部使用。"""
        return self._risk_loop

    def update_price(self, symbol: str, price: float):
        """
        更新最新价格缓存，并同步给风控循环。

        每次收到 tick 或 bar 时都会调用此方法，确保：
        - _latest_price 保持最新（供 gRPC 上下文查询）
        - RiskLoop 能基于最新价格评估止损/止盈条件

        参数:
            symbol: 品种ID字符串
            price:  最新价格
        """
        self._latest_price[symbol] = price
        if self._risk_loop:
            self._risk_loop.update_price(symbol, price)


# ================================================================================
# main() — 系统主入口
# ================================================================================
# 这是整个交易系统的"大脑"，串联了行情、因子、策略、执行和风控的所有环节。
# 执行流程分以下几个阶段：
#
# 阶段1: 环境与数据准备
#   - 验证环境变量（assert_required）
#   - 连接 TimescaleDB（get_pool）
#   - 从DB预热历史Bar（prefill_bar_buffer）
#
# 阶段2: 引擎初始化
#   - 构建 NT TradingNode（build_trading_node）
#   - 创建 DataManageActor（行情订阅配置）
#   - 创建 StrategyRegistry（策略注册表）
#   - 创建 BaseStrategy 并挂载到 NT
#   - 启动 gRPC 服务器
#
# 阶段3: Monkey-patch Bar分发（核心）
#   - 拦截 DataManageActor.on_trade_tick: 累积SOL的买卖量
#   - 拦截 DataManageActor.on_trade_tick: SOL tick分发到 TickExitManager
#   - 拦截 DataManageActor.on_bar:        因子计算 + 信号分发 + 策略执行
#
# 阶段4: 运行与关闭
#   - 调用 node.run_async() 进入 NT 事件循环
#   - 注册信号处理器，捕获 SIGTERM/SIGINT 触发优雅关闭
# ================================================================================

async def main():
    """
    主异步入口函数。

    注意：这是一个 async 函数，由 __main__ 中的 asyncio.run(main()) 启动。
    这是 NautilusTrader 要求的异步执行模式。
    """

    # —— 阶段1: 环境与数据准备 --------------------------------------------------

    # 步骤1: 验证环境变量完整
    # 检查 cfg.required_keys 中的键是否都存在
    # 包括: API密钥、DB连接串、Telegram Token、品种配置等
    assert_required()

    # 步骤2: 连接 TimescaleDB
    # 创建 asyncpg 连接池（用于行情入库、因子值存储、策略配置读取）
    pool = await get_pool()

    # 步骤3: 从数据库预热历史 bar 数据
    # prefill_bar_buffer 从 bars 表读取最近 N 条 1分钟K线
    # 填充到 _bar_buffer 中，避免冷启动时等待积累足够 bar 才能计算因子
    # 返回:
    #   _bar_buffer:    deque 对象，包含 dict{ts,open,high,low,close,volume,delta,...}
    #   _latest_btc_close: 最新的 BTCUSDT 收盘价（用于残差动量因子计算）
    from prefill_bar_buffer import prefill_bar_buffer
    _bar_buffer, _latest_btc_close = await prefill_bar_buffer(pool, 300)
    logger.info(f"Buffer pre-filled: {len(_bar_buffer)} bars, latest_btc={_latest_btc_close:.2f}")

    # 记录每个策略 slot 对应的 Tick 级退出管理器
    # TickExitManager: 在 tick 粒度实时监控 Trailing/ToxicFlow/Breakeven 退出条件
    # 字典的 key = strategy_id（如 "AlphaV2-005"），value = TickExitManager 实例
    _tick_exit_managers: dict[str, TickExitManager] = {}

    # —— 阶段2: 引擎初始化 ------------------------------------------------------

    # 步骤4: 构建 NT TradingNode（沙盘模式）
    # build_trading_node 负责：
    #   - 创建 LiveDataEngine（实时数据引擎，连接 Binance WebSocket）
    #   - 创建 LiveExecEngine（沙盘执行引擎，模拟成交）
    #   - 配置杠杆和初始资金
    #   - 返回 NautilusTrader 的 TradingNode 实例
    node = build_trading_node(
        api_key=cfg.binance.api_key,          # Binance API Key
        api_secret=cfg.binance.api_secret,    # Binance API Secret
        leverage=2,                           # 杠杆倍数
        initial_usdt=int(cfg.sandbox_initial_usdt),  # 沙盘初始资金(USDT)
    )

    # 定义 BTC 品种的完整ID字符串
    BTC_SYMBOL = f"BTCUSDT-PERP.{VENUE_NAME}"

    # 步骤5: 配置 DataManageActor（行情管理器）
    # DataManageActor 是 NautilusTrader 的 Actor 子类，负责：
    #   - 订阅交易所的实时数据流
    #   - 将数据持久化到 TimescaleDB
    #   - 触发 on_trade_tick / on_bar 等回调
    #
    # 配置项:
    #   instrument_ids:      订阅K线的品种列表（SOL + BTC）
    #   tick_instrument_ids: 订阅逐笔成交的品种列表（SOL + BTC，但BTC的tick在分发时会被过滤掉）
    #   bar_timeframes:      订阅的K线粒度（1秒/5秒/1分钟）
    dm_config = DataManageConfig(
        instrument_ids=(SYMBOL, BTC_SYMBOL),                              # K线品种
        tick_instrument_ids=(SYMBOL, BTC_SYMBOL),                        # 逐笔成交品种（DB需要BTC ticks，但分发时过滤）
        bar_timeframes=("1-SECOND", "5-SECOND", "1-MINUTE"),             # K线时间周期
    )
    # 将 DataManageActor 添加到 NT 引擎的 actor 列表
    node.trader.add_actor(DataManageActor(dm_config))

    # 步骤6: 创建策略注册表
    # StrategyRegistry 负责：
    #   - 维护策略 slot 列表（strategy_id -> StrategySlot 映射）
    #   - 管理因子索引（哪些策略需要哪些因子）
    #   - 提供查询接口（all_slots, get_slot）
    registry = StrategyRegistry()

    # 步骤7: 创建 BaseStrategy 并挂载到 NT
    # BaseStrategy 是整个系统的"执行容器"
    # 它被添加到 NT 引擎后，NT 生命周期会自动调用 on_start()/on_stop()
    base_strat = BaseStrategy(registry)
    node.trader.add_strategy(base_strat)

    # RegistrationManager: poll strategy_instances table, sync pending/active status

    # 鍚姩鏃堕噸缃墍鏈夌瓥鐣ュ疄渚嬬姸鎬?鈥斺€?绛栫暐鍙湪 trading-v2 閫氳繃 gRPC Register 鍚庢墠瀛樺湪
    await pool.execute("UPDATE strategy_instances SET status='pending'")
    logger.info("All strategy_instances reset to pending (fresh start)")
    _reg_manager = RegistrationManager(registry, pool, symbol="SOLUSDT-PERP")
    asyncio.create_task(_reg_manager.run())
    logger.info("RegistrationManager task scheduled")

    # 步骤8: 构建 NT 节点
    # node.build() 会内部组装所有组件：
    #   - 初始化数据引擎、执行引擎
    #   - 注册 Actor 和 Strategy
    #   - 准备运行时环境
    # 调用后节点进入"就绪"状态，等待 run_async() 启动事件循环
    node.build()

    # 步骤9: 启动 gRPC 服务器
    # TradingBaseServicer: gRPC 服务实现类，提供以下 RPC 方法：
    #   - Register:      策略注册（接收因子源码、策略ID）
    #   - SubmitSignal:  策略提交交易信号
    #   - GetPosition:   查询当前持仓
    #   - Heartbeat:     保活检测（超时判定断连）
    #
    # start_grpc_server: 创建并启动 gRPC 服务器
    #   监听两个地址:
    #   - unix:///tmp/nt_base_grpc.sock  Unix socket（trading-v2 首选通信方式）
    #   - 0.0.0.0:50051                 TCP端口（备选通信方式）
    grpc_servicer = TradingBaseServicer(
        telegram_bot_token=cfg.telegram.bot_token,         # Telegram 机器人 Token
        telegram_chat_id=str(cfg.telegram.admin_chat_id),
        pool=pool,  # 管理员聊天ID
    )
    grpc_server = await start_grpc_server(grpc_servicer)
    logger.info("gRPC server started (unix:///tmp/nt_base_grpc.sock + :50051)")

    # 步骤10: 将 gRPC servicer 注入 BaseStrategy
    # 这样 BaseStrategy.on_start() 中就能 set_execution_context，
    # 使 gRPC 能直接调用 OrderExecutor 进行交易执行
    base_strat._grpc_servicer = grpc_servicer


    # ============================================================================
    # 阶段3: Monkey-patch Bar 分发（整个系统最核心的逻辑）
    # ============================================================================
    #
    # NautilusTrader 的设计中，Actor 的 on_trade_tick 和 on_bar 是受保护的方法，
    # 通常只能在子类中重写。但我们采用 monkey-patch 的方式直接替换 DataManageActor
    # 实例上的这些方法，以便在行情到达时插入自定义处理逻辑。
    #
    # 为什么要 monkey-patch 而不是继承重写？
    #   因为 DataManageActor 已经在 node.trader.add_actor() 时创建了实例，
    #   且 NT 内部已经建立了数据路由连接。重建 actor 需要更复杂的生命周期管理。
    #   Monkey-patch 是在不改变 NT 生命周期的情况下插入自定义逻辑的轻量方案。
    #
    # 三个 monkey-patch:
    #   1. on_trade_tick — 累积 SOL 的买卖量（用于计算 volume 和 delta 因子）
    #   2. on_trade_tick — SOL tick 分发到 TickExitManager（tick 级退出检查）
    #   3. on_bar        — 因子计算 + 信号分发 + 策略执行（核心逻辑）
    # ---------------------------------------------------------------------------

    # —— 步骤11: 查找 DataManageActor 实例 -----------------------------------------
    # NautilusTrader 将 Actor 存储在 trader._actors 中（可能是 dict 或 list）
    # 需要遍历查找类型名包含 "DataManageActor" 的实例
    #
    # 不同的 NT 版本可能使用不同的存储结构：
    #   - 新版本: dict（actor_id -> actor）
    #   - 老版本: list（actor 列表）
    #   - 备用:   _components（某些内部版本）
    dm_actor = None
    actors_container = getattr(node.trader, "_actors", None)
    if actors_container is None:
        logger.warning("node.trader._actors not found, trying _components")
        actors_container = getattr(node.trader, "_components", [])

    # 遍历查找 DataManageActor 实例
    if isinstance(actors_container, dict):
        # dict 类型：遍历所有 value
        for actor in actors_container.values():
            if "DataManageActor" in type(actor).__name__:
                dm_actor = actor
                break
    elif hasattr(actors_container, "__iter__"):
        # 可迭代类型（list/tuple）：遍历所有元素
        for actor in actors_container:
            if "DataManageActor" in type(actor).__name__:
                dm_actor = actor
                break

    logger.info(
        f"dm_actor lookup: found={dm_actor is not None}, "
        f"actors_count={len(actors_container) if hasattr(actors_container, '__len__') else '?'}"
    )

    # —— 数据缓冲区和运行状态变量 --------------------------------------------------
    from collections import deque     # 双端队列，作为 bar_buffer 的数据结构

    # _bar_buffer 已通过 prefill_bar_buffer() 预热
    # _latest_btc_close 已通过 prefill_bar_buffer() 获取

    # 累积的买卖成交量计数器
    # 每次收到 SOL 的逐笔成交(tick)时，根据 aggressor_side 累加到对应计数器
    # 每分钟 bar 到来时，取快照并重置
    _running_buyer_vol: float = 0.0      # 当前分钟内买方主动成交量（taker buy）
    _running_seller_vol: float = 0.0     # 当前分钟内卖方主动成交量（taker sell）

    # 开始 monkey-patch（前提是成功找到了 dm_actor）
    if dm_actor:
        # 注入 BaseStrategy 引用，供 data_manage 落库时按 cid 反查策略实例
        dm_actor.bind_base_strategy(base_strat)

        # —— Monkey-patch 前置: 保存原始方法引用 -----------------------------------
        # 保存原始的 on_bar 和 on_trade_tick 方法引用
        # 在 monkey-patch 中调用，确保原始功能（DB写入等）不受影响
        _original_on_bar = dm_actor.on_bar
        _original_on_trade_tick = dm_actor.on_trade_tick


        # —— Monkey-patch 1: on_trade_tick — 累积买卖量 ----------------------------
        #
        # 功能：拦截逐笔成交数据，只累积 SOLUSDT 的买卖量。
        # BTC 的 tick 仅用于 DB 存储（调用原始方法后返回），不参与累积。
        #
        # 为什么需要累积买卖量？
        #   volume_delta = taker_buy - taker_sell 是 CVD（Cumulative Volume Delta）
        #   的核心指标，用于 CVD 背离因子计算。
        #   taker_buy_volume / taker_sell_volume 分别用于因子特征工程。
        #
        # 这些数据每1分钟 bar 到达时被快照保存到 bar_buffer 中，
        # 然后计数器归零，开始下一分钟的累积。

        def _on_trade_tick_with_accum(tick):
            # 先调用原始方法（写入 DB 等原生功能不受影响）
            _original_on_trade_tick(tick)

            # 获取品种ID字符串
            iid = str(tick.instrument_id)
            # 只处理 SOLUSDT，BTC 的 tick 不参与成交量累积
            if 'SOLUSDT' not in iid:
                return  # BTC tick: 跳过买卖量累积

            # 声明非局部变量（闭包中的外部变量）
            nonlocal _running_buyer_vol, _running_seller_vol

            # 获取本次成交数量（合约张数）
            size = float(tick.size)

            # 根据 aggressor_side 判断主动买卖方向
            # aggressor_side = "BUYER" 表示买方主动吃单（taker buy）
            # aggressor_side = "SELLER" 表示卖方主动吃单（taker sell）
            if tick.aggressor_side.name == "BUYER":
                _running_buyer_vol += size      # 累积买方成交量
            else:
                _running_seller_vol += size     # 累积卖方成交量

        # 替换 DataManageActor 的 on_trade_tick 方法
        dm_actor.on_trade_tick = _on_trade_tick_with_accum


        # —— Monkey-patch 2: on_trade_tick — Tick 级退出检查 ----------------------
        #
        # 注意：这里再次覆盖了 dm_actor.on_trade_tick，
        # 上一个 monkey-patch (_on_trade_tick_with_accum) 被作为
        # _original_on_tick 保存并嵌入到这个新闭包中。
        #
        # 功能：对 SOLUSDT 的每个 tick 进行三层退出检查：
        #   1. TickTrail（Tick级跟踪止损）：价格回落超过阈值平仓
        #   2. ToxicFlow（毒性流检测）：短期内异常大的买卖单量
        #   3. Breakeven（保本退出）：价格超过盈亏平衡点后锁定利润
        #
        # 只有开仓中的策略才会被检查（slot.has_position == True）。
        # 每个有仓位的策略 slot 对应一个 TickExitManager 实例。
        #
        # 对于首次入场 vs 金字塔加仓的处理：
        #   - 首次入场: 调用 open_position() 创建新的 Trailing 锚点
        #   - 金字塔加仓: 调用 add_position() 仅更新 VWAP，保留原有锚点

        # 保存上一个 monkey-patch 的引用
        _original_on_tick = dm_actor.on_trade_tick

        def _on_trade_tick_with_exit_check(tick):
            # 先调用累积买卖量功能（上一个 monkey-patch）
            _original_on_tick(tick)

            # 获取品种代码（如 "SOLUSDT-PERP"）
            symbol = tick.instrument_id.symbol.value
            # 只处理 SOLUSDT，BTC tick 不参与退出检查
            if symbol != 'SOLUSDT-PERP':
                return

            # 获取 tick 价格并更新价格缓存和风控循环
            tick_price = float(tick.price)
            base_strat.update_price(symbol, tick_price)

            # 声明非局部变量
            nonlocal _tick_exit_managers

            # 遍历所有策略 slot，检查是否需要退出检查
            for slot in registry.all_slots():
                sid = slot.strategy_id   # 策略ID（如 "AlphaV2-005"）

                if slot.has_position:
                    # —— 此策略有仓位，需要 tick 级退出监控 -------------------------

                    # 获取或创建 TickExitManager
                    tem = _tick_exit_managers.get(sid)
                    if tem is None:
                        # 首次为这个头寸创建 TickExitManager
                        # 初始化参数：
                        #   - entry_price: 开仓均价
                        #   - is_long: 是否多头
                        #   - symbol: 品种代码
                        tem = TickExitManager()
                        tem.open_position(
                            slot.entry_price,
                            slot.entry_side == "LONG",
                            symbol
                        )
                        # 如果有 ATR 值，同步给 TickExitManager
                        # ATR 用于动态调整 trailing stop 的宽度
                        if slot.current_atr > 0:
                            tem.update_atr(slot.current_atr)
                        _tick_exit_managers[sid] = tem

                    # 执行 tick 级退出检查
                    # on_tick 内部依次检查：
                    #   1. TickTrail: 价格从最高点回落超过 ATR * 倍数
                    #   2. ToxicFlow: 短时间内异常的大额交易
                    #   3. Breakeven: 价格超过入场价 + 最小获利阈值
                    result = tem.on_tick(
                        tick_price,                    # 当前 tick 价格
                        float(tick.size),              # 本次成交量
                        tick.aggressor_side.name == 'BUYER',  # 主动方向
                        tick.ts_event,                 # 时间戳
                        symbol,                        # 品种代码
                    )

                    # 如果返回结果不为 None，表示触发了退出条件
                    if result is not None:
                        # 调用执行器平仓
                        exc = base_strat.get_executor()
                        exc.flat(slot, result.reason)   # reason 包含退出原因
                        tem.close_position()             # 关闭 TickExitManager(in_position=False -> 后续 on_tick 返回 None)
                        # [P3 修复] 不 del tem:保留已 close 的 TickExitManager,
                        # 防止下个 tick 重建 tem 在 slot.has_position 仍 True 时重复 flat。
                        # tem 在 slot.reset_position()(on_fill 确认平仓)后由 else 分支清理。
                else:
                    # —— 此策略无仓位 ------------------------------------------------
                    # 如果还持有 TickExitManager（理论上不应该），清理掉
                    if sid in _tick_exit_managers:
                        _tick_exit_managers[sid].close_position()
                        del _tick_exit_managers[sid]

        # 替换 DataManageActor 的 on_trade_tick 方法（覆盖上一个 monkey-patch）
        dm_actor.on_trade_tick = _on_trade_tick_with_exit_check


        # —— Monkey-patch 3: on_bar — 核心 Bar 分发逻辑 ----------------------------
        #
        # 这是整个系统最复杂、最关键的逻辑入口。
        # 每次收到任何粒度的 bar（1秒/5秒/1分钟）都会触发此函数。
        #
        # 执行流程（按顺序）：
        #
        #   1. BTC 1分钟 bar — 更新 _latest_btc_close（用于残差动量因子）
        #   2. 非1分钟 bar — 跳过后续处理（只更新价格）
        #   3. 快照并重置累积成交量（buyer_vol / seller_vol）
        #   4. 追加 bar 到 _bar_buffer
        #   5. 计算 ATR（30bar 高低差均值）
        #   6. 因子计算（通过 grpc_servicer._factor_engine.execute_all）
        #   7. 构建 protobuf Bar 并推送给所有注册客户端
        #   8. 对每个策略 slot: strategy.on_bar() — signal
        #   9. 根据 signal.direction 执行/持有/退出
        #  10. 清理孤儿策略（断连超时策略的自动平仓和注销）

        def _on_bar_with_dispatch(bar):
            # 先调用原始 on_bar（写入 DB 等原生功能不受影响）
            _original_on_bar(bar)

            # 获取品种ID字符串
            iid = str(bar.bar_type.instrument_id)
            # 更新价格缓存
            base_strat.update_price(iid, float(bar.close))

            # ----------------------------------------------------------------
            # 逻辑1: BTC bar 处理
            # ----------------------------------------------------------------
            # BTCUSDT 的 bar 只需要更新 _latest_btc_close 变量。
            # 该变量用于 residual_momentum 因子（残差动量），
            # 该因子通过 OLS 回归剥离 SOL 价格中 BTC 成分的 beta 暴露。
            if "BTCUSDT" in iid:
                # 只处理1分钟粒度的 BTC bar
                if "1-MINUTE" in str(bar.bar_type.spec):
                    nonlocal _latest_btc_close, _tick_exit_managers
                    _latest_btc_close = float(bar.close)
                return  # BTC bar 处理完毕，不继续执行后续逻辑

            # ----------------------------------------------------------------
            # 逻辑2: 非1分钟 bar 过滤
            # ----------------------------------------------------------------
            # 只有 1-MINUTE 粒度的 bar 才会触发因子计算和策略信号生成。
            # 1秒/5秒 bar 仅用于价格更新和原始数据存储。
            if "1-MINUTE" not in str(bar.bar_type.spec):
                return

            # ----------------------------------------------------------------
            # 逻辑3: 成交量快照并重置
            # ----------------------------------------------------------------
            # 从累积计数器获取本分钟内的总买卖成交量
            # 然后立即重置计数器，开始下一分钟的累积
            nonlocal _running_buyer_vol, _running_seller_vol
            buyer_vol = _running_buyer_vol       # 快照买方成交量
            seller_vol = _running_seller_vol     # 快照卖方成交量
            _running_buyer_vol = 0.0             # 重置计数器
            _running_seller_vol = 0.0            # 重置计数器

            # 计算 delta（买卖净差 = 买方量 - 卖方量）
            # 正 delta 表示买方主动性强（看涨信号）
            # 负 delta 表示卖方主动性强（看跌信号）
            delta = buyer_vol - seller_vol
            volume = buyer_vol + seller_vol      # 总成交量

            # ----------------------------------------------------------------
            # 逻辑4: 日志 — 定期输出 bar_buffer 状态
            # ----------------------------------------------------------------
            # 每5个 bar 输出一次调试日志，便于监控系统状态
            if len(_bar_buffer) % 5 == 0:
                logger.info(
                    f"bar_buffer stats: len={len(_bar_buffer)} "
                    f"volume={volume:.2f} delta={delta:.2f} "
                    f"btc_close={'%.2f' % _latest_btc_close if _latest_btc_close > 0 else 'pending'}"
                )

            # ----------------------------------------------------------------
            # 逻辑5: 追加 bar 到缓冲区
            # ----------------------------------------------------------------
            # 只有1分钟 bar 被追加到 _bar_buffer
            # 用于后续的因子计算和 ATR 计算
            _bar_buffer.append({
                "ts": bar.ts_event,                              # 时间戳（纳秒级）
                "open": float(bar.open),                         # 开盘价
                "high": float(bar.high),                         # 最高价
                "low": float(bar.low),                           # 最低价
                "close": float(bar.close),                       # 收盘价
                "volume": volume,                                # 成交量（买卖合计）
                "delta": delta,                                  # 买卖净差
                "taker_buy_volume": buyer_vol,                   # 买方主动成交量
                "taker_sell_volume": seller_vol,                 # 卖方主动成交量
                "btc_close": _latest_btc_close if _latest_btc_close > 0 else None,  # BTC 收盘价
            })

            # 获取执行器引用
            executor = base_strat.get_executor()

            # ----------------------------------------------------------------
            # 逻辑6: ATR 计算（30bar 高低差均值）
            # ----------------------------------------------------------------
            # ATR（Average True Range，平均真实波幅）用于：
            #   - Tick 级退出的 Trailing Stop 宽度
            #   - 动态止盈止损距离
            #
            from utils import atr
            if len(_bar_buffer) >= 30:
                import pandas as _pd_atr; _atr_df = _pd_atr.DataFrame(list(_bar_buffer)); _current_atr = float(atr(_atr_df, period=30).iloc[-1])

                # 将 ATR 同步给所有策略 slot
                for s in registry.all_slots():
                    s.current_atr = _current_atr                        # 策略 slot 使用 ATR 调整仓位
                # 将 ATR 同步给所有 TickExitManager
                for tem in _tick_exit_managers.values():
                    tem.update_atr(_current_atr)                        # TickExitManager 使用 ATR 调整 trailing 宽度

            # ----------------------------------------------------------------
            # 逻辑7: 因子计算 + gRPC Bar 推送（统一路径）
            # ----------------------------------------------------------------
            #
            # 因子由 trading-v2 通过 gRPC Register() 提交的 Python 源码定义。
            # FactorEngine（在 grpc_servicer 内部）负责加载、编译和执行这些因子。
            #
            # 执行过程：
            #   1. 将 _bar_buffer 转为 pandas DataFrame
            #   2. FactorEngine.execute_all(df) 对所有已注册因子进行计算
            #   3. 构建 protobuf Bar 消息（包含因子值）
            #   4. 构建每个策略的 PositionState（持仓状态快照）
            #   5. 通过 gRPC push_bar 推送给所有已注册客户端
            #
            # 仅当 buffer 长度 >= 30（有足够的 bar 计算因子）且 grpc_servicer 就绪时执行
            position_states = {}
            if grpc_servicer and len(_bar_buffer) >= 30:
                import pandas as pd

                # 将 _bar_buffer 转为 DataFrame
                df = pd.DataFrame(list(_bar_buffer))
                df["ts"] = pd.to_datetime(df["ts"])      # 时间戳列转 datetime 类型
                df = df.set_index("ts")                   # 以时间戳为索引

                # 确保所有必需的列都存在（缺失的用 0.0 填充）
                for col in ["delta", "taker_buy_volume", "taker_sell_volume", "btc_close"]:
                    if col not in df.columns:
                        df[col] = 0.0

                # —— 因子计算 -------------------------------------------------------
                # FactorEngine.execute_all(df) 会依次：
                #   1. 遍历所有已注册的因子
                #   2. 对每个因子调用 compute(df) 方法
                #   3. 收集结果并返回 dict
                # 返回的 factors dict 包含因子名称->值的映射
                # 例如: {"cvd_divergence": 0.5, "residual_momentum": -0.3, "channel_breakout": 0.8}
                # —— 构建每个策略的 PositionState（持仓快照）---
                position_states = {}
                for slot in registry.all_slots():
                    if slot.has_position:
                        side = pb.PositionState.LONG if slot.entry_side == "LONG" else pb.PositionState.SHORT
                        ps = pb.PositionState(
                            side=side,
                            entry_price=slot.entry_price,
                            bars_held=int(slot.held_sec / 60),
                            highest_price=slot.highest_since_entry,
                            lowest_price=slot.lowest_since_entry,
                            current_atr=slot.current_atr,
                            breakeven_activated=getattr(slot, "breakeven_activated", False),
                        )
                        position_states[slot.strategy_id] = ps

                # —— 每策略独立 FactorEngine + 独立 Bar 推送 ---
                # 替代原"全局 execute_all + 单 bar 推所有"：每策略只算/推送自己的因子
                for sid, sinfo in grpc_servicer._strategies.items():
                    fe = sinfo.get("factor_engine")
                    if fe is None:
                        continue
                    factors = fe.execute_all(df)
                    logger.info(f"[{sid}] factors computed: {factors}")
                    pb_bar = grpc_servicer.build_bar(
                        symbol=SYMBOL,
                        ts_ns=bar.ts_event,
                        open_p=float(bar.open), high=float(bar.high),
                        low=float(bar.low), close=float(bar.close),
                        volume=volume, delta=delta,
                        taker_buy=buyer_vol, taker_sell=seller_vol,
                        btc_close=_latest_btc_close,
                        df_bars=df,
                        factors=factors,
                    )
                    if sid in position_states:
                        pb_bar.position.CopyFrom(position_states[sid])
                    try:
                        grpc_servicer._bar_queues[sid].put_nowait(pb_bar)
                    except asyncio.QueueFull:
                        logger.warning(f"策略 {sid} 的 Bar 队列已满，丢弃旧数据")

            else:
                # buffer 不足或 gRPC 未就绪时使用空因子字典
                factors = {}

            # ----------------------------------------------------------------
            # 逻辑8: 策略信号分发
            # ----------------------------------------------------------------
            # 对每个已注册的策略 slot，调用 strategy.on_bar(bar_data) 获取信号
            # 并根据信号方向执行相应操作
            #
            # Signal 的三个方向:
            #   1. direction != 0 (LONG=1 / SHORT=-1):
            #      — OrderExecutor.execute() 执行入场或方向切换
            #   2. direction == 0, reason == "hold":
            #      — 继续持有当前仓位，不做任何操作
            #   3. direction == 0, reason != "hold":
            #      — 标记 bar 级退出（pending_bar_exit），由后续逻辑处理

            # 从 factor 结果中提取趋势置信度（用于仓位大小调整）
            confidence = factors.get("trend_confidence", 0.0)

            slots = registry.all_slots()
            for slot in slots:
                # 构建 bar 数据字典（传入策略的 on_bar 方法）
                bar_data = {
                    "close": float(bar.close),     # 当前收盘价
                    "high": float(bar.high),       # 当前最高价
                    "low": float(bar.low),         # 当前最低价
                    "ts_ns": bar.ts_event,         # 时间戳
                    "factors": factors,            # 所有因子值
                    "btc_close": _latest_btc_close if _latest_btc_close > 0 else float(bar.close),
                }
                # 添加 PositionState（供 AlphaSignal.on_bar 获取持仓状态）
                ps = position_states.get(slot.strategy_id) if position_states else None
                if ps is not None:
                    bar_data["position"] = ps

                # 调用策略的 on_bar 方法获取信号
                # 注意：这里调用的 strategy 是在 trading-v2 注册时
                # 通过 gRPC 传入的 SignalStrategy 实例的 on_bar 方法
                signal = slot.strategy.on_bar(bar_data)

                if signal is not None:
                    # 保存趋势置信度到 slot（用于仓位大小决策）
                    slot.confidence = confidence

                    if signal.direction != 0:
                        # —— 入场信号 -----------------------------------------------
                        # direction != 0 表示做多(1)或做空(-1)
                        was_in_position = slot.has_position          # 记录入场前是否有仓位
                        result = executor.execute(slot, signal, float(bar.close))

                        sid = slot.strategy_id

                        if slot.has_position:
                            # —— 创建/更新 Tick 级退出管理器 ------------------------
                            # 入场成功后，为该仓位创建或更新 TickExitManager
                            tem = _tick_exit_managers.get(sid)
                            if tem is None:
                                tem = TickExitManager()
                                _tick_exit_managers[sid] = tem

                            if was_in_position and result == "pyramid":
                                # —— 金字塔加仓 -------------------------------------
                                # 已有仓位，且本次是同向加仓（金字塔加仓）
                                # add_position: 仅更新 VWAP（加权均价）
                                #   保留原有的 trailing stop 锚点
                                tem.add_position(slot.entry_price)
                            else:
                                # —— 首次入场或方向切换 -------------------------------
                                # open_position: 创建新的 trailing 锚点
                                #   重置最高/最低价追踪
                                tem.open_position(
                                    slot.entry_price,
                                    slot.entry_side == "LONG",
                                    "SOLUSDT-PERP"
                                )

                            # 同步 ATR 给 TickExitManager
                            if slot.current_atr > 0:
                                tem.update_atr(slot.current_atr)

                    elif signal.reason == "hold":
                        # —— 持有信号 -----------------------------------------------
                        # direction == 0 且 reason == "hold"
                        # 表示策略认为应继续持有当前仓位，不触发任何操作
                        # 这是为了与"平仓信号"(direction==0, reason!="hold")区分
                        result = "hold"

                    else:
                        # —— Bar 级退出信号 -----------------------------------------
                        # direction == 0 且 reason != "hold"
                        # 表示策略认为应平仓离场
                        # 与 Tick 级退出的区别：
                        #   Tick 级退出（TickExitManager）立刻执行
                        #   Bar 级退出标记后在下一个 bar 前由执行器批量处理
                        if slot.has_position:
                            # 记录退出原因到 slot 的 pending_bar_exit
                            # RiskLoop 的下一个轮询周期会处理这个退出
                            slot.pending_bar_exit = signal.reason
                            logger.info(
                                f"Bar exit queued: {slot.strategy_id} "
                                f"reason={signal.reason}"
                            )
                            result = f"queued: {signal.reason}"
                        else:
                            result = "no position"

                    # 记录信号日志（调试和监控用）
                    logger.info(
                        f"Signal: {slot.strategy_id} dir={signal.direction} "
                        f"reason={signal.reason} result={result}"
                    )

            # ----------------------------------------------------------------
            # 逻辑9: 清理孤儿策略（断连超出宽限期）
            # ----------------------------------------------------------------
            # 孤儿策略 = trading-v2 断连超过宽限期（默认60秒）的策略
            #
            # 清理流程：
            #   1. 获取孤儿策略列表（gRPC servicer 基于心跳超时判定）
            #   2. 对每个孤儿策略：
            #      a. 如果有持仓 — 先平仓（flat），等待 fill 确认
            #      b. 如果没有持仓 — 直接注销（unregister + cleanup）
            #
            # 为什么有持仓时不立即清理？
            #   因为平仓需要等待订单成交（fill），成交确认后会通过
            #   on_order_filled 回调路由到 OrderExecutor。等订单完全成交后，
            #   下次孤儿检查时发现无持仓再清理。
            if grpc_servicer:
                orphans = grpc_servicer.orphaned_strategies()            # 查询孤儿策略列表
                for sid in orphans:
                    slot = registry.get_slot(sid)                       # 查找策略 slot

                    if slot is None:
                        # 策略 slot 不存在（异常情况），直接清理 gRPC 端记录
                        grpc_servicer.cleanup_strategy(sid)
                        logger.warning(f"Orphan {sid}: cleaned up (no slot)")
                        continue

                    if slot.has_position:
                        # —— 有仓位：先平仓，暂不清理 -------------------------------
                        # flat 会提交市价平仓单，订单需要时间成交
                        executor.flat(slot, "strategy_disconnected")
                        logger.warning(
                            f"Orphan {sid}: flattening position ({slot.entry_side} "
                            f"{slot.held_sec:.0f}s held) on disconnect"
                        )
                        # 不要立即清理 — 等待 fill 确认后再处理
                    else:
                        # —— 无仓位：直接注销 -----------------------------------------
                        registry.unregister(sid)                        # 从注册表移除
                        grpc_servicer.cleanup_strategy(sid)             # 清理 gPRC 端记录
                        if sid in _tick_exit_managers:
                            _tick_exit_managers[sid].close_position()    # 关闭 TickExitManager
                            del _tick_exit_managers[sid]
                        logger.warning(f"Orphan {sid}: cleaned up (no position)")

        # 替换 DataManageActor 的 on_bar 方法
        dm_actor.on_bar = _on_bar_with_dispatch
        logger.info("Bar dispatch wired: DataManageActor -> factors -> strategies -> executor")


    # —— 阶段4: 运行与优雅关闭 ----------------------------------------------------

    # 步骤12: 注册信号处理器
    # SIGTERM: systemctl stop 发送的终止信号
    # SIGINT:  Ctrl+C 发送的中断信号
    # 都触发 _shutdown() 开始优雅关闭流程
    def _shutdown():
        logger.info("Shutdown signal received")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, lambda s, f: _shutdown())
        except Exception:
            pass

    logger.info(f"nt-base running: mode={cfg.mode} symbol={SYMBOL}")

    # 步骤13: 启动 NT 事件循环
    # node.run_async() 阻塞直到引擎停止（NautilusTrader 内部事件循环）
    # 引擎运行期间，所有数据流、因子计算、信号处理都在这里进行
    try:
        await node.run_async()
    except KeyboardInterrupt:
        pass  # Ctrl+C 忽略异常（由 finally 中的关闭逻辑处理）
    finally:
        # —— 优雅关闭流程 ---------------------------------------------------------
        logger.info("Shutting down...")

        # 1. 停止 gRPC 服务器（等待5秒完成正在处理的请求）
        await grpc_server.stop(grace=5.0)
        logger.info("gRPC server stopped")

        # 2. 释放 NT 引擎资源（关闭 WebSocket 连接、清理运行时状态）
        node.dispose()

        # 3. 关闭 TimescaleDB 连接池
        await close_pool()
        logger.info("nt-base stopped")


# ================================================================================
# 程序入口
# ================================================================================

if __name__ == "__main__":
    # asyncio.run() 启动事件循环并运行 main() 协程
    # 这是 Python 3.7+ 推荐的做法（自动管理事件循环的创建和关闭）
    asyncio.run(main())
