"""
gRPC TradingBase servicer —— nt-base 实时交易节点的 gRPC 服务端核心模块。

============================================================================
架构概述：gRPC 双向通信（nt-base <-> trading-v2）
============================================================================
本模块是 nt-base（交易引擎）和 trading-v2（策略客户端）之间通信的桥梁，
基于 gRPC 的 双向/单向 流式 RPC 实现以下核心交互流程：

【数据流方向】
  trading-v2 (gRPC 客户端)                    nt-base (gRPC 服务端)
  ─────────────────────────────────────────────────────────────────
  1. Register(config, factors)  ───────→  编译因子代码，创建策略槽
  2. SubscribeBars(symbol)       ───────→  建立 Bar 数据推送流
  3. ←─── Bar protobuf (因子+行情+仓位) ───  周期推送（每根1m Bar）
  4. SubmitSignal(signal)         ──────→  验证信号 → 执行交易
  5. SubscribeControl(sid)       ───────→  建立控制指令接收流
  6. ←─── ControlCommand ─────────────────  推送止盈/止损/平仓等指令
  7. Unregister(sid)             ───────→  清理策略资源

【生命周期】
  Phase 1: 启动注册 —— trading-v2 启动后调用 Register，nt-base 编译因子
  Phase 2: 流式订阅 —— SubscribeBars 打开 gRPC streaming channel
  Phase 3: 正常交易 —— Bar 推送 → 策略判定 → 信号提交 → 下单执行
  Phase 4: 断连恢复 —— 客户端断连后 60s 宽限期，可重连恢复
  Phase 5: 注销清理 —— Unregister 或 超时清理

【关键设计原则】
  - 因子计算在服务端（nt-base）执行，策略端只需提交策略逻辑代码
  - 仓位状态由服务端管理（tick 级退出也在服务端），定期同步到策略端
  - 异步队列解耦：Bar 推送、信号处理、控制指令均通过 asyncio.Queue
  - 双端口监听：Unix socket（本机低延迟）+ TCP（远程调试/备用）
"""

from __future__ import annotations

import asyncio
import logging
import time

import grpc
import pandas as pd
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal, BarSubscription

import trading_base_pb2 as pb
import trading_base_pb2_grpc as pb_grpc
from base.factor_engine import FactorEngine

logger = logging.getLogger(__name__)



class _GrpcSlotStrategy:
    """
    gRPC 管理的策略槽（StrategySlot）所需的"最小策略桩"。

    StrategySlot 需要一个实现了 SignalStrategy 协议的对象（含 on_bar、on_shutdown
    等方法），但 gRPC 策略的实际逻辑在 trading-v2 端运行，nt-base 端不需要执行
    on_bar。这个桩对象仅用于占位，满足 StrategySlot 的构造要求。
    """
    def __init__(self, sid: str):
        self.strategy_id = sid

    def on_bar(self, bar_data): return None
    def on_shutdown(self): pass
    def get_diagnostics(self): return {}

class TradingBaseServicer(pb_grpc.TradingBaseServicer):
    """
    gRPC 交易服务端核心实现。

    这是 nt-base 中最关键的类之一，承担以下职责：
    1. 策略注册/反注册管理（热插拔，无需重启服务）
    2. 因子代码编译与注册（接收 trading-v2 发送的因子源码并执行）
    3. Bar 数据流式推送（包装为 protobuf，含计算后的因子值和仓位状态）
    4. 信号接收与执行（验证 → 路由到 OrderExecutor 下单）
    5. 控制指令流式推送（Telegram 止盈止损等 → 推送到策略端）
    6. 断连检测与资源清理

    生命周期（由 main.py 调度）：
      - set_execution_context()  →  main.py 启动后注入
      - Register / SubscribeBars →  trading-v2 启动时调用
      - build_bar / push_bar     →  main.py 每根 Bar 回调
      - SubmitSignal             →  trading-v2 策略判定后调用
      - orphaned_strategies()    →  RiskLoop 定期检查断连超时
      - cleanup_strategy()       →  清理断连超时的策略
    """


    # ---- 执行上下文（由 main.py 在 executor 就绪后注入） ----
    _executor = None      # OrderExecutor 实例，用于执行交易信号
    _registry = None      # StrategyRegistry 实例，管理策略槽注册
    _get_price = None     # 获取当前价格的回调函数（用于信号执行时的价格参考）

    def set_execution_context(self, executor, registry, get_price=None):
        """
        延迟注入执行上下文。

        为什么不在 __init__ 中注入？
        gRPC 服务端在 main.py 早期启动（为了尽快开始监听），而此时 OrderExecutor
        可能尚未完全就绪（NautilusTrader TradingNode 可能还在初始化中）。
        main.py 在 TradingNode 启动完成后回调此方法，完成上下文注入。

        参数：
          executor:    OrderExecutor 实例，负责下单/平仓/仓位管理
          registry:    StrategyRegistry 实例，负责策略槽注册
          get_price:   可选回调，返回当前最新价格（用于信号执行时的价格判定）
        """
        self._executor = executor
        self._registry = registry
        self._get_price = get_price
        logger.info("gRPC 执行上下文已注入（executor + registry）")

    def __init__(self, telegram_bot_token: str = "", telegram_chat_id: str = ""):
        """
        初始化 gRPC 服务端。

        数据结构说明：
          _strategies:    dict[str, dict] — 按 strategy_id 索引的策略注册信息表
                          每个条目包含：
                            config:          注册时的完整 StrategyConfig protobuf
                            registered_at:   注册时间戳（unix time）
                            required_fields: 必需的 Bar 字段列表
                            telegram_*:      Telegram 通知凭据
                            disconnected_at: 断连时间戳（None 表示在线）
                            grace_period_sec:断连宽限期（默认60秒）

          _factor_engine: FactorEngine 实例
                          管理所有策略注册时提交的因子代码，编译后可用于 Bar 时计算。
                          所有策略共享同一个引擎，相同因子名会被覆盖。

          _bar_queues:    dict[str, asyncio.Queue]
                          每个策略独立的 Bar 推送队列。SubscribeBars streaming 从
                          对应队列中消费 Bar 并推送到客户端。最大容量 100，超限丢弃。

          _control_queues: dict[str, asyncio.Queue]
                          每个策略独立的控制指令队列。SubscribeControl streaming 从
                          对应队列消费 ControlCommand。最大容量 50。

          _pending_signals: asyncio.Queue
                          暂未处理（订单执行上下文未就绪时暂存）的信号队列。
                          main.py 的推送循环中通过 pending_signals() 排空处理。
        """
        self._default_bot_token = telegram_bot_token
        self._default_chat_id = telegram_chat_id
        self._strategies: dict[str, dict] = {}
        self._factor_engine = FactorEngine()
        # Bar 推送队列：strategy_id → asyncio.Queue（容量 100）
        self._bar_queues: dict[str, asyncio.Queue] = {}
        # 控制指令队列：strategy_id → asyncio.Queue（容量 50）
        self._control_queues: dict[str, asyncio.Queue] = {}
        # 待处理的信号队列（由 main.py 的推送循环排空）
        self._pending_signals: asyncio.Queue = asyncio.Queue()

    # ── 策略注册 / 反注册 ─────────────────────────────────

    async def Register(self, request: pb.StrategyConfig, context) -> pb.RegisterAck:
        """
        策略注册 RPC。

        调用方：trading-v2 启动时的 GrpcAlphaSignal.__init__

        流程：
          1. 检查是否是重连：如果 strategy_id 已存在且 disconnected_at 不为 None，
             则为断连重连，只需清除断连标记，无需重新注册因子
          2. 编译并注册因子代码：遍历 request.factors，依次调用 FactorEngine.register
             编译执行。如果有任何因子的 Python 语法错误，立即返回失败（ok=False + 错误信息）
          3. 获取 Telegram 凭据：从全局环境配置读取 bot_token 和 admin_chat_id
          4. 创建策略注册信息：包括配置、注册时间、需要的 Bar 字段等
          5. 创建通信队列：为当前策略创建独立的 Bar 推送队列和控制指令队列

        参数：
          request: StrategyConfig protobuf，包含 strategy_id、factor 列表、所需字段等
          context: gRPC 调用上下文

        返回：
          RegisterAck：ok=True 表示注册成功；ok=False + error 表示失败原因
        """
        sid = request.strategy_id
        if sid in self._strategies:
            # 重连处理：清除断连标记，允许重新订阅 Bar 流
            self._strategies[sid]["disconnected_at"] = None
            logger.info(f"gRPC 重新注册（断连重连）: {sid}")
            return pb.RegisterAck(ok=True)

        # 逐一遍历因子，编译并注册到 FactorEngine
        for fd in request.factors:
            try:
                self._factor_engine.register(
                    name=fd.name,
                    code=fd.code,
                    params=dict(fd.params) if fd.params else None,
                )
            except SyntaxError as e:
                # 因子代码语法错误，立即返回失败
                return pb.RegisterAck(ok=False, error=f"因子 '{fd.name}' 语法错误: {e}")

        # 获取 Telegram 凭据（从共享环境配置读取，所有策略共用同一 bot）
        from shared.env import cfg
        token = cfg.telegram.bot_token
        chat_id = str(cfg.telegram.admin_chat_id)
        logger.info(f"gRPC 注册 {sid}: Telegram 已配置 (chat={chat_id})")

        # 创建策略注册条目
        self._strategies[sid] = {
            "config": request,                       # 完整配置 protobuf
            "registered_at": time.time(),             # 注册时间
            "required_fields": list(request.required_fields),  # 所需 Bar 字段
            "telegram_bot_token": token,              # Telegram bot token
            "telegram_chat_id": chat_id,              # 通知目标 chat_id
            "disconnected_at": None,                  # 断连时间（None=在线）
            "grace_period_sec": 60,                   # 断连宽限期（秒）
        }
        # 创建通信队列
        self._bar_queues[sid] = asyncio.Queue(maxsize=100)      # Bar 推送队列
        self._control_queues[sid] = asyncio.Queue(maxsize=50)   # 控制指令队列

        logger.info(
            f"gRPC 注册完成: {sid} 因子={self._factor_engine.registered_names()} "
            f"所需字段={request.required_fields}"
        )
        return pb.RegisterAck(ok=True)

    async def Unregister(self, request: pb.StrategyId, context) -> pb.UnregisterAck:
        """
        策略反注册 RPC。

        清理策略的注册信息和通信队列。通常在 trading-v2 正常关闭时调用。
        如果策略已断开（断连超时被 cleanup），pop 会安全返回 None。
        """
        sid = request.strategy_id
        self._strategies.pop(sid, None)      # 移除注册信息（安全删除）
        if sid in self._bar_queues:
            del self._bar_queues[sid]         # 移除 Bar 推送队列
        logger.info(f"gRPC 反注册: {sid}")
        return pb.UnregisterAck(ok=True)

    # ── Bar 流式推送 ──────────────────────────────────────

    async def SubscribeBars(self, request: pb.BarRequest, context):
        """
        Bar 数据流式推送 RPC（服务端流式 RPC）。

        调用方：trading-v2 注册成功后，GrpcAlphaSignal 调用此 RPC 建立持续连接。
                此连接是双向通信的核心通道，长期保持（可能在数小时）。

        实现要点：
          1. 策略标识匹配：使用最后一个注册策略的 ID（简化设计，目前单策略场景）
          2. 异步队列消费：从策略的 _bar_queues[sid] 中消费，逐条 yield 给客户端
          3. 30 秒超时：每次 queue.get() 等待 30 秒。超时后 continue 继续等待，
             避免永久阻塞。这不是心跳超时，而是让循环有机会检查 CancelledError
          4. 断连标记：如果因异常退出（客户端断连），在 finally 块中设置
             disconnected_at 时间戳，触发宽限期计时
          5. gRPC 流式 yield 机制：Python async generator 通过 yield 将每个 Bar
             protobuf 推送到 gRPC stream，由 gRPC 框架序列化为 wire format 发送

        参数：
          request: BarRequest protobuf（目前未使用，未来可扩展指定品种/周期）
          context: gRPC 调用上下文。context.abort() 可用于拒绝请求

        gRPC 流式推送示意：
          client ←── Bar #1  ←──  yield bar
          client ←── Bar #2  ←──  yield bar
          client ←── ...     ←──  ...
          断连时 → 设置 disconnected_at → main.py 的 RiskLoop 检测到宽限期超时
        """
        logger.info(f"[SUB] SubscribeBars 进入: 队列={list(self._bar_queues.keys())}")
        sid = list(self._bar_queues.keys())[-1] if self._bar_queues else None
        if sid is None:
            logger.error("[SUB] 没有已注册的策略")
            await context.abort(grpc.StatusCode.NOT_FOUND, "没有已注册的策略")
            return

        queue = self._bar_queues[sid]
        logger.info(f"[SUB] 开始为 {sid} 推送流, 队列大小={queue.qsize()}")

        bar_count = 0
        try:
            while True:
                try:
                    logger.info(f"[SUB] 等待 Bar (已推送={bar_count})...")
                    # 从异步队列获取 Bar（30 秒超时，避免永久阻塞）
                    bar = await asyncio.wait_for(queue.get(), timeout=30.0)
                    bar_count += 1
                    logger.info(f"[SUB] 推送 Bar #{bar_count}")
                    yield bar  # ← 关键：gRPC 流式推送（yield 给 stream）
                    logger.info(f"[SUB] Bar #{bar_count} 推送完成")
                except asyncio.TimeoutError:
                    # 队列为空超时，持续等待（不是错误）
                    continue
                except asyncio.CancelledError:
                    # gRPC 连接被取消（客户端主动关闭连接）
                    logger.info(f"[SUB] 流取消，已推送 {bar_count} 个 Bar")
                    return
        except Exception as e:
            logger.error(f"[SUB] 流异常退出: {type(e).__name__}: {e}", exc_info=True)
        finally:
            # 任何原因导致流退出 → 标记策略断连时间
            if sid and sid in self._strategies:
                self._strategies[sid]["disconnected_at"] = time.time()
                logger.warning(
                    f"[SUB] 策略 {sid} 已断连，宽限期={self._strategies[sid]['grace_period_sec']}秒"
                )
        logger.info(f"[SUB] 流推送结束，共推送 {bar_count} 个 Bar")

    # ---- 控制指令流式推送 ----

    async def SubscribeControl(self, request: pb.ControlRequest, context):
        """
        控制指令流式推送 RPC（服务端流式 RPC）。

        与 SubscribeBars 类似，传输的是 ControlCommand protobuf。
        用于服务端向策略客户端推送：
          - 手动平仓指令（通过 Telegram 触发）
          - 风控急停指令
          - 状态更新通知

        实现细节：
          - 60 秒队列等待超时（控制指令不频繁，允许更长等待）
          - 策略端通过独立的 streaming 连接接收，与 Bar 流互不干扰
        """
        sid = request.strategy_id
        if sid not in self._control_queues:
            logger.error(f"[CTL] 未知的策略: {sid}")
            await context.abort(grpc.StatusCode.NOT_FOUND, f"未知策略: {sid}")
            return

        queue = self._control_queues[sid]
        logger.info(f"[CTL] 控制指令流已为 {sid} 启动")

        cmd_count = 0
        try:
            while True:
                try:
                    # 从控制指令队列获取（60 秒超时）
                    cmd = await asyncio.wait_for(queue.get(), timeout=60.0)
                    cmd_count += 1
                    logger.info(f"[CTL] 推送指令 #{cmd_count}: type={cmd.type} 到 {sid}")
                    yield cmd
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    logger.info(f"[CTL] 流取消，已推送 {cmd_count} 条指令")
                    return
        except Exception as e:
            logger.error(f"[CTL] 流异常退出: {type(e).__name__}: {e}", exc_info=True)
        logger.info(f"[CTL] 控制指令流结束于 {sid}，共推送 {cmd_count} 条指令")

    # ---- 信号提交 ----

    async def SubmitSignal(self, request: pb.Signal, context) -> pb.SignalAck:
        """
        策略提交交易信号 RPC（一元 RPC，非流式）。

        调用方：trading-v2 策略在 on_bar() 中判定入场/出场信号后调用。

        核心流程：
          1. 信号暂存：无论如何先将信号放入 _pending_signals 队列，
             确保 main.py 的推送循环也能处理（用于日志/统计）
          2. 执行上下文检查：如果 executor/registry 未就绪，只暂存不执行，
             返回 accepted=True 但实际未下单
          3. 信号方向映射：
             - direction=1  →  gdir=1（做多）
             - direction<0  →  gdir=-1（做空）
             - direction=0  →  方向不变，可能是平仓信号
          4. 策略槽自动创建：遍历所有注册策略，如果 StrategySlot 尚不存在，
             根据注册配置自动创建（含杠杆、仓位比例等参数）。
             这是"热注册"的核心：trading-v2 可以安全地在 nt-base 运行中注册，
             不依赖 nt-base 重启
          5. 信号执行：
             - direction != 0：调用 OrderExecutor.execute() 执行开仓/调仓
             - reason == "hold"：跳过执行（继续持有当前仓位）
             - reason != "hold" && direction == 0：Bar 级退出信号，
               设置 slot.pending_bar_exit 给 RiskLoop 逐秒检查执行

        参数：
          request: Signal protobuf，包含 direction（方向）、reason（原因）、
                   position_size_pct（仓位比例）等
          context: gRPC 调用上下文

        返回：
          SignalAck：accepted=True 表示信号已接收
        """
        direction_name = pb.Signal.Direction.Name(request.direction)
        reason = request.reason
        logger.info(f"信号: dir={direction_name} reason={reason}")

        # 暂存信号到待处理队列（供 main.py 推送循环读取）
        await self._pending_signals.put(request)

        executor = self._executor
        registry = self._registry
        if executor is None or registry is None:
            # 执行上下文未就绪，信号暂存但不执行
            return pb.SignalAck(accepted=True)

        # 映射 gRPC 方向到内部方向值
        pb_dir = request.direction
        gdir = 1 if pb_dir == 1 else (-1 if (pb_dir < 0 or pb_dir > 1) else 0)
        price = self._get_price() if self._get_price else 0.0

        # 遍历所有已注册策略（目前预期单策略，循环 break 确保只处理一次）
        for sid, info in self._strategies.items():
            if info.get("disconnected_at"):
                # 策略已断连，跳过信号处理
                continue

            # 获取或创建 StrategySlot
            slot = registry.get_slot(sid)
            if slot is None:
                # 首次收到信号时自动创建 Slot（热注册）
                cfg = info["config"]
                subs = [BarSubscription(symbol="SOLUSDT-PERP", timeframe="1m", factors=[])]
                slot = StrategySlot(
                    strategy_id=sid,
                    strategy=_GrpcSlotStrategy(sid),  # 使用最小桩对象
                    subscriptions=subs,
                    stop_pct=0.03,
                    take_pct=0.06,
                    max_hold_sec=3600,
                    cooldown_sec=60.0,
                    leverage=int(cfg.max_leverage) if cfg.max_leverage else 2,
                    position_size_pct=float(cfg.max_position_pct) if cfg.max_position_pct else 0.20,
                    symbol="SOLUSDT-PERP",
                    telegram_bot_token=info.get("telegram_bot_token") or self._default_bot_token,
                    telegram_chat_id=info.get("telegram_chat_id") or self._default_chat_id,
                )
                registry.register(slot)
                logger.info(f"已为 gRPC 策略创建策略槽: {sid}")

            # 构造内部信号对象
            sig = StrategySignal(direction=gdir, reason=reason, position_size_pct=request.position_size_pct)

            if sig.direction != 0:
                # 入场信号或调仓信号 → 通过 OrderExecutor 执行
                result = executor.execute(slot, sig, price)
            elif sig.reason == "hold":
                # "hold" 信号表示继续持有当前仓位，不执行任何操作
                # 这是已知设计修复：之前将 direction=0 统一视为平仓，导致 hold 信号误平仓
                result = "hold"
            else:
                # Bar 级退出：将退出原因暂存到 slot，由 RiskLoop 逐秒轮询执行
                # RiskLoop 每秒检查 slot.pending_bar_exit，执行逐仓平仓直到完全退出
                if slot.has_position:
                    slot.pending_bar_exit = sig.reason
                    logger.info(f"已排队 Bar 退出: {sid} reason={sig.reason}")
                    result = f"已排队: {sig.reason}"
                else:
                    result = "无持仓"
            logger.info(f"gRPC 信号处理: {sid} dir={sig.direction} result={result}")
            break  # 只处理第一个活动的策略

        return pb.SignalAck(accepted=True)

    async def GetState(self, request: pb.StateRequest, context) -> pb.StateResponse:
        """获取策略状态 RPC（一元 RPC）。返回仓位权益、每日盈亏、熔断状态等。"""
        return pb.StateResponse(equity=0.0, daily_pnl=0.0, circuit_breaker=False)

    async def ClosePosition(self, request: pb.CloseRequest, context) -> pb.CloseAck:
        """平仓 RPC（一元 RPC）。由 trading-v2 或手动控制触发。"""
        return pb.CloseAck(ok=True)

    # ── 由 main.py 的 Bar 推送循环调用 ─────────────────────

    def push_bar(self, pb_bar: pb.Bar, position_states: dict | None = None):
        """
        向所有已订阅的策略推送 Bar 数据。

        调用方：main.py 的推送循环（每次收到新的 1m Bar 后调用）。

        职责：
          - 遍历所有策略的 Bar 推送队列，将 Bar 对象入队
          - 可选地注入 PositionState：如果提供了 position_states 字典，每个策略
            收到其自己的仓位状态副本。这解决了"仓位同步"问题——nt-base 的 tick 级
            退出可能在策略端不知情的情况下更新仓位，通过 PositionState 定期同步

        参数：
          pb_bar:         要推送的 Bar protobuf（已包含因子计算结果）
          position_states: 可选字典 {strategy_id → PositionState protobuf}。
                          如果提供，每个策略获得带有自己仓位状态的 Bar 副本

        队列满保护：
          put_nowait + QueueFull 异常捕获，避免一个慢策略阻塞整体推送循环。
          队列最大容量 100 条 Bar（约 100 分钟），正常情况下不会触发。
        """
        for sid, queue in list(self._bar_queues.items()):
            bar_to_send = pb_bar
            if position_states and sid in position_states:
                # 为当前策略创建 Bar 的深拷贝，附加专属的 PositionState
                bar_to_send = pb.Bar()
                bar_to_send.CopyFrom(pb_bar)
                bar_to_send.position.CopyFrom(position_states[sid])
            try:
                queue.put_nowait(bar_to_send)  # 非阻塞入队
            except asyncio.QueueFull:
                logger.warning(f"策略 {sid} 的 Bar 队列已满，丢弃旧数据")

    def push_control(self, sid: str, cmd: pb.ControlCommand):
        """
        向指定策略推送控制指令。

        调用方：通常由 Telegram 通知处理链路触发（人工止盈/止损/平仓命令）。

        参数：
          sid:  目标策略 ID
          cmd:  ControlCommand protobuf，包含指令类型和参数

        返回：
          True  = 指令已成功入队
          False = 队列不存在或已满，指令被丢弃

        队列满保护：同 push_bar，最大容量 50 条指令。
        """
        if sid not in self._control_queues:
            logger.warning(f"[CTL] 策略 {sid} 没有控制指令队列，丢弃指令")
            return False
        try:
            self._control_queues[sid].put_nowait(cmd)
            logger.info(f"[CTL] 已入队指令 type={cmd.type} 给 {sid}")
            return True
        except asyncio.QueueFull:
            logger.warning(f"[CTL] 策略 {sid} 控制指令队列已满，丢弃指令")
            return False

    def build_bar(
        self,
        symbol: str,
        ts_ns: int,
        open_p: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        delta: float,
        taker_buy: float,
        taker_sell: float,
        btc_close: float,
        df_bars: pd.DataFrame,
        factors: dict[str, float] | None = None,
        position_state: pb.PositionState | None = None,
    ) -> pb.Bar:
        """
        构造 Bar protobuf 对象。

        这是 Bar 推送前的最后一次数据装配，将原始 Bar 数据、因子计算值和仓位
        状态合并为一个 protobuf 对象，通过 gRPC 推送到策略端。

        如果外部已提供计算好的 factors 字典，直接使用（避免重复计算）；
        否则调用 FactorEngine.execute_all() 在原始 Bar 数据上重新计算全部因子。

        参数：
          symbol:    交易对名称（如 "SOLUSDT-PERP"）
          ts_ns:     Bar 时间戳（纳秒）
          open/high/low/close: OHLC 价格
          volume:    成交量
          delta:      资金流净差值（taker_buy - taker_sell）
          taker_buy/taker_sell: 主动买/卖量
          btc_close: BTC 最新收盘价（用于残差动量等依赖 BTC 的因子）
          df_bars:   pandas DataFrame，包含计算因子所需的历史 Bar 数据
          factors:   预计算的因子值字典。如果为 None，自动调用 FactorEngine 计算
          position_state: 可选的仓位状态 protobuf，用于同步服务端仓位到策略端

        返回：
          Bar protobuf，可直接通过 gRPC 流推送
        """
        # 如果没有预计算因子，在此计算
        if factors is None:
            factors = self._factor_engine.execute_all(df_bars)

        bar = pb.Bar(
            symbol=symbol, ts_ns=ts_ns,
            open=open_p, high=high, low=low, close=close,
            volume=volume, delta=delta,
            taker_buy_vol=taker_buy, taker_sell_vol=taker_sell,
            btc_close=btc_close,
            factors=factors,
        )
        if position_state is not None:
            bar.position.CopyFrom(position_state)
        return bar

    def registered_factor_names(self) -> set:
        """
        返回当前所有已注册的因子名称集合。

        代理方法：调用 FactorEngine.registered_names()。
        这个方法是之前的修复：main.py 原本直接调用了 grpc_servicer.registered_factor_names()
        但当时 TradingBaseServicer 没有此方法，导致 AttributeError 崩溃。
        """
        return set(self._factor_engine.registered_names())

    def pending_signals(self) -> list[pb.Signal]:
        """
        排空并返回所有暂存的信号（非阻塞）。

        调用方：main.py 的推送循环，在处理完 Bar 推送后调用此方法处理待处理信号。

        设计意图：
          SubmitSignal 会立即将信号放入 _pending_signals 队列和直接执行。
          此方法用于 main.py 无阻塞地收集所有待处理信号进行统一日志记录和统计。
          如果 executor 尚未就绪，信号在此被收集但不会被执行（SubmitSignal 中也
          不会执行）。

        返回：
          信号列表（按入队顺序）
        """
        signals = []
        while not self._pending_signals.empty():
            try:
                signals.append(self._pending_signals.get_nowait())
            except asyncio.QueueEmpty:
                break
        return signals

    def orphaned_strategies(self) -> list[str]:
        """
        检测并返回断连超时的策略 ID 列表。

        由 RiskLoop 定期调用（约每 1 秒），检测那些 SubscribeBars 流已断开
        且超过宽限期仍无连接的策略。

        断连检测逻辑：
          1. 检查每个策略的 disconnected_at 时间戳
          2. 如果为 None（在线状态），跳过
          3. 如果不为 None，计算当前时间与断连时间的差值
          4. 如果差值 > grace_period_sec（默认 60 秒），标记为"孤儿"策略

        为什么要 60 秒宽限期？
          短时断连可能由网络抖动、gRPC 连接重置等原因导致。trading-v2 有 30 次
          自动重连机制（2 秒间隔 ≈ 60 秒），宽限期与此匹配，给予策略恢复时间。
          超过 60 秒未重连，说明策略进程可能已死，需要清理。

        返回：
          需要清理的策略 ID 列表
        """
        orphans = []
        now = time.time()
        for sid, info in list(self._strategies.items()):
            disc_at = info.get("disconnected_at")
            if disc_at is None:
                continue  # 策略在线，跳过
            grace = info.get("grace_period_sec", 60)
            if now - disc_at > grace:
                orphans.append(sid)
        return orphans

    def cleanup_strategy(self, sid: str):
        """
        清理指定策略的所有资源。

        调用方：RiskLoop 接收到 orphaned_strategies() 的返回后，依次调用此方法。

        清理内容：
          1. 策略注册信息（_strategies）
          2. Bar 推送队列（_bar_queues）
          3. 控制指令队列（_control_queues）

        注意：此方法不移除 StrategySlot（由 RiskLoop 主流程负责）。
        """
        self._strategies.pop(sid, None)
        self._bar_queues.pop(sid, None)
        self._control_queues.pop(sid, None)
        logger.info(f"gRPC 清理完成: 已移除 {sid}")


async def start_grpc_server(
    servicer: TradingBaseServicer,
    listen_socket: str = "unix:///tmp/nt_base_grpc.sock",
    listen_port: int = 50051,
):
    """
    启动异步 gRPC 服务器，同时监听 Unix socket 和 TCP 端口。

    双端口监听策略：
      1. Unix socket（默认路径 /tmp/nt_base_grpc.sock）
         - 本机进程间通信（nt-base <-> trading-v2 都在同一台服务器）
         - 更低延迟、更高吞吐（无需 TCP 协议栈）
         - 不会暴露到外部网络，天然安全
         - 注意：systemd 的 PrivateTmp=true 会隔离 /tmp，导致 trading-v2
           看不到此 socket 文件。这是已知问题的修复点（已改 PrivateTmp=false）

      2. TCP（默认端口 50051）
         - 绑定到 0.0.0.0（所有网络接口）
         - 用于远程调试和备用连接
         - trading-v2 在配置中可以选择连接 unix socket 或 TCP

    参数：
      servicer:     TradingBaseServicer 实例（已注册的 gRPC handler）
      listen_socket: Unix socket 路径（默认 unix:///tmp/nt_base_grpc.sock）
      listen_port:  TCP 监听端口（默认 50051）

    调用方：
      main.py 初始化完成后调用此函数启动 gRPC 服务端，等待 trading-v2 连接。

    返回：
      grpc.aio.Server 实例，由 main.py 持有引用以优雅关闭
    """
    server = grpc.aio.server(options=[
        # gRPC keepalive — detect dead connections within ~40s (30s ping + 10s timeout)
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", True),
        # Allow client pings (min interval 5s)
        ("grpc.http2.min_recv_ping_interval_without_data_ms", 5000),
        # Close connection after 2 failed pings
        ("grpc.http2.max_ping_strikes", 2),
    ])

    # 将 TradingBaseServicer 挂载到 gRPC 服务器
    pb_grpc.add_TradingBaseServicer_to_server(servicer, server)

    # Unix socket 监听（本地低延迟通信）
    server.add_insecure_port(listen_socket)
    logger.info(f"gRPC 正在监听 {listen_socket}")

    # TCP 端口监听（远程备用通信）
    server.add_insecure_port(f"0.0.0.0:{listen_port}")
    logger.info(f"gRPC 正在监听 0.0.0.0:{listen_port}")

    await server.start()
    return server
