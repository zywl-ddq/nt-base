# -*- coding: utf-8 -*-
"""
===========================================================
模块:    base/trading_node
模块名:  交易节点构建器
===========================================================
用途:    NautilusTrader TradingNode 工厂函数。
         构建并配置用于 Binance USDT 期货测试网 (testnet) 沙盘交易的节点。

接口: build_trading_node(api_key, api_secret, leverage, initial_usdt) -> TradingNode

配置说明:
  - SandboxExecutionClient: 沙盘执行客户端 (模拟交易，不涉及真实资金)
  - BinanceDataClientConfig: Binance USDT 期货数据客户端配置
    - use_agg_trade_ticks=False: 不使用聚合逐笔成交 (使用原始 trade tick)
  - NETTING OMS 类型: 每个品种只有一个持仓 (无多空双向)
  - 默认杠杆可配置
  - 初始资金来自 SANDBOX_INITIAL_USDT 环境变量

架构说明:
  - 不使用 Binance 的数据客户端提供的 bar (native Binance klines)
    Bar 数据由 DataManageActor 通过 WebSocket trade tick 聚合 (INTERNAL) 提供
  - 执行端使用沙盘模式，所有订单在本地模拟器中执行，不发送到交易所
  - 仅交易 SOLUSDT-PERP 和观察 BTCUSDT-PERP (用于因子计算)

安全性:
  API 凭证通过参数传入 (从环境变量读取，绝不硬编码)。

作者:    nt-base 系统
版本:    1.0.0
===========================================================
"""
from __future__ import annotations

from decimal import Decimal

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.factories import BinanceLiveDataClientFactory
from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig
from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
from nautilus_trader.config import (
    LiveDataEngineConfig, LiveExecEngineConfig,
    LiveRiskEngineConfig, LoggingConfig, InstrumentProviderConfig,
)
from nautilus_trader.live.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId


# ── 交易品种常量 ──────────────────────────────────────────────
# NT 格式: <基础币种><计价币种>-<合约类型>.<交易所>
SYMBOL = "SOLUSDT-PERP.BINANCE"       # 交易标的: SOL 永续合约
BTC_SYMBOL = "BTCUSDT-PERP.BINANCE"   # 观察标的: BTC (用于因子计算，不下单)
VENUE = "BINANCE"                     # 交易所名称标识


def build_trading_node(api_key: str, api_secret: str,
                       leverage: int = 2, initial_usdt: int = 1000) -> TradingNode:
    """构建并返回一个已配置的 NautilusTrader TradingNode 实例。

    这是系统唯一的交易节点工厂函数。节点配置了：
    - Binance 数据客户端 (用于获取品种信息和 WebSocket 行情)
    - 沙盘执行客户端 (所有订单在本地模拟执行)
    - NETTING OMS 类型 (每个品种单一持仓)

    Args:
        api_key: Binance API key (从环境变量 BINANCE_API_KEY 读取)
        api_secret: Binance API secret (从环境变量 BINANCE_API_SECRET 读取)
        leverage: 默认杠杆倍数 (默认 2x)
        initial_usdt: 沙盘初始 USDT 余额 (默认 1000)

    Returns:
        已配置好的 TradingNode 实例，可直接通过 node.run() 启动

    Notes:
        - SandboxExecutionClient: 使用 NautilusTrader 内置的沙盘执行客户端
          所有委托 (order) 在本地模拟执行，不发送到 Binance 实际市场。
          这是测试网 (testnet) 模式，不涉及真实资金。
        - NETTING OMS (净额订单管理系统): 每个品种同时只能有一个方向的持仓。
          当持有多头时开空头，会先平多再开空。这是 Binance USDT 永续合约
          的实际模式。另一种是 HEDGING (对冲模式)，允许同时持有多头和空头。
        - BinanceDataClientConfig 中 use_agg_trade_ticks=False:
          不使用聚合逐笔成交 (aggTrade)，使用原始 trade tick 进行 bar 聚合。
        - reconciliation=True: 启用执行引擎的重置 Reconciliation，
          重启时与沙盘状态对齐。
    """

    # ── 品种信息提供器配置 ──────────────────────────────────────
    # 只加载 SOL 和 BTC 两个品种的合约信息
    instr_provider = InstrumentProviderConfig(
        load_all=False,                           # 不加载所有品种
        load_ids=frozenset([SYMBOL, BTC_SYMBOL]), # 只加载这两个品种
    )

    # ── 交易节点配置 ────────────────────────────────────────────
    node_cfg = TradingNodeConfig(
        # 交易者 ID: 系统中所有策略共享此 ID
        trader_id=TraderId("NT-BASE-001"),
        # 日志级别: INFO (生产环境)
        logging=LoggingConfig(log_level="INFO"),
        # 数据引擎: 默认配置
        data_engine=LiveDataEngineConfig(),
        # 风控引擎: 默认配置
        risk_engine=LiveRiskEngineConfig(),
        # 执行引擎: 启用 Reconciliation (重启时同步状态)
        exec_engine=LiveExecEngineConfig(reconciliation=True),
        # ── 数据客户端 ──────────────────────────────────────────
        # BinanceDataClientConfig: 用于连接 Binance WebSocket
        # 获取实时行情数据 (tick, L2, 资金费率等)
        # 注意: bar 数据不由这个客户端提供 (由 DataManageActor 从 tick 聚合)
        data_clients={
            VENUE: BinanceDataClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                account_type=BinanceAccountType.USDT_FUTURES,  # USDT 永续合约
                instrument_provider=instr_provider,
                use_agg_trade_ticks=False,  # 使用原始 tick，非聚合 tick
            ),
        },
        # ── 执行客户端 ──────────────────────────────────────────
        # SandboxExecutionClientConfig: 沙盘执行客户端
        #
        # 为什么使用沙盘而非真实执行？
        # 1. 系统运行在测试网 (testnet) 模式，不涉及真实资金
        # 2. 沙盘执行在本地模拟订单簿和成交，不需要发送到 Binance
        # 3. 使用 NETTING OMS 类型，与 Binance USDT 永续合约的实际模式一致
        # 4. 初始余额通过 starting_balances 设置
        exec_clients={
            VENUE: SandboxExecutionClientConfig(
                venue=VENUE,
                starting_balances=[f"{initial_usdt:.0f} USDT"],
                base_currency="USDT",
                account_type="MARGIN",     # 保证金账户类型
                oms_type="NETTING",        # NETTING OMS: 每个品种单一持仓
                default_leverage=Decimal(str(leverage)),
                instrument_provider=instr_provider,
            ),
        },
    )

    # ── 构建节点 ────────────────────────────────────────────────
    node = TradingNode(config=node_cfg)

    # 注册客户端工厂
    # BinanceLiveDataClientFactory: 根据 BinanceDataClientConfig 创建数据客户端
    # SandboxLiveExecClientFactory: 根据 SandboxExecutionClientConfig 创建执行客户端
    node.add_data_client_factory(VENUE, BinanceLiveDataClientFactory)
    node.add_exec_client_factory(VENUE, SandboxLiveExecClientFactory)

    return node
