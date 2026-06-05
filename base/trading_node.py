"""
Module:    base/trading_node
Purpose:   NautilusTrader TradingNode factory. Builds and configures the
           sandbox trading node with Binance Futures testnet connectivity.

Interface: build_trading_node(api_key, api_secret, leverage, initial_usdt) -> TradingNode

Configuration:
  - SandboxExecutionClient (paper trading, no real funds)
  - Binance USDT Futures testnet
  - NETTING OMS type (one position per symbol)
  - Default leverage configurable
  - Starting balance from SANDBOX_INITIAL_USDT env var
  - NO Binance data client (bars come from WebSocket aggregation + DataManageActor)

Security:
  API credentials passed as parameters (sourced from environment, never hardcoded).

Author:    nt-base system
Version:   1.0.0
"""
from __future__ import annotations
"""TradingNode builder — Binance data + sandbox execution."""
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

SYMBOL = "SOLUSDT-PERP.BINANCE"
VENUE = "BINANCE"


def build_trading_node(api_key: str, api_secret: str,
                       leverage: int = 2, initial_usdt: int = 1000) -> TradingNode:
    instr_provider = InstrumentProviderConfig(
        load_all=False, load_ids=frozenset([SYMBOL]),
    )
    node_cfg = TradingNodeConfig(
        trader_id=TraderId("NT-BASE-001"),
        logging=LoggingConfig(log_level="INFO"),
        data_engine=LiveDataEngineConfig(),
        risk_engine=LiveRiskEngineConfig(),
        exec_engine=LiveExecEngineConfig(reconciliation=True),
        data_clients={
            VENUE: BinanceDataClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                account_type=BinanceAccountType.USDT_FUTURES,
                instrument_provider=instr_provider,
                use_agg_trade_ticks=False,
            ),
        },
        exec_clients={
            VENUE: SandboxExecutionClientConfig(
                venue=VENUE,
                starting_balances=[f"{initial_usdt:.0f} USDT"],
                base_currency="USDT", account_type="MARGIN", oms_type="NETTING",
                default_leverage=Decimal(str(leverage)),
                instrument_provider=instr_provider,
            ),
        },
    )
    node = TradingNode(config=node_cfg)
    node.add_data_client_factory(VENUE, BinanceLiveDataClientFactory)
    node.add_exec_client_factory(VENUE, SandboxLiveExecClientFactory)
    return node
