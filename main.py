"""nt-base — trading base service entrypoint."""
from __future__ import annotations
import asyncio, sys, signal, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from shared.env import cfg, assert_required
from shared.log import setup_logging
from shared.db import get_pool, close_pool
from base.data_manage import DataManageActor, DataManageConfig
from base.trading_node import build_trading_node
from base.registry import StrategyRegistry
from base.executor import OrderExecutor
from risk.loop import RiskLoop
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.identifiers import InstrumentId, Venue

logger = setup_logging("nt_base")
VENUE_NAME = "BINANCE"
SYMBOL = f"{cfg.primary_symbol}-PERP.{VENUE_NAME}"


class BaseStrategy(Strategy):
    """NT Strategy that owns registry, executor, and risk loop."""

    def __init__(self, registry: StrategyRegistry):
        super().__init__()
        self._registry = registry
        self._executor = None
        self._risk_loop = None
        self._latest_price: dict[str, float] = {SYMBOL: 0.0}

    def on_start(self):
        sol_id = InstrumentId.from_str(SYMBOL)
        venue = Venue("BINANCE")
        self._executor = OrderExecutor(
            sol_id=sol_id, venue=venue,
            portfolio=self.portfolio,
            order_factory=self.order_factory,
            cache=self.cache,
        )
        self._risk_loop = RiskLoop(self._registry, self._executor)
        asyncio.create_task(self._risk_loop.start())
        self.log.info("BaseStrategy started: executor + risk_loop ready")

    def on_stop(self):
        if self._risk_loop:
            asyncio.create_task(self._risk_loop.stop())
        if self._executor:
            self._executor.flat_all(self._registry.all_slots(), "on_stop")
        self.log.info("BaseStrategy stopped")

    def get_executor(self):
        return self._executor

    def get_risk_loop(self):
        return self._risk_loop

    def update_price(self, symbol: str, price: float):
        self._latest_price[symbol] = price
        if self._risk_loop:
            self._risk_loop.update_price(symbol, price)


async def main():
    assert_required()
    pool = await get_pool()

    node = build_trading_node(
        api_key=cfg.binance.api_key,
        api_secret=cfg.binance.api_secret,
        leverage=2,
        initial_usdt=int(cfg.sandbox_initial_usdt),
    )

    # DataManageActor for Binance WS + persistence
    dm_config = DataManageConfig(
        instrument_ids=(SYMBOL,),
        bar_timeframes=("1-SECOND", "5-SECOND", "1-MINUTE"),
    )
    node.trader.add_actor(DataManageActor(dm_config))

    # Registry
    registry = StrategyRegistry()

    # Our BaseStrategy owns the executor and risk loop
    base_strat = BaseStrategy(registry)
    node.trader.add_strategy(base_strat)

    node.build()

    # ── Wire bar dispatch ──
    dm_actor = None
    for actor in node.trader._actors:
        try:
            name = type(actor).__name__
        except Exception:
            name = ""
        if "DataManage" in name or "DataManageActor" in name:
            dm_actor = actor
            break

    if dm_actor:
        _original_on_bar = dm_actor.on_bar

        def _on_bar_with_dispatch(bar):
            _original_on_bar(bar)
            iid = str(bar.bar_type.instrument_id)
            base_strat.update_price(iid, float(bar.close))

            if "1-MINUTE" not in str(bar.bar_type.spec):
                return

            slots = registry.get_slots("SOLUSDT-PERP", "1m")
            executor = base_strat.get_executor()
            if not slots or not executor:
                return

            for slot in slots:
                bar_data = {
                    "close": float(bar.close),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "ts_ns": bar.ts_event,
                    "factors": {},
                }
                signal = slot.strategy.on_bar(bar_data)
                if signal and signal.direction != 0:
                    result = executor.execute(slot, signal, float(bar.close))
                    logger.info(f"Signal: {slot.strategy_id} {signal.direction} -> {result}")

        dm_actor.on_bar = _on_bar_with_dispatch
        logger.info("Bar dispatch wired: DataManageActor -> strategies -> executor")

    # Graceful shutdown
    def _shutdown():
        logger.info("Shutdown signal received")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, lambda s, f: _shutdown())
        except Exception:
            pass

    logger.info(f"nt-base running: mode={cfg.mode} symbol={SYMBOL}")

    try:
        await node.run_async()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        node.dispose()
        await close_pool()
        logger.info("nt-base stopped")


if __name__ == "__main__":
    asyncio.run(main())
