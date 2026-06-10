"""
Module:    main (nt-base entrypoint)
Purpose:   Trading Base Service 鈥?the persistent runtime hosting dynamic
           trading strategies. Connects to Binance Futures testnet,
           manages data subscriptions, factor computation, bar dispatch,
           and dynamic strategy registration.

Execution Flow:
  1. assert_required()         鈥?validate environment secrets
  2. get_pool()                鈥?connect to TimescaleDB
  3. build_trading_node()      鈥?create NT TradingNode (sandbox)
  4. DataManageActor           鈥?subscribe bars/ticks/L2/OI + persist to DB
  5. BaseStrategy(NT)          鈥?owns OrderExecutor + RiskLoop
  6. gRPC Server               鈥?unix socket + TCP, strategy registration
  7. Bar dispatch monkey-patch 鈥?intercepts dm_actor.on_bar for factor computation
                                 and strategy signal dispatch

Bar Dispatch (monkey-patched dm_actor.on_bar):
  Every bar (1s/5s/1m) -> update price -> buffer OHLC
  Every 1m bar -> compute factors -> dispatch to registered strategy slots
  Signal != 0 -> OrderExecutor.execute(slot, signal, price)

Dynamic Registration:
  Strategies register via gRPC Register() at startup.
  Registration is in-memory only ??no DB persistence needed.
  When nt-base restarts, strategies reconnect and re-register.

Shutdown:
  SIGTERM -> flat_all positions -> deregister strategies -> close DB pool

Logging:
  Dual output: systemd journal (via stdout) + /root/nt-base/logs/nt_base.log

Author:    nt-base system
Version:   2.0.0 (dynamic registration)
"""
from __future__ import annotations
"""nt-base — trading base service entrypoint."""
import asyncio
import sys
import signal
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
from risk.tick_exit import TickExitManager
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.identifiers import InstrumentId, Venue
import trading_base_pb2 as pb

# gRPC server (strategy communication)
from base.grpc_server import TradingBaseServicer, start_grpc_server

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
            submit_order=self.submit_order,
            cache=self.cache,
            order_factory=self.order_factory,
        )
        self._risk_loop = RiskLoop(self._registry, self._executor)
        asyncio.create_task(self._risk_loop.start())
        gs = getattr(self, "_grpc_servicer", None)
        if gs:
            gs.set_execution_context(
                executor=self._executor,
                registry=self._registry,
                get_price=lambda: self._latest_price.get(SYMBOL, 0.0),
            )
            self.log.info("gRPC execution context wired")
        asyncio.create_task(self._cleanup_pending_loop())
        self.log.info("BaseStrategy started: executor + risk_loop ready")

    async def _cleanup_pending_loop(self):
        """Periodically clean stale pending fill notifications."""
        while True:
            await asyncio.sleep(10)
            if self._executor:
                n = self._executor.cleanup_pending()
                if n:
                    self.log.warning(f"Cleaned {n} stale pending notifications")

    def on_order_filled(self, event):
        """Route NT fill events to executor for actual-fill-price notifications."""
        if self._executor:
            cid = str(event.client_order_id)
            commission = float(event.commission.as_decimal()) if event.commission else 0.0
            self._executor.on_fill(cid, float(event.last_px), float(event.last_qty), commission)

    def on_order_canceled(self, event):
        """IOC remainder canceled — accept whatever quantity filled."""
        if self._executor:
            cid = str(event.client_order_id)
            self._executor.accept_partial_fill(cid)

    def on_order_expired(self, event):
        """Order expired — accept whatever quantity filled."""
        if self._executor:
            cid = str(event.client_order_id)
            self._executor.accept_partial_fill(cid)

    def on_stop(self):
        if self._risk_loop:
            asyncio.create_task(self._risk_loop.stop())
        if self._executor:
            self._executor.flush_pending()
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

    # Pre-fill bar buffer from historical tick data (avoids cold-start wait)
    from prefill_bar_buffer import prefill_bar_buffer
    _bar_buffer, _latest_btc_close = await prefill_bar_buffer(pool, 300)
    logger.info(f"Buffer pre-filled: {len(_bar_buffer)} bars, latest_btc={_latest_btc_close:.2f}")
    _tick_exit_managers: dict[str, TickExitManager] = {}

    node = build_trading_node(
        api_key=cfg.binance.api_key,
        api_secret=cfg.binance.api_secret,
        leverage=2,
        initial_usdt=int(cfg.sandbox_initial_usdt),
    )

    BTC_SYMBOL = f"BTCUSDT-PERP.{VENUE_NAME}"

    dm_config = DataManageConfig(
        instrument_ids=(SYMBOL, BTC_SYMBOL),
        tick_instrument_ids=(SYMBOL, BTC_SYMBOL),  # DB needs BTC ticks, but dispatch filters them
        bar_timeframes=("1-SECOND", "5-SECOND", "1-MINUTE"),
    )
    node.trader.add_actor(DataManageActor(dm_config))

    # Registry
    registry = StrategyRegistry()

    # Our BaseStrategy owns the executor and risk loop
    base_strat = BaseStrategy(registry)
    node.trader.add_strategy(base_strat)

    node.build()

    # Start gRPC server for strategy communication
    grpc_servicer = TradingBaseServicer(
        telegram_bot_token=cfg.telegram.bot_token,
        telegram_chat_id=str(cfg.telegram.admin_chat_id),
    )
    grpc_server = await start_grpc_server(grpc_servicer)
    logger.info("gRPC server started (unix:///tmp/nt_base_grpc.sock + :50051)")

    # Wire gRPC servicer to base strategy for signal execution
    base_strat._grpc_servicer = grpc_servicer


    # ── Wire bar dispatch ──
    # Find DataManageActor — NT stores actors in trader._actors (list or dict)
    dm_actor = None
    actors_container = getattr(node.trader, "_actors", None)
    if actors_container is None:
        logger.warning("node.trader._actors not found, trying _components")
        actors_container = getattr(node.trader, "_components", [])

    if isinstance(actors_container, dict):
        for actor in actors_container.values():
            if "DataManageActor" in type(actor).__name__:
                dm_actor = actor
                break
    elif hasattr(actors_container, "__iter__"):
        for actor in actors_container:
            if "DataManageActor" in type(actor).__name__:
                dm_actor = actor
                break

    logger.info(f"dm_actor lookup: found={dm_actor is not None}, actors_count={len(actors_container) if hasattr(actors_container, '__len__') else '?'}")

    from collections import deque
    # _bar_buffer initialized via prefill_bar_buffer() above
    _running_buyer_vol: float = 0.0
    _running_seller_vol: float = 0.0
    # _latest_btc_close initialized via prefill above

    if dm_actor:
        _original_on_bar = dm_actor.on_bar
        _original_on_trade_tick = dm_actor.on_trade_tick

        def _on_trade_tick_with_accum(tick):
            _original_on_trade_tick(tick)
            iid = str(tick.instrument_id)
            if 'SOLUSDT' not in iid:
                return  # BTC tick: skip delta accumulation
            nonlocal _running_buyer_vol, _running_seller_vol
            size = float(tick.size)
            if tick.aggressor_side.name == "BUYER":
                _running_buyer_vol += size
            else:
                _running_seller_vol += size

        dm_actor.on_trade_tick = _on_trade_tick_with_accum

        # Dispatch SOL ticks to tick-level exit manager (L2 trailing, L3 breakeven)
        _original_on_tick = dm_actor.on_trade_tick
        def _on_trade_tick_with_exit_check(tick):
            _original_on_tick(tick)
            symbol = tick.instrument_id.symbol.value
            if symbol != 'SOLUSDT-PERP':
                return
            tick_price = float(tick.price)
            base_strat.update_price(symbol, tick_price)

            nonlocal _tick_exit_managers
            for slot in registry.all_slots():
                sid = slot.strategy_id
                if slot.has_position:
                    tem = _tick_exit_managers.get(sid)
                    if tem is None:
                        tem = TickExitManager()
                        tem.open_position(slot.entry_price, slot.entry_side == "LONG", symbol)
                        if slot.current_atr > 0:
                            tem.update_atr(slot.current_atr)
                        _tick_exit_managers[sid] = tem
                    result = tem.on_tick(
                        tick_price, float(tick.size),
                        tick.aggressor_side.name == 'BUYER',
                        tick.ts_event, symbol,
                    )
                    if result is not None:
                        exc = base_strat.get_executor()
                        exc.flat(slot, result.reason)
                        tem.close_position()
                        del _tick_exit_managers[sid]
                else:
                    if sid in _tick_exit_managers:
                        _tick_exit_managers[sid].close_position()
                        del _tick_exit_managers[sid]

        dm_actor.on_trade_tick = _on_trade_tick_with_exit_check

        def _on_bar_with_dispatch(bar):
            _original_on_bar(bar)
            iid = str(bar.bar_type.instrument_id)
            base_strat.update_price(iid, float(bar.close))

            if "BTCUSDT" in iid:
                if "1-MINUTE" in str(bar.bar_type.spec):
                    nonlocal _latest_btc_close, _tick_exit_managers
                    _latest_btc_close = float(bar.close)
                return

            if "1-MINUTE" not in str(bar.bar_type.spec):
                return

            # Snapshot accumulated tick volumes for this bar, then reset
            nonlocal _running_buyer_vol, _running_seller_vol
            buyer_vol = _running_buyer_vol
            seller_vol = _running_seller_vol
            _running_buyer_vol = 0.0
            _running_seller_vol = 0.0
            delta = buyer_vol - seller_vol
            volume = buyer_vol + seller_vol

            if len(_bar_buffer) % 5 == 0:
                logger.info(f"bar_buffer stats: len={len(_bar_buffer)} volume={volume:.2f} delta={delta:.2f} btc_close={'%.2f' % _latest_btc_close if _latest_btc_close > 0 else 'pending'}")

            # Buffer only 1-minute bars for factor computation
            _bar_buffer.append({
                "ts": bar.ts_event,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": volume,
                "delta": delta,
                "taker_buy_volume": buyer_vol,
                "taker_sell_volume": seller_vol,
                "btc_close": _latest_btc_close if _latest_btc_close > 0 else None,
            })

            executor = base_strat.get_executor()

            # Compute ATR from bar buffer for tick-level exits
            import numpy as np
            if len(_bar_buffer) >= 30:
                recent_bars = list(_bar_buffer)[-30:]
                highs = np.array([b['high'] for b in recent_bars], dtype=float)
                lows = np.array([b['low'] for b in recent_bars], dtype=float)
                _current_atr = float(np.mean(highs - lows))
                for s in registry.all_slots():
                    s.current_atr = _current_atr
                for tem in _tick_exit_managers.values():
                    tem.update_atr(_current_atr)

            # Factor computation + gRPC bar push (single unified path).
            # FactorEngine (fed by trading-v2 via gRPC Register) is the
            # sole factor executor — no local factor/compute.py fallback.
            if grpc_servicer and len(_bar_buffer) >= 30:
                import pandas as pd
                df = pd.DataFrame(list(_bar_buffer))
                df["ts"] = pd.to_datetime(df["ts"])
                df = df.set_index("ts")
                for col in ["delta", "taker_buy_volume", "taker_sell_volume", "btc_close"]:
                    if col not in df.columns:
                        df[col] = 0.0

                # Compute factors once via FactorEngine
                factors = grpc_servicer._factor_engine.execute_all(df)
                logger.info(f"factors computed: {factors}")

                # Build and push Bar with pre-computed factors
                pb_bar = grpc_servicer.build_bar(
                    symbol=SYMBOL, ts_ns=bar.ts_event,
                    open_p=float(bar.open), high=float(bar.high),
                    low=float(bar.low), close=float(bar.close),
                    volume=volume, delta=delta,
                    taker_buy=buyer_vol, taker_sell=seller_vol,
                    btc_close=_latest_btc_close,
                    df_bars=df,
                    factors=factors,
                )
                # Build per-strategy position states so trading-v2
                # knows the authoritative position (managed by nt-base tick exits)
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

                grpc_servicer.push_bar(pb_bar, position_states=position_states)
            else:
                factors = {}

            confidence = factors.get("trend_confidence", 0.0)

            slots = registry.all_slots()
            for slot in slots:
                bar_data = {
                    "close": float(bar.close),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "ts_ns": bar.ts_event,
                    "factors": factors,
                }
                signal = slot.strategy.on_bar(bar_data)
                if signal is not None:
                    slot.confidence = confidence
                    if signal.direction != 0:
                        was_in_position = slot.has_position
                        result = executor.execute(slot, signal, float(bar.close))
                        sid = slot.strategy_id
                        if slot.has_position:
                            tem = _tick_exit_managers.get(sid)
                            if tem is None:
                                tem = TickExitManager()
                                _tick_exit_managers[sid] = tem
                            if was_in_position and result == "pyramid":
                                # Pyramid add: only update VWAP, preserve trailing anchors
                                tem.add_position(slot.entry_price)
                            else:
                                # First entry: full position initialization
                                tem.open_position(slot.entry_price, slot.entry_side == "LONG", "SOLUSDT-PERP")
                            if slot.current_atr > 0:
                                tem.update_atr(slot.current_atr)
                    elif signal.reason == "hold":
                        result = "hold"
                    else:
                        result = str(executor.flat(slot, signal.reason))
                        sid = slot.strategy_id
                        if sid in _tick_exit_managers:
                            _tick_exit_managers[sid].close_position()
                            del _tick_exit_managers[sid]
                    logger.info(f"Signal: {slot.strategy_id} dir={signal.direction} reason={signal.reason} result={result}")

            # ── Cleanup orphaned strategies (disconnected past grace period) ──
            if grpc_servicer:
                orphans = grpc_servicer.orphaned_strategies()
                for sid in orphans:
                    slot = registry.get_slot(sid)
                    if slot is None:
                        grpc_servicer.cleanup_strategy(sid)
                        logger.warning(f"Orphan {sid}: cleaned up (no slot)")
                        continue
                    if slot.has_position:
                        executor.flat(slot, "strategy_disconnected")
                        logger.warning(
                            f"Orphan {sid}: flattening position ({slot.entry_side} "
                            f"{slot.held_sec:.0f}s held) on disconnect"
                        )
                        # Don't cleanup yet — wait for fill to confirm close
                    else:
                        registry.unregister(sid)
                        grpc_servicer.cleanup_strategy(sid)
                        if sid in _tick_exit_managers:
                            _tick_exit_managers[sid].close_position()
                            del _tick_exit_managers[sid]
                        logger.warning(f"Orphan {sid}: cleaned up (no position)")

        dm_actor.on_bar = _on_bar_with_dispatch
        logger.info("Bar dispatch wired: DataManageActor -> factors -> strategies -> executor")

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
        await grpc_server.stop(grace=5.0)
        logger.info("gRPC server stopped")
        node.dispose()
        await close_pool()
        logger.info("nt-base stopped")


if __name__ == "__main__":
    asyncio.run(main())
