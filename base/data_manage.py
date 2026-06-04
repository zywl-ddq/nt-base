"""DataManageActor — sole data subscription owner + TimescaleDB persistence.

Subscribes to ALL bar types (1s/5s/1m), ticks, orderbook, funding via the
DataEngine. Batch-writes market & execution data to TimescaleDB.

This is the SINGLE active data subscriber. Strategies listen passively
on the MessageBus for the bar topics this Actor publishes to.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
from nautilus_trader.common.actor import Actor, ActorConfig
from nautilus_trader.model.data import Bar, BarType, TradeTick
from nautilus_trader.model.events import (
    OrderEvent,
    OrderFilled,
    PositionChanged,
    PositionClosed,
    PositionEvent,
    PositionOpened,
)
from nautilus_trader.model.identifiers import InstrumentId

from shared.env import cfg

logger = logging.getLogger(__name__)


# ─── Config ──────────────────────────────────────────────────────────


class DataManageConfig(ActorConfig, frozen=True):
    """Data manage config: instruments, timeframes, flush settings.

    DataManageActor is the sole active subscriber for all bar types.
    Strategies listen passively on msgbus — no subscriber conflicts.
    """

    instrument_ids: tuple[str, ...]
    bar_timeframes: tuple[str, ...] = ("1-SECOND", "5-SECOND", "1-MINUTE")
    flush_interval_sec: float = 5.0
    max_buffer: int = 1000
    collect_l2: bool = True
    l2_snapshot_interval_sec: float = 1.0
    collect_oi: bool = True
    oi_poll_interval_sec: float = 30.0


# ─── Helpers ─────────────────────────────────────────────────────────


def _ns_to_dt(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1_000_000_000.0, tz=timezone.utc)


_AGG_SUFFIX = {
    "SECOND": "s", "MINUTE": "m", "HOUR": "h", "DAY": "d",
    "MILLISECOND": "ms", "WEEK": "w", "MONTH": "M", "YEAR": "y",
}


def _bar_tf_label(bar_type: BarType) -> str:
    """Human-friendly timeframe: '1s', '5s', '1m'.

    Parses str(spec) which has form '<step>-<AGG>-<PRICE>'.
    """
    s = str(bar_type.spec)  # e.g. '5-SECOND-LAST'
    parts = s.split("-")
    if len(parts) >= 2:
        step, agg = parts[0], parts[1]
        return f"{step}{_AGG_SUFFIX.get(agg, agg.lower()[:1])}"
    return s


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return json.dumps(str(obj))


# ─── Actor ───────────────────────────────────────────────────────────


class DataManageActor(Actor):
    """Persists NT events into TimescaleDB."""

    def __init__(self, config: DataManageConfig) -> None:
        super().__init__(config)
        self._cfg: DataManageConfig = config

        self._pool: asyncpg.Pool | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._flush_task: asyncio.Task | None = None
        self._funding_task: asyncio.Task | None = None
        self._running = False

        # Buffers
        self._bar_buf: list[tuple] = []
        self._tick_buf: list[tuple] = []
        self._funding_buf: list[tuple] = []
        self._l2_buf: list[tuple] = []
        self._oi_buf: list[tuple] = []

        # Counters
        self._n_bars = 0
        self._n_ticks = 0
        self._n_funding = 0
        self._n_order_events = 0
        self._n_position_events = 0
        self._n_l2 = 0
        self._n_oi = 0
        self._books: dict[str, dict] = {}
        self._l2_snapshot_task: asyncio.Task | None = None
        self._oi_task: asyncio.Task | None = None

    # ── lifecycle ────────────────────────────────────────────────

    def on_start(self) -> None:
        self.log.info(f"DataManageActor starting (instruments={self._cfg.instrument_ids})")
        self._loop = asyncio.get_event_loop()
        self._running = True

        # Schedule async pool creation
        self._loop.create_task(self._async_init())

        # Subscribe to TICKS via the standard API — ticks have no aggregator
        # contention with strategies (each subscriber gets independent
        # dispatch) so this is safe.
        for s in self._cfg.instrument_ids:
            iid = InstrumentId.from_str(s)
            self.subscribe_trade_ticks(iid)

        # CRITICAL: Do NOT call subscribe_bars(). NT 1.227 binds each
        # BarType to a single Aggregator and refuses a second subscriber
        # with a "currently in use, subscription can't be started" warn —
        # which silently stops bar dispatch to whoever subscribed second.
        # When this Actor was registered before the Strategy (as in
        # Controller._boot_node), the Strategy ended up with no on_bar
        # callbacks and produced ZERO trades for hours.
        #
        # Fix: passively listen on the MessageBus topic the Strategy's
        # subscription will publish to. We get a copy of every bar
        # without owning the aggregator.
        # Controller (via DataManageConfig) decides WHAT bar types to produce.
        # We execute subscribe_bars() because NT requires an Actor to send
        # this command; Controller cannot call it directly. All consumers
        # (including ourselves) listen passively on msgbus.
        for s in self._cfg.instrument_ids:
            for tf in self._cfg.bar_timeframes:
                tf_u = tf.upper()
                # INTERNAL (NT aggregates from ticks) for sub-minute bars
                # EXTERNAL (Binance provides pre-built klines) for minute+ bars
                src = "EXTERNAL" if ("MINUTE" in tf_u or "HOUR" in tf_u or "DAY" in tf_u) else "INTERNAL"
                bt_str = f"{s}-{tf_u}-LAST-{src}"
                bt = BarType.from_str(bt_str)
                topic = f"data.bars.{bt}"
                try:
                    self.subscribe_bars(bt)
                    self.log.info(f"subscribed to bars: {bt}")
                except Exception as e:
                    self.log.warning(f"subscribe_bars {bt} failed: {e}")

        # Subscribe to order/position events via msgbus wildcard
        try:
            self.msgbus.subscribe("events.order.*", self._on_order_event)
            self.msgbus.subscribe("events.position.*", self._on_position_event)
        except Exception as e:
            self.log.warning(f"msgbus wildcard subscribe failed: {e}; falling back to on_event")

        if self._cfg.collect_l2:
            for s in self._cfg.instrument_ids:
                iid = InstrumentId.from_str(s)
                self._books[s] = {"bids": {}, "asks": {}}
                self.subscribe_order_book_deltas(iid)
                self.log.info(f"L2 subscribed: {iid}")

    def on_stop(self) -> None:
        self.log.info(
            f"DataManageActor stopping. Counters: "
            f"bars={self._n_bars} ticks={self._n_ticks} "
            f"funding={self._n_funding} l2={self._n_l2} "
            f"oi={self._n_oi} orders={self._n_order_events} "
            f"positions={self._n_position_events}"
        )
        self._running = False
        if self._flush_task and self._loop:
            self._loop.create_task(self._final_flush_and_close())

    async def _async_init(self) -> None:
        try:
            self._pool = await asyncpg.create_pool(
                dsn=cfg.timescale.dsn, min_size=2, max_size=8
            )
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            self.log.info("DataManage asyncpg pool ready")
        except Exception as e:
            self.log.error(f"DataManage pool init failed: {e}")
            return

        self._flush_task = self._loop.create_task(self._flush_loop())
        self._funding_task = self._loop.create_task(self._funding_poll_loop())
        if self._cfg.collect_l2:
            self._l2_snapshot_task = self._loop.create_task(self._l2_snapshot_loop())
        if self._cfg.collect_oi:
            self._oi_task = self._loop.create_task(self._oi_poll_loop())

    async def _funding_poll_loop(self) -> None:
        """Two-tier funding poll, semantically clean:

        Tier A (every 5 min) — fetch *settled* rate history (8h boundaries)
            via Binance fundingRateHistory. These rows are written with
            kind='settled' and are the ONLY rows that backtest /
            funding_accountant should consider for PnL accounting.

        Tier B (every 60s) — fetch the current premium-index *snapshot* (a
            forward-looking observation, NOT a settlement). Written with
            kind='snapshot'. Used only for monitoring.

        Pre-P0-3 the loop wrote every 60s premium-index point as a normal
        funding row, polluting the series and producing wildly inflated
        funding-cost estimates in backtests.
        """
        try:
            import ccxt.async_support as ccxt_async
        except Exception as e:
            self.log.warning(f"ccxt not available, skipping funding poller: {e}")
            return
        ex = ccxt_async.binance({"options": {"defaultType": "future"}})
        # Map NT instrument -> (ccxt_symbol, db_symbol)
        ccxt_symbols: dict[str, tuple[str, str]] = {}
        for s in self._cfg.instrument_ids:
            base = s.split("-PERP")[0]                   # SOLUSDT
            if base.endswith("USDT"):
                ccxt_symbols[s] = (f"{base[:-4]}/USDT:USDT", s.split(".")[0])

        last_settled_fetch = 0.0  # epoch seconds of last history pull
        SETTLED_INTERVAL = 300.0  # 5 min between history pulls

        try:
            while self._running:
                await asyncio.sleep(60)
                if not self._pool:
                    continue
                now_epoch = asyncio.get_event_loop().time()

                # ── Tier A: settled history ────────────────────────────
                if now_epoch - last_settled_fetch >= SETTLED_INTERVAL:
                    last_settled_fetch = now_epoch
                    settled_rows: list[tuple] = []
                    for nt_sym, (csym, db_sym) in ccxt_symbols.items():
                        try:
                            # since = now - 24h, limit=10 covers >24h of 8h boundaries
                            since_ms = int(
                                (datetime.now(timezone.utc).timestamp() - 86400) * 1000
                            )
                            hist = await ex.fetch_funding_rate_history(
                                csym, since=since_ms, limit=10
                            )
                            for h in hist:
                                ts_ms = h.get("timestamp")
                                rate = h.get("fundingRate")
                                if ts_ms is None or rate is None:
                                    continue
                                ts = datetime.fromtimestamp(
                                    ts_ms / 1000.0, tz=timezone.utc
                                )
                                settled_rows.append(
                                    (db_sym, ts, Decimal(str(rate)),
                                     None, None, None, "settled")
                                )
                        except Exception as e:
                            self.log.warning(
                                f"funding history {csym} failed: {e}"
                            )
                    if settled_rows:
                        try:
                            async with self._pool.acquire() as conn:
                                await conn.executemany(
                                    """INSERT INTO funding
                                         (symbol,ts,rate,mark_price,index_price,
                                          next_funding_time,kind)
                                       VALUES ($1,$2,$3,$4,$5,$6,$7)
                                       ON CONFLICT (symbol,ts,kind) DO NOTHING""",
                                    settled_rows,
                                )
                            self._n_funding += len(settled_rows)
                        except Exception as e:
                            self.log.error(f"settled funding insert failed: {e}")

                # ── Tier B: snapshot (premium index) ──────────────────
                snap_rows: list[tuple] = []
                for nt_sym, (csym, db_sym) in ccxt_symbols.items():
                    try:
                        fr = await ex.fetch_funding_rate(csym)
                        ts = datetime.fromtimestamp(
                            (fr.get("timestamp") or 0) / 1000.0, tz=timezone.utc
                        ) if fr.get("timestamp") else datetime.now(timezone.utc)
                        rate = Decimal(str(fr.get("fundingRate") or 0))
                        mark = Decimal(str(fr.get("markPrice") or 0)) if fr.get("markPrice") else None
                        idx = Decimal(str(fr.get("indexPrice") or 0)) if fr.get("indexPrice") else None
                        nft = fr.get("fundingDatetime")
                        nft_dt = None
                        if nft:
                            try:
                                nft_dt = datetime.fromisoformat(nft.replace("Z", "+00:00"))
                            except Exception:
                                nft_dt = None
                        snap_rows.append(
                            (db_sym, ts, rate, mark, idx, nft_dt, "snapshot")
                        )
                    except Exception as e:
                        self.log.warning(f"funding snapshot {csym} failed: {e}")
                if snap_rows:
                    try:
                        async with self._pool.acquire() as conn:
                            await conn.executemany(
                                """INSERT INTO funding
                                     (symbol,ts,rate,mark_price,index_price,
                                      next_funding_time,kind)
                                   VALUES ($1,$2,$3,$4,$5,$6,$7)
                                   ON CONFLICT (symbol,ts,kind) DO NOTHING""",
                                snap_rows,
                            )
                    except Exception as e:
                        self.log.error(f"snapshot funding insert failed: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            try:
                await ex.close()
            except Exception:
                pass

    async def _flush_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._cfg.flush_interval_sec)
                await self._flush_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"flush_loop error: {e}")

    async def _final_flush_and_close(self) -> None:
        # Cancel funding poller first so it can't write after pool closes
        for t in (self._funding_task, self._flush_task):
            if t is not None and not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        await self._flush_all()
        if self._pool:
            await self._pool.close()
        self.log.info("DataManage pool closed")

    # ── data handlers ────────────────────────────────────────────

    def on_bar(self, bar: Bar) -> None:
        try:
            tf = _bar_tf_label(bar.bar_type)
            symbol = bar.bar_type.instrument_id.symbol.value  # e.g. SOLUSDT-PERP
            ts = _ns_to_dt(bar.ts_event)
            self._bar_buf.append(
                (
                    symbol,
                    tf,
                    ts,
                    Decimal(str(bar.open)),
                    Decimal(str(bar.high)),
                    Decimal(str(bar.low)),
                    Decimal(str(bar.close)),
                    Decimal(str(bar.volume)),
                    None,  # quote_volume not in Bar
                    None,  # trades not in Bar
                )
            )
            self._n_bars += 1
            if len(self._bar_buf) >= self._cfg.max_buffer and self._loop:
                self._loop.create_task(self._flush_bars())
            # Bars are published by NT DataEngine to msgbus automatically;
        # strategies receive them via their own msgbus subscriptions.
        # No manual re-publish needed (and it caused recursion).
        except Exception as e:
            self.log.error(f"on_bar error: {e}")

    def on_trade_tick(self, tick: TradeTick) -> None:
        try:
            symbol = tick.instrument_id.symbol.value
            self._tick_buf.append(
                (
                    symbol,
                    int(tick.trade_id.value) if str(tick.trade_id.value).isdigit() else hash(tick.trade_id.value) & 0x7FFFFFFFFFFFFFFF,
                    Decimal(str(tick.price)),
                    Decimal(str(tick.size)),
                    "BUY" if tick.aggressor_side.name == "BUYER" else "SELL",
                    _ns_to_dt(tick.ts_event),
                    _ns_to_dt(tick.ts_init),
                )
            )
            self._n_ticks += 1
            if len(self._tick_buf) >= self._cfg.max_buffer and self._loop:
                self._loop.create_task(self._flush_ticks())
        except Exception as e:
            self.log.error(f"on_trade_tick error: {e}")

    def on_funding_rate(self, fr) -> None:  # FundingRateUpdate
        # NT BinanceDataClient emits these at the 8h settlement boundary.
        # Persist as kind='settled' (the authoritative source for PnL accrual).
        try:
            symbol = fr.instrument_id.symbol.value
            self._funding_buf.append(
                (
                    symbol,
                    _ns_to_dt(fr.ts_event),
                    Decimal(str(fr.rate)),
                    None,  # mark_price
                    None,  # index_price
                    None,  # next_funding_time (not always available)
                    "settled",
                )
            )
            self._n_funding += 1
        except Exception as e:
            self.log.error(f"on_funding_rate error: {e}")

    def on_event(self, event) -> None:
        # Backstop for environments where msgbus wildcard doesn't fire
        try:
            if isinstance(event, OrderEvent):
                self._enqueue_order_event(event)
            elif isinstance(event, PositionEvent):
                self._enqueue_position_event(event)
        except Exception as e:
            self.log.error(f"on_event error: {e}")

    def _on_order_event(self, event) -> None:
        try:
            self._enqueue_order_event(event)
        except Exception as e:
            self.log.error(f"_on_order_event error: {e}")

    def _on_position_event(self, event) -> None:
        try:
            self._enqueue_position_event(event)
        except Exception as e:
            self.log.error(f"_on_position_event error: {e}")

    # ── event → DB ───────────────────────────────────────────────

    def _enqueue_order_event(self, event: OrderEvent) -> None:
        if not self._pool or not self._loop:
            return
        self._n_order_events += 1
        self._loop.create_task(self._write_order_event(event))

    def _enqueue_position_event(self, event: PositionEvent) -> None:
        if not self._pool or not self._loop:
            return
        self._n_position_events += 1
        self._loop.create_task(self._write_position_event(event))

    async def _emit_event_row(self, level: str, kind: str, payload: dict) -> None:
        """Insert a row into events table for Telegram event_watcher to pick up."""
        if not self._pool:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO events (level,kind,component,payload)
                       VALUES ($1,$2,'nautilus',$3::jsonb)""",
                    level, kind, _safe_json(payload),
                )
        except Exception as e:
            self.log.error(f"emit_event {kind} failed: {e}")

    async def _write_order_event(self, event: OrderEvent) -> None:
        try:
            order = self.cache.order(event.client_order_id) if hasattr(self, "cache") else None
            if order is None:
                # fallback: skip if cache unavailable
                return
            symbol = order.instrument_id.symbol.value
            order_id = str(order.client_order_id)
            status = order.status.name
            side = order.side.name
            otype = order.order_type.name
            qty = Decimal(str(order.quantity))
            raw_price = getattr(order, "price", None)  # MarketOrder has no price
            price = Decimal(str(raw_price)) if raw_price is not None else None
            ts_sub = _ns_to_dt(order.ts_init)
            ts_upd = _ns_to_dt(event.ts_event)

            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO orders (order_id, client_id, symbol, side, type,
                                         quantity, price, status, ts_submitted, ts_updated, raw)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
                    ON CONFLICT (order_id) DO UPDATE SET
                       status=EXCLUDED.status, ts_updated=EXCLUDED.ts_updated,
                       price=COALESCE(EXCLUDED.price, orders.price), raw=EXCLUDED.raw
                    """,
                    order_id, str(event.strategy_id) if event.strategy_id else None,
                    symbol, side, otype, qty, price, status, ts_sub, ts_upd,
                    _safe_json({"event": type(event).__name__, "ts": ts_upd.isoformat()}),
                )

            if isinstance(event, OrderFilled):
                fill_id = str(event.trade_id) if event.trade_id else f"{order_id}-{event.ts_event}"
                fill_price = Decimal(str(event.last_px))
                fill_qty = Decimal(str(event.last_qty))
                fee = Decimal(str(event.commission.as_decimal())) if event.commission else Decimal(0)
                fee_ccy = event.commission.currency.code if event.commission else None
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO fills (fill_id, order_id, symbol, side, price,
                                            quantity, fee, fee_currency, ts_event, raw)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
                        ON CONFLICT (fill_id) DO NOTHING
                        """,
                        fill_id, order_id, symbol, side,
                        fill_price, fill_qty, fee, fee_ccy,
                        _ns_to_dt(event.ts_event),
                        _safe_json({"liquidity": event.liquidity_side.name if event.liquidity_side else None}),
                    )
                # Emit a Telegram-pushable event row
                await self._emit_event_row("INFO", "trade_fill", {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(fill_qty),
                    "price": str(fill_price),
                    "notional": str(fill_qty * fill_price),
                    "fee": str(fee),
                    "fee_ccy": fee_ccy,
                    "strategy_id": str(event.strategy_id) if event.strategy_id else None,
                    "liquidity": event.liquidity_side.name if event.liquidity_side else None,
                })
        except Exception as e:
            self.log.error(f"write_order_event failed: {e}")

    async def _write_position_event(self, event: PositionEvent) -> None:
        try:
            pos = self.cache.position(event.position_id) if hasattr(self, "cache") else None
            if pos is None:
                return
            symbol = pos.instrument_id.symbol.value
            side = pos.side.name  # LONG/SHORT/FLAT
            qty = Decimal(str(pos.quantity))
            avg_px = Decimal(str(pos.avg_px_open)) if pos.avg_px_open else None
            realized = Decimal(str(pos.realized_pnl.as_decimal())) if pos.realized_pnl else Decimal(0)
            unrealized = Decimal(0)  # snapshot via portfolio if needed; skip on event
            opened_at = _ns_to_dt(pos.ts_opened)
            closed_at = _ns_to_dt(pos.ts_closed) if pos.ts_closed else None
            nt_pos_id = str(event.position_id)
            # Pull strategy_id from the event so per-strategy PnL queries work.
            # Pre-2026-05-23 every row had strategy_id=NULL.
            strat_id_str = str(event.strategy_id) if event.strategy_id else None
            strat_db_id: int | None = None
            if strat_id_str:
                # Strategy.id format: "<Name>-<order_id_tag>" where the tag
                # is the zero-padded DB id (set in strategy_loader). Parse:
                tail = strat_id_str.rsplit("-", 1)[-1]
                if tail.isdigit():
                    strat_db_id = int(tail)

            # NETTING quirk: the same NT position_id is reopened after
            # every FLAT. PositionOpened fires for each new life-cycle,
            # so we must distinguish by (nt_position_id, opened_at).
            #
            # Strategy:
            #   PositionOpened    → INSERT with ON CONFLICT (nt_pos_id,
            #                       opened_at) DO UPDATE (idempotent if
            #                       NT replays the event after restart).
            #   PositionChanged   → UPDATE WHERE nt_position_id=... AND
            #                       opened_at=... (one row, not "any open
            #                       row of this symbol", which previously
            #                       smeared updates across reopens).
            #   PositionClosed    → UPDATE same row + set closed_at.
            async with self._pool.acquire() as conn:
                if isinstance(event, PositionOpened):
                    await conn.execute(
                        """
                        INSERT INTO positions
                            (nt_position_id, symbol, strategy_id, side,
                             quantity, avg_price, realized_pnl, opened_at, raw)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                        ON CONFLICT (nt_position_id, opened_at)
                        DO UPDATE SET
                            side=EXCLUDED.side,
                            quantity=EXCLUDED.quantity,
                            avg_price=EXCLUDED.avg_price,
                            realized_pnl=EXCLUDED.realized_pnl
                        """,
                        nt_pos_id, symbol, strat_db_id, side,
                        qty, avg_px, realized, opened_at,
                        _safe_json({"position_id": nt_pos_id,
                                    "event": "PositionOpened"}),
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE positions
                           SET side=$3, quantity=$4,
                               avg_price=COALESCE($5, avg_price),
                               realized_pnl=$6, unrealized_pnl=$7,
                               closed_at=$8
                         WHERE nt_position_id=$1 AND opened_at=$2
                        """,
                        nt_pos_id, opened_at, side, qty, avg_px,
                        realized, unrealized, closed_at,
                    )

            # Emit Telegram event: position_open or position_close
            if isinstance(event, PositionOpened):
                await self._emit_event_row("INFO", "position_open", {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(qty),
                    "avg_price": str(avg_px) if avg_px else None,
                    "strategy_id": str(event.strategy_id) if event.strategy_id else None,
                })
            elif isinstance(event, PositionClosed):
                duration = (
                    (pos.ts_closed - pos.ts_opened) / 1_000_000_000
                    if pos.ts_closed and pos.ts_opened else 0
                )
                await self._emit_event_row("INFO", "position_close", {
                    "symbol": symbol,
                    "side_was": pos.entry.name if hasattr(pos, "entry") and pos.entry else None,
                    "qty_peak": str(pos.peak_qty.as_decimal()) if hasattr(pos, "peak_qty") else None,
                    "avg_open": str(pos.avg_px_open) if pos.avg_px_open else None,
                    "avg_close": str(pos.avg_px_close) if hasattr(pos, "avg_px_close") and pos.avg_px_close else None,
                    "realized_pnl": str(realized),
                    "duration_sec": round(duration, 1),
                    "strategy_id": str(event.strategy_id) if event.strategy_id else None,
                })
        except Exception as e:
            self.log.error(f"write_position_event failed: {e}")

    # ── flush ────────────────────────────────────────────────────


    # ── Order Book Deltas ───────────────────────────────────────────────

    def on_order_book_deltas(self, deltas) -> None:
        """NT callback for OrderBookDeltas.
        deltas.deltas: list[OrderBookDelta]
        delta.order: BookOrder(price, size, side)
        delta.is_delete: bool
        """
        try:
            sid = str(deltas.instrument_id)
            book = self._books.get(sid)
            if book is None:
                return
            for delta in deltas.deltas:
                try:
                    order = delta.order
                    p, s = float(order.price), float(order.size)
                    sk = "bids" if str(order.side) == "BUY" else "asks"
                    if delta.is_delete or s == 0.0:
                        book[sk].pop(p, None)
                    else:
                        book[sk][p] = s
                except Exception:
                    pass
        except Exception as e:
            self.log.error(f"on_order_book_deltas error: {e}")

    async def _l2_snapshot_loop(self) -> None:
        self.log.info(f"L2 snapshot loop starting (every {self._cfg.l2_snapshot_interval_sec}s)")
        try:
            from datetime import datetime, timezone
            while self._running:
                try:
                    await asyncio.sleep(self._cfg.l2_snapshot_interval_sec)
                    if not self._running:
                        break
                    now = datetime.now(timezone.utc)
                    for sid, book in self._books.items():
                        bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:10]
                        asks = sorted(book["asks"].items(), key=lambda x: x[0])[:10]
                        for i, (price, size) in enumerate(bids, 1):
                            self._l2_buf.append((now, sid, "bid", i, price, size))
                        for i, (price, size) in enumerate(asks, 1):
                            self._l2_buf.append((now, sid, "ask", i, price, size))
                        self._n_l2 += 20
                    if len(self._l2_buf) >= self._cfg.max_buffer:
                        await self._flush_l2()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.log.error(f"L2 snapshot error: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            self.log.info("L2 snapshot loop stopped.")

    async def _oi_poll_loop(self) -> None:
        self.log.info(f"OI polling starting (every {self._cfg.oi_poll_interval_sec}s)")
        try:
            import urllib.request, json
            while self._running:
                try:
                    await asyncio.sleep(self._cfg.oi_poll_interval_sec)
                    if not self._running:
                        break
                    url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=SOLUSDT"
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        data = json.loads(resp.read())
                    if "openInterest" in data and "time" in data:
                        ts = datetime.fromtimestamp(data["time"] / 1000, tz=timezone.utc)
                        self._oi_buf.append((ts, "SOLUSDT-PERP", float(data["openInterest"])))
                        self._n_oi += 1
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.log.error(f"OI poll error: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            self.log.info("OI polling stopped.")

    async def _flush_l2(self) -> None:
        if not self._pool or not self._l2_buf:
            return
        buf = self._l2_buf
        self._l2_buf = []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO l2_snapshots (ts, symbol, side, level, price, size) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
                    buf)
        except Exception as e:
            self.log.error(f"L2 flush failed ({len(buf)} rows): {e}")

    async def _flush_oi(self) -> None:
        if not self._pool or not self._oi_buf:
            return
        buf = self._oi_buf
        self._oi_buf = []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO open_interest (ts, symbol, value) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    buf)
        except Exception as e:
            self.log.error(f"OI flush failed ({len(buf)} rows): {e}")

    async def _flush_all(self) -> None:
        await self._flush_bars()
        await self._flush_ticks()
        await self._flush_funding()
        if self._cfg.collect_l2:
            await self._flush_l2()
        if self._cfg.collect_oi:
            await self._flush_oi()

    async def _flush_bars(self) -> None:
        if not self._pool or not self._bar_buf:
            return
        batch, self._bar_buf = self._bar_buf, []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO bars (symbol,timeframe,ts,open,high,low,close,volume,quote_volume,trades)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                       ON CONFLICT (symbol,timeframe,ts) DO NOTHING""",
                    batch,
                )
        except Exception as e:
            self.log.error(f"flush_bars failed ({len(batch)} rows): {e}")

    async def _flush_ticks(self) -> None:
        if not self._pool or not self._tick_buf:
            return
        batch, self._tick_buf = self._tick_buf, []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO ticks (symbol,trade_id,price,size,aggressor,ts_event,ts_init)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)
                       ON CONFLICT (symbol,trade_id,ts_event) DO NOTHING""",
                    batch,
                )
        except Exception as e:
            self.log.error(f"flush_ticks failed ({len(batch)} rows): {e}")

    async def _flush_funding(self) -> None:
        if not self._pool or not self._funding_buf:
            return
        batch, self._funding_buf = self._funding_buf, []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO funding
                         (symbol,ts,rate,mark_price,index_price,
                          next_funding_time,kind)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)
                       ON CONFLICT (symbol,ts,kind) DO NOTHING""",
                    batch,
                )
        except Exception as e:
            self.log.error(f"flush_funding failed ({len(batch)} rows): {e}")