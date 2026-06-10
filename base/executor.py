"""
OrderExecutor v2 -- with dynamic position sizing.

New in v2:
  - _open() adjusts position_size_pct by trend confidence:
    weak trend -> smaller size (defensive)
    strong trend -> full size (offensive)
  - Slot stores confidence for sizing decisions
"""
import asyncio
import time
import logging
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal
from base.notify import send_message, fmt_entry, fmt_close

logger = logging.getLogger(__name__)


def _notify(slot: StrategySlot, text: str):
    _lg = logging.getLogger(__name__)
    _lg.info(f"_notify: tok={bool(slot.telegram_bot_token)} chat={bool(slot.telegram_chat_id)} tok_len={len(slot.telegram_bot_token) if slot.telegram_bot_token else 0}")
    if slot.telegram_bot_token and slot.telegram_chat_id:
        _lg.info(f"_notify: SENDING to chat {slot.telegram_chat_id}")
        asyncio.ensure_future(
            send_message(slot.telegram_bot_token, slot.telegram_chat_id, text)
        )


class OrderExecutor:
    def __init__(self, sol_id, venue, portfolio, submit_order, cache, order_factory=None):
        self._sol_id = sol_id
        self._venue = venue
        self._portfolio = portfolio
        self._submit_order = submit_order
        self._cache = cache
        self._order_factory = order_factory
        self._pending: dict[str, dict] = {}  # client_order_id -> notification context

    def execute(self, slot: StrategySlot, signal: StrategySignal,
                current_price: float) -> str:
        if slot.tripped:
            return "rejected: tripped"
        if time.time() - slot.last_trade_time < slot.cooldown_sec:
            return "rejected: cooldown"

        target_long = signal.direction > 0
        pos = self._get_position()

        if pos is not None:
            currently_long = pos.side.name == "LONG"
            if currently_long == target_long:
                # Pyramid: validate total position before adding.
                # TickExitManager.add_position() preserves trailing state.
                from nautilus_trader.model.enums import OrderSide
                from decimal import Decimal
                account = self._portfolio.account(self._venue)
                equity = float(account.balance_total().as_decimal())
                current_notional = self._current_position_notional()
                max_notional = self._max_position_notional(slot)
                req_pct = signal.position_size_pct if signal.position_size_pct > 0 else slot.position_size_pct
                req_notional = equity * req_pct * slot.leverage
                new_total = current_notional + req_notional
                if new_total > max_notional:
                    available = max(0.0, max_notional - current_notional)
                    clipped_pct = available / (equity * slot.leverage) if equity > 0 else 0.0
                    min_notional = equity * slot.position_size_pct * slot.leverage * 0.1
                    if available < min_notional:
                        return "rejected: position limit reached"
                    logger.info(
                        f"Pyramid clipped: {req_pct:.3f} -> {clipped_pct:.3f} "
                        f"(current={current_notional:.2f} max={max_notional:.2f})"
                    )
                    size_pct = clipped_pct
                else:
                    size_pct = req_pct
                self._open(OrderSide.BUY if target_long else OrderSide.SELL,
                           current_price, slot, signal.reason, size_pct_override=size_pct)
                return f"pyramid {size_pct:.3f}"
            # Opposite direction: blocked.
            # Strategy must send a close signal first (direction=0),
            # wait for flat confirmation, then enter on a subsequent bar.
            return "rejected: reversal blocked (close first)"
        else:
            from nautilus_trader.model.enums import OrderSide
            size_pct = signal.position_size_pct if signal.position_size_pct > 0 else None
            self._open(OrderSide.BUY if target_long else OrderSide.SELL,
                       current_price, slot, signal.reason, size_pct_override=size_pct)
            return f"entry {slot.entry_side}"

    def _create_market_order(self, instrument_id, order_side, quantity, time_in_force):
        if self._order_factory is not None:
            return self._order_factory.market(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
            )
        return self._cache.instrument(instrument_id).create_order(
            order_side=order_side,
            quantity=quantity,
            time_in_force=time_in_force,
            post_only=False,
            reduce_only=False,
            quote_quantity=False,
        )

    def _get_position(self):
        for p in self._cache.positions_open(instrument_id=self._sol_id):
            if float(p.quantity.as_decimal()) > 0:
                return p
        return None

    def _adjusted_size(self, slot: StrategySlot) -> float:
        """Scale position size by trend confidence."""
        conf = getattr(slot, 'confidence', 0.0)
        floor = 0.25
        slope = 0.75
        scale = floor + slope * conf
        return slot.position_size_pct * scale


    # -- Position sizing validation --

    def _max_position_notional(self, slot) -> float:
        """Maximum total notional for this strategy, caps pyramid adds.
        Uses 2x the base position_size_pct as the hard cap."""
        from decimal import Decimal
        account = self._portfolio.account(self._venue)
        equity = float(account.balance_total().as_decimal())
        max_pct = slot.position_size_pct * 2.0
        return equity * max_pct * slot.leverage

    def _current_position_notional(self) -> float:
        """Current position notional at last price."""
        pos = self._get_position()
        if pos is None:
            return 0.0
        instr = self._cache.instrument(self._sol_id)
        price = float(instr.last_price)
        qty = float(pos.quantity.as_decimal())
        return qty * price

    def _open(self, side, price, slot, reason, size_pct_override=None):
        from nautilus_trader.model.enums import TimeInForce
        from decimal import Decimal
        instr = self._cache.instrument(self._sol_id)
        account = self._portfolio.account(self._venue)
        equity = float(account.balance_total().as_decimal())
        if size_pct_override is not None:
            conf = getattr(slot, "confidence", 0.0)
            floor = 0.25; slope = 0.75
            scale = floor + slope * conf
            adj_size_pct = size_pct_override * scale
        else:
            adj_size_pct = self._adjusted_size(slot)
        notional = equity * adj_size_pct * slot.leverage
        qty = instr.make_qty(Decimal(str(notional / float(price))))
        order = self._create_market_order(
            instrument_id=self._sol_id, order_side=side,
            quantity=qty, time_in_force=TimeInForce.IOC,
        )
        self._submit_order(order)
        slot.last_trade_time = time.time()

        side_str = "LONG" if side.name == "BUY" else "SHORT"
        # Defer notification AND slot state update to on_fill for actual fill price
        cid = str(order.client_order_id)
        self._pending[cid] = {
            "type": "entry",
            "slot": slot,
            "side": side_str,
            "reason": reason,
            "estimated_price": price,
            "notional": notional,
            "expected_qty": float(qty),
            "accum_qty": 0.0,
            "accum_notional": 0.0,
            "total_commission": 0.0,
            "created_at": time.time(),
        }

    def has_pending_close_for(self, slot) -> bool:
        """Check if there is already a pending close order for this slot."""
        for p in self._pending.values():
            if p.get("slot") is slot and p.get("type") == "close":
                return True
        return False

    def flat(self, slot, reason=""):
        pos = self._get_position()
        if pos is None:
            return False
        from nautilus_trader.model.enums import OrderSide, TimeInForce
        side = OrderSide.SELL if pos.side.name == "LONG" else OrderSide.BUY
        instr = self._cache.instrument(self._sol_id)
        # Guard: don"t submit duplicate close orders for the same slot
        if self.has_pending_close_for(slot):
            return False
        try:
            exit_px = float(instr.last_price)
        except Exception:
            exit_px = 0.0
        order = self._create_market_order(
            instrument_id=self._sol_id, order_side=side,
            quantity=pos.quantity, time_in_force=TimeInForce.IOC,
        )
        self._submit_order(order)

        # Read slot state BEFORE any changes — flat() is an action, not a result.
        # Slot state will be updated by on_fill() when the fill is confirmed.
        entry_px = slot.entry_price if slot.has_position else float(pos.avg_px_open)
        side_was = slot.entry_side if slot.has_position else ("LONG" if pos.side.name == "LONG" else "SHORT")

        logger.info(f"FLAT {slot.strategy_id} reason={reason}")

        # Defer notification AND slot state update to on_fill for actual exit price
        cid = str(order.client_order_id)
        self._pending[cid] = {
            "type": "close",
            "slot": slot,
            "side_was": side_was,
            "entry_px": entry_px,
            "reason": reason,
            "estimated_exit_px": exit_px,
            "expected_qty": float(pos.quantity.as_decimal()),
            "accum_qty": 0.0,
            "accum_notional": 0.0,
            "total_commission": 0.0,
            "created_at": time.time(),
        }
        return True

    def flat_all(self, slots, reason="shutdown"):
        for s in slots:
            if s.has_position:
                self.flat(s, reason)

    # ── Fill-based notification (actual exchange prices) ──

    def on_fill(self, client_order_id: str, last_px: float, last_qty: float,
                commission: float = 0.0):
        """Called from BaseStrategy.on_order_filled. Accumulates fills and sends
        notification with VWAP once the order is fully filled."""
        pending = self._pending.get(client_order_id)
        if pending is None:
            return  # not our order, or already processed

        pending["accum_qty"] += last_qty
        pending["accum_notional"] += last_qty * last_px
        pending["total_commission"] += commission

        # Check if fully filled (allow 1% tolerance for rounding)
        if pending["accum_qty"] < pending["expected_qty"] * 0.99:
            return  # wait for more fills

        vwap = pending["accum_notional"] / pending["accum_qty"] if pending["accum_qty"] > 0 else last_px
        slot = pending["slot"]

        if pending["type"] == "entry":
            # Update slot state on confirmed fill
            slot.open_position(pending["side"], vwap)
            _notify(slot, fmt_entry(
                slot.strategy_id, str(self._sol_id), pending["side"],
                vwap, pending["accum_qty"], pending["notional"], pending["reason"],
            ))

        elif pending["type"] == "close":
            entry_px = pending["entry_px"]
            qty = pending["accum_qty"]
            held_sec = slot.held_sec  # compute at fill time, slot hasn't been reset yet
            if pending["side_was"] == "LONG":
                pnl = qty * (vwap - entry_px) - pending["total_commission"]
            else:
                pnl = qty * (entry_px - vwap) - pending["total_commission"]
            # Update slot state on confirmed fill
            slot.reset_position()
            _notify(slot, fmt_close(
                slot.strategy_id, str(self._sol_id), pending["side_was"],
                entry_px, vwap, pnl, held_sec, pending["reason"],
            ))

        del self._pending[client_order_id]

    def flush_pending(self):
        """Send all pending notifications with estimated prices (shutdown fallback)."""
        now = time.time()
        stale = []
        for cid, p in list(self._pending.items()):
            if now - p["created_at"] > 10:  # 10s grace: if fill hasn't arrived by now, use estimate
                stale.append(cid)
                slot = p["slot"]
                if p["type"] == "entry":
                    px = p.get("estimated_price", 0)
                    _notify(slot, fmt_entry(
                        slot.strategy_id, str(self._sol_id), p["side"],
                        px, p["expected_qty"], p["notional"], p["reason"] + " (est)",
                    ))
                elif p["type"] == "close":
                    px = p.get("estimated_exit_px", 0)
                    entry_px = p["entry_px"]
                    qty = p["expected_qty"]
                    held_sec = slot.held_sec  # read from slot at flush time
                    if p["side_was"] == "LONG":
                        pnl = qty * (px - entry_px) - p["total_commission"] if px > 0 else -p["total_commission"]
                    else:
                        pnl = qty * (entry_px - px) - p["total_commission"] if px > 0 else -p["total_commission"]
                    _notify(slot, fmt_close(
                        slot.strategy_id, str(self._sol_id), p["side_was"],
                        entry_px, px, pnl, held_sec, p["reason"] + " (est)",
                    ))
        for cid in stale:
            del self._pending[cid]
        if stale:
            logger.info(f"flush_pending: sent {len(stale)} stale notifications")

    def accept_partial_fill(self, client_order_id: str):
        """Accept whatever quantity has filled so far.
        Called when an order is canceled/expired (IOC remainder canceled)."""
        pending = self._pending.get(client_order_id)
        if pending is None:
            return
        if pending["accum_qty"] <= 0:
            del self._pending[client_order_id]
            return

        vwap = pending["accum_notional"] / pending["accum_qty"]
        slot = pending["slot"]

        if pending["type"] == "entry":
            # Only update slot if no position is tracked yet (avoid double-open).
            if slot.has_position:
                del self._pending[client_order_id]
                return
            slot.open_position(pending["side"], vwap)
            _notify(slot, fmt_entry(
                slot.strategy_id, str(self._sol_id), pending["side"],
                vwap, pending["accum_qty"], pending["notional"],
                pending["reason"] + " (partial)",
            ))

        elif pending["type"] == "close":
            # Only send partial notification if position still open.
            # If already closed by a subsequent fill, skip silently.
            if not slot.has_position:
                del self._pending[client_order_id]
                return
            entry_px = pending["entry_px"]
            qty = pending["accum_qty"]
            held_sec = slot.held_sec  # compute now, slot not reset for partial close
            if pending["side_was"] == "LONG":
                pnl = qty * (vwap - entry_px) - pending["total_commission"]
            else:
                pnl = qty * (entry_px - vwap) - pending["total_commission"]
            # Do NOT reset slot — position still exists (just reduced).
            _notify(slot, fmt_close(
                slot.strategy_id, str(self._sol_id), pending["side_was"],
                entry_px, vwap, pnl, held_sec,
                pending["reason"] + " (partial)",
            ))

        del self._pending[client_order_id]

    def cleanup_pending(self, max_age_sec: float = 10.0):
        """Handle stale pending entries. For orders with partial fills, accept
        whatever filled. For orders with zero fills, log and discard."""
        now = time.time()
        stale = [cid for cid, p in self._pending.items() if now - p["created_at"] > max_age_sec]
        for cid in stale:
            p = self._pending.get(cid)
            if p and p.get("accum_qty", 0) > 0:
                logger.warning(
                    f"cleanup_pending: partial accept for {cid} ({p['type']}), "
                    f"filled {p['accum_qty']}/{p['expected_qty']}"
                )
                self.accept_partial_fill(cid)
            else:
                logger.warning(
                    f"cleanup_pending: fill never arrived for {cid} ({p['type']})"
                )
                if cid in self._pending:
                    del self._pending[cid]
        return len(stale)
