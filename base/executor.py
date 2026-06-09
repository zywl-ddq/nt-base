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
                return "ignored: same direction"
            self.flat(slot, signal.reason)
            if signal.direction != 0:
                from nautilus_trader.model.enums import OrderSide
                self._open(OrderSide.BUY if target_long else OrderSide.SELL,
                           current_price, slot, signal.reason)
            return "reversed" if signal.direction != 0 else "flatted"
        else:
            from nautilus_trader.model.enums import OrderSide
            self._open(OrderSide.BUY if target_long else OrderSide.SELL,
                       current_price, slot, signal.reason)
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

    def _open(self, side, price, slot, reason):
        from nautilus_trader.model.enums import TimeInForce
        from decimal import Decimal
        instr = self._cache.instrument(self._sol_id)
        account = self._portfolio.account(self._venue)
        equity = float(account.balance_total().as_decimal())
        adj_size_pct = self._adjusted_size(slot)
        notional = equity * adj_size_pct * slot.leverage
        qty = instr.make_qty(Decimal(str(notional / float(price))))
        order = self._create_market_order(
            instrument_id=self._sol_id, order_side=side,
            quantity=qty, time_in_force=TimeInForce.IOC,
        )
        self._submit_order(order)
        slot.open_position("LONG" if side.name == "BUY" else "SHORT", price)
        slot.last_trade_time = time.time()

        side_str = "LONG" if side.name == "BUY" else "SHORT"
        _notify(slot, fmt_entry(
            slot.strategy_id, str(self._sol_id), side_str,
            price, float(qty), notional, reason,
        ))

    def flat(self, slot, reason=""):
        pos = self._get_position()
        if pos is None:
            return False
        from nautilus_trader.model.enums import OrderSide, TimeInForce
        side = OrderSide.SELL if pos.side.name == "LONG" else OrderSide.BUY
        instr = self._cache.instrument(self._sol_id)
        try:
            exit_px = float(instr.last_price)
        except Exception:
            exit_px = 0.0
        order = self._create_market_order(
            instrument_id=self._sol_id, order_side=side,
            quantity=pos.quantity, time_in_force=TimeInForce.IOC,
        )
        self._submit_order(order)

        entry_px = slot.entry_price if slot.has_position else float(pos.avg_px_open)
        side_was = slot.entry_side if slot.has_position else ("LONG" if pos.side.name == "LONG" else "SHORT")
        if exit_px > 0 and entry_px > 0:
            if side_was == "LONG":
                pnl = float(pos.quantity.as_decimal()) * (exit_px - entry_px)
            else:
                pnl = float(pos.quantity.as_decimal()) * (entry_px - exit_px)
        else:
            pnl = 0.0

        _notify(slot, fmt_close(
            slot.strategy_id, str(self._sol_id), side_was,
            entry_px, exit_px, pnl, slot.held_sec, reason,
        ))

        slot.reset_position()
        logger.info(f"FLAT {slot.strategy_id} reason={reason}")
        return True

    def flat_all(self, slots, reason="shutdown"):
        for s in slots:
            if s.has_position:
                self.flat(s, reason)
