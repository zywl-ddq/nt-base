"""
Module:    base/executor
Purpose:   Order execution engine. Translates StrategySignals into NautilusTrader
           market orders with position sizing, risk gating, and Telegram notifications.

Class: OrderExecutor
  __init__(sol_id, venue, portfolio, submit_order, cache)
      sol_id: InstrumentId       鈥?trading instrument (SOLUSDT-PERP)
      venue: Venue               鈥?exchange venue (BINANCE)
      portfolio: Portfolio       鈥?NT portfolio for equity queries
      submit_order: callable     鈥?Strategy.submit_order bound method
      cache: Cache               鈥?NT cache for instrument/position queries

  execute(slot, signal, price) -> str
      Main entry: checks risk gates (tripped, cooldown), evaluates position
      state, routes to _open or flat. Returns result status string.

  _open(side, price, slot, reason)
      Computes position size from equity * position_size_pct * leverage,
      creates IOC market order, submits, updates slot state, sends Telegram.

  flat(slot, reason)
      Closes current position with IOC market order. Computes realized PnL
      approximation, sends Telegram close notification with PnL and hold time.

  flat_all(slots, reason)
      Emergency close-all. Called on strategy stop or daily loss trip.

Security Notes:
  - Order quantity derived from account equity (not fixed), preventing over-leverage.
  - IOC (Immediate-Or-Cancel) prevents partial fills from dangling.
  - Telegram notifications are fire-and-forget (asyncio.ensure_future).

Telegram Integration:
  Each slot has independent telegram_bot_token and telegram_chat_id.
  Notifications on: entry, exit (with PnL), reverse.

Author:    nt-base system
Version:   1.1.0
"""
from __future__ import annotations
'''OrderExecutor with Telegram notifications.'''
import asyncio
import time
import logging
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal
from base.notify import (
    send_message, fmt_entry, fmt_close, fmt_reverse,
)

logger = logging.getLogger(__name__)


def _notify(slot: StrategySlot, text: str):
    if slot.telegram_bot_token and slot.telegram_chat_id:
        asyncio.ensure_future(
            send_message(slot.telegram_bot_token, slot.telegram_chat_id, text)
        )


class OrderExecutor:
    def __init__(self, sol_id, venue, portfolio, submit_order, cache):
        self._sol_id = sol_id
        self._venue = venue
        self._portfolio = portfolio
        self._submit_order = submit_order
        self._cache = cache

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
                self._open(OrderSide.BUY if target_long else OrderSide.SELL, current_price, slot, signal.reason)
            return "reversed" if signal.direction != 0 else "flatted"
        else:
            from nautilus_trader.model.enums import OrderSide
            self._open(OrderSide.BUY if target_long else OrderSide.SELL, current_price, slot, signal.reason)
            return f"entry {slot.entry_side}"

    def _create_market_order(self, instrument_id, order_side, quantity, time_in_force):
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

    def _open(self, side, price, slot, reason):
        from nautilus_trader.model.enums import TimeInForce
        from decimal import Decimal
        instr = self._cache.instrument(self._sol_id)
        account = self._portfolio.account(self._venue)
        equity = float(account.balance_total().as_decimal())
        notional = equity * slot.position_size_pct * slot.leverage
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
                pnl = (exit_px - entry_px) / entry_px * slot.position_size_pct * slot.leverage * 1000
            else:
                pnl = (entry_px - exit_px) / entry_px * slot.position_size_pct * slot.leverage * 1000
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
