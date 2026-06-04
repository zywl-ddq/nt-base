"""OrderExecutor — signals to NT orders with risk gating."""
from __future__ import annotations
import time, logging
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal

logger = logging.getLogger(__name__)


class OrderExecutor:
    def __init__(self, sol_id, venue, portfolio, submit_order, cache):
        self._sol_id = sol_id
        self._venue = venue
        self._portfolio = portfolio
        self._submit_order = submit_order  # callable: Strategy.submit_order
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
        """Create a market order. Uses Strategy's order_factory if available."""
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

    def flat(self, slot, reason=""):
        pos = self._get_position()
        if pos is None:
            return False
        from nautilus_trader.model.enums import OrderSide, TimeInForce
        side = OrderSide.SELL if pos.side.name == "LONG" else OrderSide.BUY
        order = self._create_market_order(
            instrument_id=self._sol_id, order_side=side,
            quantity=pos.quantity, time_in_force=TimeInForce.IOC,
        )
        self._submit_order(order)
        slot.reset_position()
        logger.info(f"FLAT {slot.strategy_id} reason={reason}")
        return True

    def flat_all(self, slots, reason="shutdown"):
        for s in slots:
            if s.has_position:
                self.flat(s, reason)
