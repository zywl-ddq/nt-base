"""Unit tests for OrderExecutor."""
import pytest
from base.executor import OrderExecutor
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal


class MockInstrument:
    def __init__(self, last_price=100.0):
        self.last_price = last_price

    def create_order(self, **kwargs):
        return f"OrderSide={kwargs.get('order_side')}, Qty={kwargs.get('quantity')}"

    def make_qty(self, qty):
        return qty


class MockPositionSide:
    def __init__(self, name="LONG"):
        self.name = name


class MockQuantity:
    def __init__(self, val=1.0):
        self._val = val

    def as_decimal(self):
        return self._val


class MockPosition:
    def __init__(self, side_name="LONG", qty=1.0):
        self.side = MockPositionSide(side_name)
        self.quantity = MockQuantity(qty)
        self.avg_px_open = 100.0


class MockCache:
    def __init__(self, instrument):
        self._instrument = instrument
        self.positions = []

    def instrument(self, instrument_id):
        return self._instrument

    def positions_open(self, instrument_id):
        return self.positions


class MockBalance:
    def as_decimal(self):
        return 1000.0


class MockAccount:
    def balance_total(self):
        return MockBalance()


class MockPortfolio:
    def account(self, venue):
        return MockAccount()


class FakeStrategy:
    strategy_id = "test"
    subscriptions = []

    def on_bar(self, d):
        return None

    def on_shutdown(self):
        pass

    def get_diagnostics(self):
        return {}


def test_order_executor_entry_and_flat():
    submitted_orders = []

    def submit_order(order):
        submitted_orders.append(order)

    instrument = MockInstrument(last_price=100.0)
    cache = MockCache(instrument)
    portfolio = MockPortfolio()

    executor = OrderExecutor(
        sol_id="SOLUSDT-PERP",
        venue="BINANCE",
        portfolio=portfolio,
        submit_order=submit_order,
        cache=cache,
        order_factory=None, # will fall back to cache instrument create_order under mock
    )

    slot = StrategySlot(
        strategy_id="test-strategy",
        strategy=FakeStrategy(),
        position_size_pct=0.20,
        leverage=2,
        cooldown_sec=0.0,
    )

    # 1. Test entry LONG signal
    sig = StrategySignal(direction=1, reason="Test entry long")
    res = executor.execute(slot, sig, current_price=100.0)

    assert "entry" in res
    assert slot.has_position
    assert slot.entry_side == "LONG"
    assert slot.entry_price == 100.0
    assert len(submitted_orders) == 1
    assert "OrderSide" in submitted_orders[0]

    # Mock position in cache
    cache.positions = [MockPosition("LONG", 4.0)]

    # 2. Test execute with same direction LONG signal (should be ignored)
    res_ignored = executor.execute(slot, sig, current_price=101.0)
    assert res_ignored == "ignored: same direction"

    # 3. Test flattening
    flattened = executor.flat(slot, reason="Stop triggered")
    assert flattened
    assert not slot.has_position
    cache.positions = []
    assert len(submitted_orders) == 2
    assert "OrderSide" in submitted_orders[1]
