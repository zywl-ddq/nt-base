"""Tests for StrategySlot."""
from base.slot import StrategySlot

class FakeStrategy:
    strategy_id = "test"; subscriptions = []
    def on_bar(self, d): return None
    def on_shutdown(self): pass
    def get_diagnostics(self): return {}

def test_initial_state():
    s = StrategySlot(strategy_id="test", strategy=FakeStrategy())
    assert not s.has_position
    assert not s.tripped

def test_open_close_position():
    s = StrategySlot(strategy_id="test", strategy=FakeStrategy())
    s.open_position("LONG", 72.5)
    assert s.has_position and s.entry_side == "LONG" and s.entry_price == 72.5
    s.reset_position()
    assert not s.has_position

def test_held_sec():
    s = StrategySlot(strategy_id="test", strategy=FakeStrategy())
    s.open_position("SHORT", 70.0)
    assert s.held_sec >= 0
