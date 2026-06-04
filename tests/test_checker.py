"""Tests for risk checker."""
from risk.checker import check_stop, check_take, check_hold, check_daily, check_all
from base.slot import StrategySlot

class FakeStrategy:
    strategy_id = "test"; subscriptions = []
    def on_bar(self, d): return None
    def on_shutdown(self): pass
    def get_diagnostics(self): return {}

def make_slot(**kw):
    s = StrategySlot(strategy_id="test", strategy=FakeStrategy(), **kw)
    return s

class TestStopLoss:
    def test_long_stop_hit(self):
        s = make_slot(stop_pct=0.03); s.open_position("LONG", 100.0)
        assert check_stop(s, 96.0).should_exit
    def test_long_stop_not_hit(self):
        s = make_slot(stop_pct=0.03); s.open_position("LONG", 100.0)
        assert not check_stop(s, 98.0).should_exit
    def test_short_stop_hit(self):
        s = make_slot(stop_pct=0.03); s.open_position("SHORT", 100.0)
        assert check_stop(s, 104.0).should_exit
    def test_no_position(self):
        s = make_slot()
        assert not check_stop(s, 90.0).should_exit

class TestTakeProfit:
    def test_long_take_hit(self):
        s = make_slot(take_pct=0.06); s.open_position("LONG", 100.0)
        assert check_take(s, 107.0).should_exit
    def test_long_take_not_hit(self):
        s = make_slot(take_pct=0.06); s.open_position("LONG", 100.0)
        assert not check_take(s, 104.0).should_exit
