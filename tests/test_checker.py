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


class TestHoldTime:
    def test_hold_time_not_exceeded(self):
        s = make_slot(max_hold_sec=3600)
        s.open_position("LONG", 100.0)
        # s.held_sec should be near 0
        assert not check_hold(s).should_exit
        assert not check_hold(s, 100.0).should_exit

    def test_hold_time_exceeded(self):
        s = make_slot(max_hold_sec=3600)
        s.open_position("LONG", 100.0)
        s.entry_time = s.entry_time - 3700  # simulate holding for 3700s
        assert check_hold(s).should_exit
        assert check_hold(s, 100.0).should_exit


class TestDailyLoss:
    def test_daily_no_loss(self):
        s = make_slot(max_daily_loss_pct=0.05, daily_start_equity=1000.0)
        s.daily_pnl = 0.0
        assert not check_daily(s).should_exit

    def test_daily_loss_tripped(self):
        s = make_slot(max_daily_loss_pct=0.05, daily_start_equity=1000.0)
        s.daily_pnl = -60.0  # -6%
        assert check_daily(s).should_exit


class TestCheckAll:
    def test_check_all_no_triggers(self):
        s = make_slot(stop_pct=0.03, take_pct=0.06, max_hold_sec=3600)
        s.open_position("LONG", 100.0)
        assert len(check_all(s, 100.0)) == 0

    def test_check_all_trigger(self):
        s = make_slot(stop_pct=0.03, take_pct=0.06, max_hold_sec=3600)
        s.open_position("LONG", 100.0)
        actions = check_all(s, 95.0)  # -5% (trips stop loss)
        assert len(actions) == 1
        assert actions[0].kind == "stop_loss"
