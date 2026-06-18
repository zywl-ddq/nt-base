# -*- coding: utf-8 -*-
"""
tests/test_checker.py -- 风控检查函数单元测试
================================================

测试目标
--------
risk/checker.py 模块中的风控检查函数：
- check_stop：   止损检查（价格向不利方向移动超过阀值）
- check_take：   止盈检查（价格向有利方向移动超过阀值）
- check_hold：   持仓时间检查（超时强制退出）
- check_daily：  日亏损检查（当日亏损超过最大允许比例）
- check_all：    一次性执行全部检查

测试覆盖场景
-----------
TestStopLoss:
  - test_long_stop_hit：     多头止损触发（价格跌 3%）
  - test_long_stop_not_hit： 多头止损未触发（价格跌 2% < 3%）
  - test_short_stop_hit：    空头止损触发（价格涨 3%）
  - test_no_position：       无持仓时止损检查返回 False

TestTakeProfit:
  - test_long_take_hit：     多头止盈触发（价格涨 7% > 6%）
  - test_long_take_not_hit： 多头止盈未触发（价格涨 4% < 6%）

TestHoldTime:
  - test_hold_time_not_exceeded： 未超时，不触发
  - test_hold_time_exceeded：     超时（模拟 entry_time 提前 3700 秒）

TestDailyLoss:
  - test_daily_no_loss：          当日无亏损，不触发
  - test_daily_loss_tripped：     当日亏损 6% > 5%，触发

TestCheckAll:
  - test_check_all_no_triggers：  所有条件检查，无触发
  - test_check_all_trigger：      止损触发，验证返回的 kind="stop_loss"

依赖
----
- FakeStrategy（模拟策略接口）
- StrategySlot（创建测试用策略槽位）

作者: nt-base system
版本: 1.0.0
"""
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
        # -5% 时 check_trail 与 check_stop 同时触发——开仓瞬间 trail 线
        # (highest_since_entry - stop_pct*entry = 100-3 = 97) 与硬止损线重合，
        # 这是 check_all “报告全部触发项”语义下的正确行为（RiskLoop 实际短路）。
        actions = check_all(s, 95.0)
        kinds = [a.kind for a in actions]
        assert "stop_loss" in kinds                 # 硬止损必然触发
        assert all(a.should_exit for a in actions)  # 返回的都是退出动作
