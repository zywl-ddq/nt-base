# -*- coding: utf-8 -*-
"""
tests/test_slot.py -- StrategySlot 单元测试
=============================================

测试目标
--------
base/slot.py 中的 StrategySlot 类：
- 初始状态检查
- 开仓/平仓状态转换
- 持仓时间计算

测试覆盖场景
-----------
test_initial_state：
  - 新建 slot 时 has_position=False, tripped=False

test_open_close_position：
  - open_position("LONG", 72.5) 后：
    has_position=True, entry_side="LONG", entry_price=72.5
  - reset_position() 后：has_position=False

test_held_sec：
  - 开仓后 held_sec >= 0（刚开仓时经过时间极短但可能不为 0）

依赖
----
- FakeStrategy（模拟策略接口）

注意事项
--------
held_sec 基于 Python time.time() 计算，在快速执行中可能为 0
因此断言使用 >= 0 而非 == 0

作者: nt-base system
版本: 1.0.0
"""
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
