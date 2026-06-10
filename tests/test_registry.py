# -*- coding: utf-8 -*-
"""
tests/test_registry.py -- StrategyRegistry 单元测试
=====================================================

测试目标
--------
base/registry.py 中的 StrategyRegistry 类：
- 策略注册和查询（register / get_slots / count）
- 因子索引维护（active_factors）
- 注销后索引清理（unregister）
- 重复注册拒绝
- 注销不存在的策略不报错

测试覆盖场景
-----------
test_register_query：
  - 注册一个策略，验证 count=1
  - 查询 "SOLUSDT-PERP/1m" 的 slot，返回 1 个

test_factor_index：
  - 注册两个策略，分别依赖不同因子组合
  - 验证 active_factors 返回因子的并集

test_unregister_cleans_index：
  - 注册两个策略（依赖相同因子）
  - 注销其中一个，验证因子索引仍保留
  - 注销另一个，验证因子索引为空

test_duplicate_rejected：
  - 同一个 strategy_id 注册两次应抛出 ValueError

test_unregister_nonexistent：
  - 注销不存在的 strategy_id 不应抛出异常

依赖
----
- FakeStrategy（模拟策略接口）
- BarSubscription（模拟订阅信息）

作者: nt-base system
版本: 1.0.0
"""
"""Tests for StrategyRegistry."""
import pytest
from base.registry import StrategyRegistry
from base.slot import StrategySlot
from base.signal_protocol import BarSubscription

class FakeStrategy:
    def __init__(self, sid): self.strategy_id = sid
    def on_bar(self, d): return None
    def on_shutdown(self): pass
    def get_diagnostics(self): return {}

def make_slot(sid, factors=None):
    subs = [BarSubscription(symbol="SOLUSDT-PERP", timeframe="1m", factors=factors or [])]
    return StrategySlot(strategy_id=sid, strategy=FakeStrategy(sid), subscriptions=subs)

def test_register_query():
    reg = StrategyRegistry()
    reg.register(make_slot("alpha", ["trend_regime", "cvd_divergence"]))
    assert reg.count == 1
    assert len(reg.get_slots("SOLUSDT-PERP", "1m")) == 1

def test_factor_index():
    reg = StrategyRegistry()
    reg.register(make_slot("alpha", ["trend_regime"]))
    reg.register(make_slot("beta", ["trend_regime", "cvd_divergence"]))
    assert reg.active_factors() == {"trend_regime", "cvd_divergence"}

def test_unregister_cleans_index():
    reg = StrategyRegistry()
    reg.register(make_slot("alpha", ["trend_regime"]))
    reg.register(make_slot("beta", ["trend_regime"]))
    reg.unregister("alpha")
    assert reg.active_factors() == {"trend_regime"}
    reg.unregister("beta")
    assert len(reg.active_factors()) == 0

def test_duplicate_rejected():
    reg = StrategyRegistry()
    reg.register(make_slot("alpha"))
    with pytest.raises(ValueError):
        reg.register(make_slot("alpha"))

def test_unregister_nonexistent():
    reg = StrategyRegistry()
    reg.unregister("nonexistent")
