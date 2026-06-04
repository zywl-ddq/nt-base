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
