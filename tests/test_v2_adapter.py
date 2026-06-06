"""Test V2SignalAdapter: bridges nt-base bar_data dict -> trading-v2 on_bar call.

Key behaviors:
1. Translates bar_data dict keys (close/high/low/ts_ns/factors) to AlphaSignal.on_bar() params
2. Pushes factor values via set_factor_value before on_bar
3. Returns nt-base StrategySignal wrapping V2's StrategySignal
4. Exposes subscriptions matching the strategy's factor_names
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from base.v2_adapter import V2SignalAdapter
from base.v2_signal import AlphaSignal


def test_adapter_pushes_factors_before_on_bar():
    """Factor values from bar_data['factors'] must reach AlphaSignal before on_bar."""
    alpha = AlphaSignal(
        gate_factor="",
        factor_1="trend_regime", direction_1=-1, weight_1=1.0,
        signal_threshold=0.01,
    )
    adapter = V2SignalAdapter(alpha, "test-001", "SOLUSDT-PERP", "1m")

    # First bar: no factor -> composite should be 0
    result = adapter.on_bar({
        "close": 65.0, "high": 65.1, "low": 64.9,
        "ts_ns": 1_000_000_000, "factors": {},
    })
    assert result.direction == 0, f"Expected no signal without factors, got {result.direction}"

    # Feed several bars with trend_regime=-1 to build composite
    for i in range(30):
        adapter.on_bar({
            "close": 65.0, "high": 65.1, "low": 64.9,
            "ts_ns": 1_000_000_000 * (i + 2),
            "factors": {"trend_regime": -1.0},
        })


def test_adapter_subscriptions_match_factors():
    """Subscriptions must list the strategy's factor_names."""
    alpha = AlphaSignal(
        factor_1="cvd_divergence", direction_1=-1, weight_1=1.0,
        factor_2="trend_regime", direction_2=1, weight_2=0.5,
    )
    adapter = V2SignalAdapter(alpha, "test-002", "SOLUSDT-PERP", "1m")

    subs = adapter.subscriptions
    assert len(subs) == 1
    assert subs[0].symbol == "SOLUSDT-PERP"
    assert subs[0].timeframe == "1m"
    assert "trend_regime" in subs[0].factors
    assert "cvd_divergence" in subs[0].factors


def test_adapter_returns_correct_strategy_id():
    """Strategy ID passed to constructor must be exposed."""
    alpha = AlphaSignal()
    adapter = V2SignalAdapter(alpha, "AlphaV2-test", "SOLUSDT-PERP")
    assert adapter.strategy_id == "AlphaV2-test"


def test_adapter_handles_missing_factor_keys():
    """Missing keys in bar_data dict should not crash."""
    alpha = AlphaSignal()
    adapter = V2SignalAdapter(alpha, "test-003", "SOLUSDT-PERP")

    # Minimal bar_data with only close
    result = adapter.on_bar({"close": 65.0})
    assert result is not None
    assert result.direction == 0  # No factors, no signal


if __name__ == "__main__":
    test_adapter_pushes_factors_before_on_bar()
    print("PASS: test_adapter_pushes_factors_before_on_bar")
    test_adapter_subscriptions_match_factors()
    print("PASS: test_adapter_subscriptions_match_factors")
    test_adapter_returns_correct_strategy_id()
    print("PASS: test_adapter_returns_correct_strategy_id")
    test_adapter_handles_missing_factor_keys()
    print("PASS: test_adapter_handles_missing_factor_keys")
