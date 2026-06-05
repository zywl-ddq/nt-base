"""Regression test: AlphaSignal.on_bar() must not raise UnboundLocalError on 'regime'.

BUG: regime was computed inside `if self._in_position:` block but also referenced
in the entry path. When trend triggered an entry from flat state, `regime` was
unbound -> UnboundLocalError -> process crash.

Run: cd /root/nt-base && python3 tests/test_regime_regression.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from base.v2_signal import AlphaSignal


def test_entry_from_flat_does_not_crash():
    """Entry path from flat state must not raise UnboundLocalError on 'regime'.

    The entry return uses regime in an f-string. Before the fix, regime was
    only defined in the 'in position' branch, causing a crash on first entry.
    """
    alpha = AlphaSignal(
        gate_factor="",
        factor_1="trend_regime", direction_1=-1, weight_1=1.0,
        signal_threshold=0.01,
    )

    seen_entry = False
    for i in range(30):
        ts = 1_000_000_000 * (i + 1)
        alpha.set_factor_value("trend_regime", ts, -1.0)
        signal = alpha.on_bar(
            close=65.0, high=65.1, low=64.9,
            delta_buy_vol=0.0, delta_sell_vol=0.0,
            btc_close=65000.0, ts_ns=ts,
        )
        if signal and signal.direction != 0:
            seen_entry = True
            assert "composite=" in signal.reason, (
                f"Entry reason should include composite value, got: {signal.reason}"
            )

    assert seen_entry, (
        "Expected at least one entry signal (dir != 0) within 30 bars"
    )


def test_no_unbound_local_error_in_entry_path():
    """Regression: the exact crash path from production logs.

    Log showed: 'reason=f\"composite={...} regime={regime}\"' at v2_signal.py:157
    with UnboundLocalError because `regime` was only defined in the in-position block.
    """
    alpha = AlphaSignal(
        gate_factor="",
        factor_1="trend_regime", direction_1=-1, weight_1=1.0,
        signal_threshold=0.01,
    )

    for i in range(80):
        ts = 1_000_000_000 * (i + 1)
        close = 65.0 - i * 0.02
        alpha.set_factor_value("trend_regime", ts, -1.0)
        # This MUST NOT raise UnboundLocalError
        alpha.on_bar(
            close=close, high=close + 0.05, low=close - 0.05,
            delta_buy_vol=0.0, delta_sell_vol=0.0,
            btc_close=65000.0, ts_ns=ts,
        )

    # If we got here without exception, the fix works
    diag = alpha.get_diagnostics()
    assert diag["in_position"], "Should be in a position"
    assert diag["direction"] == -1, f"Expected SHORT bias, got {diag}"


if __name__ == "__main__":
    test_entry_from_flat_does_not_crash()
    print("PASS: test_entry_from_flat_does_not_crash")
    test_no_unbound_local_error_in_entry_path()
    print("PASS: test_no_unbound_local_error_in_entry_path")
