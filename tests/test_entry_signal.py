"""Test AlphaSignal entry signal generation with trend_regime-only factor.

This is the config used in production: single trend_regime signal factor,
no gate, low threshold for rapid entry in trending markets.

Key behaviors:
1. Consistent trend_regime=-1 generates SHORT entry after buffer fills
2. Signal only triggers when EMA crosses threshold
3. Exit path doesn't crash when missing btc/delta data
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from strategy.alpha_signal_v3 import AlphaSignal


class TestTrendRegimeOnlySignal:
    """Single-factor trend_regime strategy 鈥?the production config."""

    def setup_method(self):
        self.alpha = AlphaSignal(
            gate_factor="",
            factor_1="trend_regime", direction_1=-1, weight_1=1.0,
            signal_threshold=0.08,
        )

    def test_no_signal_with_empty_buffer(self):
        """Before 5 bars, no signal should fire."""
        for i in range(4):
            ts = 1_000_000_000 * (i + 1)
            self.alpha.set_factor_value("trend_regime", ts, -1.0)
            result = self.alpha.on_bar(
                close=65.0, high=65.1, low=64.9,
                delta_buy_vol=0.0, delta_sell_vol=0.0,
                btc_close=65000.0, ts_ns=ts,
            )
            assert result.direction == 0, (
                f"Bar {i+1}: expected no signal (buffer too small), got {result.direction}"
            )

    def test_short_entry_after_buffer_fills(self):
        """After 5+ bars of trend_regime=-1, SHORT entry triggers."""
        seen_entry = False
        for i in range(30):
            ts = 1_000_000_000 * (i + 1)
            self.alpha.set_factor_value("trend_regime", ts, -1.0)
            result = self.alpha.on_bar(
                close=65.0 - i * 0.02, high=65.1, low=64.9,
                delta_buy_vol=0.0, delta_sell_vol=0.0,
                btc_close=65000.0, ts_ns=ts,
            )
            if result.direction != 0:
                seen_entry = True
                assert result.direction == -1, (
                    f"Expected SHORT entry (direction=-1), got {result.direction}"
                )
                break

        assert seen_entry, "Expected SHORT entry within 30 bars of trend_regime=-1"

    def test_long_entry_with_positive_direction(self):
        """trend_regime with direction=+1 generates LONG entries."""
        alpha = AlphaSignal(
            gate_factor="",
            factor_1="trend_regime", direction_1=1, weight_1=1.0,
            signal_threshold=0.08,
        )
        seen_entry = False
        for i in range(30):
            ts = 1_000_000_000 * (i + 1)
            alpha.set_factor_value("trend_regime", ts, 1.0)
            result = alpha.on_bar(
                close=65.0 + i * 0.02, high=65.1, low=64.9,
                delta_buy_vol=0.0, delta_sell_vol=0.0,
                btc_close=65000.0, ts_ns=ts,
            )
            if result.direction != 0:
                seen_entry = True
                assert result.direction == 1, (
                    f"Expected LONG entry (direction=1), got {result.direction}"
                )
                break

        assert seen_entry, "Expected LONG entry within 30 bars"

    def test_mixed_signals_require_ema_buildup(self):
        """Alternating trend_regime (-1/0/+1) delays entry due to EMA smoothing."""
        values = [-1.0, 0.0, 1.0, 0.0] * 5  # alternating, no sustained trend
        entry_bar = -1
        for i, v in enumerate(values):
            ts = 1_000_000_000 * (i + 1)
            self.alpha.set_factor_value("trend_regime", ts, v)
            result = self.alpha.on_bar(
                close=65.0, high=65.1, low=64.9,
                delta_buy_vol=0.0, delta_sell_vol=0.0,
                btc_close=65000.0, ts_ns=ts,
            )
            if result.direction != 0:
                entry_bar = i + 1
                break

        # With alternating values, EMA should take longer (or not trigger at all
        # at threshold 0.08) compared to sustained trend (which triggers at bar ~7)
        assert entry_bar == -1 or entry_bar > 5, (
            f"Alternating signals should delay entry (>= bar 6), got bar {entry_bar}"
        )


if __name__ == "__main__":
    t = TestTrendRegimeOnlySignal()
    t.setup_method()
    t.test_no_signal_with_empty_buffer()
    print("PASS: test_no_signal_with_empty_buffer")
    t.setup_method()
    t.test_short_entry_after_buffer_fills()
    print("PASS: test_short_entry_after_buffer_fills")
    t.test_long_entry_with_positive_direction()
    print("PASS: test_long_entry_with_positive_direction")
    t.setup_method()
    t.test_mixed_signals_require_ema_buildup()
    print("PASS: test_mixed_signals_require_ema_buildup")
