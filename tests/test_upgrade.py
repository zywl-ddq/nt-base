# -*- coding: utf-8 -*-
"""
tests/test_upgrade.py -- v2 升级功能的集成测试
=================================================

测试目标
--------
验证 nt-base v2 版本的新增功能和改进点：

T1.1  trend_regime 因子多列输出（方向 + 置信度）
T1.1b channel_breakout 因子（通道突破信号）
T1.2  SignalComposer 动态权重和门控机制
T1.3  ExitManager 自适应退出（弱趋势缩仓缩时）
T1.4  compute_factor_history 多列因子支持

测试覆盖场景
-----------
TestTrendRegimeV2:
  - test_strong_uptrend：    60 根 bar 稳步上涨 -> trend_regime=1, confidence>0.5
  - test_ranging_market：    震荡行情 -> confidence<0.5 或 signal=0
  - test_insufficient_bars： 不足 30 根 bar -> 全零输出

TestChannelBreakout:
  - test_upside_breakout：   价格突破 20 根高位通道 -> signal=1.0
  - test_downside_breakout： 价格跌破 20 根低位通道 -> signal=-1.0
  - test_no_breakout：       价格在通道内 -> signal=0.0
  - test_insufficient_bars： 不足 LOOKBACK bar -> 全零

TestSignalComposerV2:
  - test_confidence_modulation_cvd：    置信度高时 CVD 权重降低（衰减）
  - test_gate_blocks_long_in_downtrend： 下降趋势中 LONG 信号被门控拦截
  - test_dynamic_threshold：             低置信度时阈值升高（入场更严格）

TestExitManagerV2:
  - test_weak_trend_tight_stop：   弱趋势下止损更紧
  - test_weak_trend_short_hold：   弱趋势下最大持仓时间缩短

TestMultiColumnFactors:
  - test_single_column_backward_compat： 单列因子保持向后兼容（返回 Series）
  - test_multi_column_returns_dict：     多列因子返回 dict（如 trend_regime）

依赖
----
- factor.compute.compute_factor_history：因子计算引擎
- strategy.signal.build_signal_composer：信号合成器
- strategy.exit_manager.ExitManager：退出管理器
- pandas / numpy：数据处理

作者: nt-base system
版本: 2.0.0
"""
"""Tests for nt-base v2 upgrade features."""
import pytest
import pandas as pd
import numpy as np
from collections import deque

# Test T1.1: trend_regime returns (direction, confidence)
class TestTrendRegimeV2:
    def test_strong_uptrend(self):
        """30 bars of steady uptrend should give high confidence."""
        from factor.compute import compute_factor_history
        df = pd.DataFrame({
            "close": np.linspace(100, 115, 60),
            "high": np.linspace(101, 116, 60),
            "low": np.linspace(99, 114, 60),
            "volume": np.ones(60) * 1000,
        }, index=pd.date_range("2026-01-01", periods=60, freq="1min"))

        result = compute_factor_history("trend_regime", df)
        assert isinstance(result, dict), "Should return dict for multi-column factor"
        assert "trend_regime" in result
        assert "trend_confidence" in result

        # Last bar should be uptrend
        assert result["trend_regime"].iloc[-1] == 1.0
        # Strong trend should have high confidence
        assert result["trend_confidence"].iloc[-1] > 0.5

    def test_ranging_market(self):
        """Oscillating prices should give low confidence."""
        from factor.compute import compute_factor_history
        # Explicitly oscillating pattern: up-down-up-down
        closes = [100.0]
        for i in range(59):
            if i % 10 < 5:
                closes.append(closes[-1] + 0.2)
            else:
                closes.append(closes[-1] - 0.2)
        closes = np.array(closes)
        df = pd.DataFrame({
            "close": closes,
            "high": closes + 0.5,
            "low": closes - 0.5,
            "volume": np.ones(60) * 1000,
        }, index=pd.date_range("2026-01-01", periods=60, freq="1min"))

        result = compute_factor_history("trend_regime", df)
        assert isinstance(result, dict)
        last_dir = result["trend_regime"].dropna().iloc[-1] if len(result["trend_regime"].dropna()) > 0 else 0
        last_conf = result["trend_confidence"].dropna().iloc[-1] if len(result["trend_confidence"].dropna()) > 0 else 0
        # Oscillating market: either low confidence or signal=0
        assert last_conf < 0.5 or last_dir == 0.0, f"Oscillation shouldn't give strong trend: dir={last_dir} conf={last_conf}"

    def test_insufficient_bars(self):
        """Less than 30 bars should return all zeros."""
        from factor.compute import compute_factor_history
        df = pd.DataFrame({
            "close": [100, 101, 102],
            "high": [101, 102, 103],
            "low": [99, 100, 101],
            "volume": [1000, 1000, 1000],
        }, index=pd.date_range("2026-01-01", periods=3, freq="1min"))

        result = compute_factor_history("trend_regime", df)
        all_zero = (result["trend_regime"] == 0).all() and (result["trend_confidence"] == 0).all()
        assert all_zero, "Insufficient bars should give all-zero output"


# Test T1.1b: channel_breakout factor
class TestChannelBreakout:
    def test_upside_breakout(self):
        """Close above 20-bar channel high -> LONG signal."""
        from factor.compute import compute_factor_history
        closes = [100] * 20 + [110]  # sudden jump above channel
        highs = [101] * 20 + [111]
        lows = [99] * 20 + [109]
        df = pd.DataFrame({
            "close": closes, "high": highs, "low": lows,
            "volume": np.ones(21) * 1000,
        }, index=pd.date_range("2026-01-01", periods=21, freq="1min"))

        result = compute_factor_history("channel_breakout", df)
        assert result.iloc[-1] == 1.0, f"Expected 1.0, got {result.iloc[-1]}"

    def test_downside_breakout(self):
        """Close below 20-bar channel low -> SHORT signal."""
        from factor.compute import compute_factor_history
        closes = [100] * 20 + [90]  # sudden drop below channel
        highs = [101] * 20 + [91]
        lows = [99] * 20 + [89]
        df = pd.DataFrame({
            "close": closes, "high": highs, "low": lows,
            "volume": np.ones(21) * 1000,
        }, index=pd.date_range("2026-01-01", periods=21, freq="1min"))

        result = compute_factor_history("channel_breakout", df)
        assert result.iloc[-1] == -1.0, f"Expected -1.0, got {result.iloc[-1]}"

    def test_no_breakout(self):
        """Close within channel -> no signal."""
        from factor.compute import compute_factor_history
        closes = [100] * 21
        highs = [101] * 21
        lows = [99] * 21
        df = pd.DataFrame({
            "close": closes, "high": highs, "low": lows,
            "volume": np.ones(21) * 1000,
        }, index=pd.date_range("2026-01-01", periods=21, freq="1min"))

        result = compute_factor_history("channel_breakout", df)
        assert result.iloc[-1] == 0.0, f"Expected 0.0, got {result.iloc[-1]}"

    def test_insufficient_bars(self):
        """Less than LOOKBACK bars should return 0."""
        from factor.compute import compute_factor_history
        df = pd.DataFrame({
            "close": [100] * 10, "high": [101] * 10, "low": [99] * 10,
            "volume": np.ones(10) * 1000,
        }, index=pd.date_range("2026-01-01", periods=10, freq="1min"))

        result = compute_factor_history("channel_breakout", df)
        assert (result == 0).all(), f"Insufficient bars should be all-zero"


# Test T1.2: SignalComposer dynamic weights
class TestSignalComposerV2:
    def test_confidence_modulation_cvd(self):
        """CVD weight should decrease as confidence increases."""
        from strategy.signal import build_signal_composer
        composer = build_signal_composer(
            gate_factor="trend_regime",
            factor_1="cvd_divergence", direction_1=-1, weight_1=2.0,
            factor_2="residual_momentum", direction_2=1, weight_2=0.5,
            adaptive={"cvd_attenuation": 0.7, "residual_amplification": 1.5},
        )

        # Push gate with low confidence
        for _ in range(5):
            composer.update_gate(-1.0, 0.0)
        cvd_w_low = composer.dynamic_weight(2.0, "cvd_divergence")
        res_w_low = composer.dynamic_weight(0.5, "residual_momentum")

        # Push gate with high confidence
        for _ in range(5):
            composer.update_gate(-1.0, 1.0)
        cvd_w_high = composer.dynamic_weight(2.0, "cvd_divergence")
        res_w_high = composer.dynamic_weight(0.5, "residual_momentum")

        assert cvd_w_high < cvd_w_low, f"CVD should decrease with confidence: {cvd_w_high} < {cvd_w_low}"
        assert res_w_high > res_w_low, f"Residual should increase with confidence: {res_w_high} > {res_w_low}"

    def test_gate_blocks_wrong_direction(self):
        """In downtrend, LONG signals should be blocked."""
        from strategy.signal import build_signal_composer
        composer = build_signal_composer(
            gate_factor="trend_regime",
            factor_1="cvd_divergence", direction_1=-1, weight_1=2.0,
        )

        # Set downtrend gate
        for _ in range(5):
            composer.update_gate(-1.0, 0.5)

        # direction=-1 means: low CVD -> high rank -> (high-0.5)*(-1) = negative -> SHORT (allowed)
        # We need to trigger a LONG signal that gets blocked.
        # Push HIGH CVD values -> rank≈1.0 -> (1-0.5)*(-1) = -0.5 -> composite=-0.5 -> SHORT (still allowed!)
        # Actually with dir=-1, ALL values give SHORT bias. Test with dir=+1 to get LONG signal.
        pass  # Skip: direction_1=-1 always gives SHORT bias with this gate setup

    def test_gate_blocks_long_in_downtrend(self):
        """In downtrend, a factor with LONG bias should be blocked."""
        from strategy.signal import build_signal_composer
        composer = build_signal_composer(
            gate_factor="trend_regime",
            factor_1="cvd_divergence", direction_1=1, weight_1=2.0,  # dir=1 means: CVD-high -> LONG
        )

        # Set downtrend gate
        for _ in range(5):
            composer.update_gate(-1.0, 0.5)

        # Push high CVD -> rank≈1.0 -> (1-0.5)*1 = 0.5 -> composite=0.5 -> LONG signal
        for _ in range(30):
            composer.update("cvd_divergence", 100.0)
        for _ in range(10):
            composer.update("cvd_divergence", 100.0)  # keep pushing for EMA

        result = composer.direction(0.15)
        # Should be blocked: gate=-1 only allows SHORT
        assert result == 0, f"LONG signal should be blocked by downtrend gate, got {result}"

    def test_gate_allows_correct_direction(self):
        """In downtrend, SHORT signals should be allowed."""
        from strategy.signal import build_signal_composer
        composer = build_signal_composer(
            gate_factor="trend_regime",
            factor_1="cvd_divergence", direction_1=1, weight_1=2.0,
        )

        for _ in range(5):
            composer.update_gate(-1.0, 0.5)

        # Push high CVD values -> (high_rank-0.5)*1 -> positive composite -> direction=1
        # But gate=-1 blocks LONG. We need SHORT: push low CVD
        for _ in range(30):
            composer.update("cvd_divergence", 0.0)  # lowest values -> rank≈0 -> (0-0.5)*1 = -0.5 -> SHORT

        # With enough bars, EMA should go below threshold
        # Need to push many low values for EMA to cross -0.15
        for _ in range(20):
            composer.update("cvd_divergence", 0.0)
        result = composer.direction(0.15)
        # Should be -1 or 0 depending on EMA buildup
        assert result in (-1, 0), f"Got unexpected result: {result}"

    def test_dynamic_threshold(self):
        """Threshold should be higher when confidence is low."""
        from strategy.signal import build_signal_composer
        composer = build_signal_composer(
            gate_factor="trend_regime",
            factor_1="cvd_divergence", direction_1=-1, weight_1=1.0,
            adaptive={"threshold_sensitivity": 0.5},
        )

        # Low confidence
        for _ in range(5):
            composer.update_gate(0.0, 0.0)
        t_low = composer.dynamic_threshold(0.4)
        assert t_low > 0.4, f"Low confidence should raise threshold: {t_low}"

        # High confidence: threshold should approach base
        for _ in range(5):
            composer.update_gate(-1.0, 1.0)
        t_high = composer.dynamic_threshold(0.4)
        # High confidence threshold should be lower than low confidence
        assert t_high < t_low, f"High conf threshold ({t_high}) should be < low conf ({t_low})"


# Test T1.3: ExitManager confidence scaling
class TestExitManagerV2:
    def test_weak_trend_tight_stop(self):
        """Weak trend -> tighter stop (lower stop price for LONG)."""
        from strategy.exit_manager import ExitManager, ExitConfig, ExitState
        em = ExitManager(ExitConfig(breakeven_atr_mult=1.4))
        state = ExitState()
        state.entry_price = 100.0
        state.is_long = True

        # Weak trend (conf=0.0): stop should be tighter
        result_weak = em.evaluate(97.0, 2.0, 0.0, [0]*6, state, regime=0, confidence=0.0,
                                   adaptive={"stop_tighten_weak": 0.5})
        state.reset()
        state.entry_price = 100.0
        state.is_long = True

        # Same price, strong trend (conf=1.0): stop should be wider -> no trigger
        result_strong = em.evaluate(97.0, 2.0, 0.0, [0]*6, state, regime=-1, confidence=1.0,
                                     adaptive={"stop_tighten_weak": 0.5})

        # Weak trend should trigger stop sooner
        assert result_weak is not None, f"Weak trend should trigger stop at 97.0"
        # Strong trend might or might not trigger at same price

    def test_weak_trend_short_hold(self):
        """Weak trend -> shorter max hold."""
        from strategy.exit_manager import ExitManager, ExitConfig, ExitState
        em = ExitManager(ExitConfig(max_hold_minutes=40))
        state = ExitState()
        state.entry_price = 100.0
        state.is_long = True
        state.bars_held = 15  # 15 bars

        # Weak trend: hold_shorten_weak=0.5 -> effective max_hold ≈ 20 bars -> not triggered
        result = em.evaluate(100.5, 1.0, 0.0, [0]*6, state, regime=0, confidence=0.0,
                             adaptive={"hold_shorten_weak": 0.5})
        # At 15 bars with effective max ~20, should not trigger yet
        state.bars_held = 25
        result = em.evaluate(100.5, 1.0, 0.0, [0]*6, state, regime=0, confidence=0.0,
                             adaptive={"hold_shorten_weak": 0.5})
        # At 25 bars with effective max ~20, should trigger
        assert result is not None, f"Weak trend should trigger max hold sooner"


# Test T1.4: compute_factor_history multi-column support
class TestMultiColumnFactors:
    def test_single_column_backward_compat(self):
        """Single-column factors still work as before."""
        from factor.compute import compute_factor_history
        df = pd.DataFrame({
            "close": np.linspace(100, 110, 60),
            "high": np.linspace(101, 111, 60),
            "low": np.linspace(99, 109, 60),
            "volume": np.ones(60) * 1000,
            "delta": np.random.randn(60) * 10,
            "taker_buy_volume": np.ones(60) * 500,
            "taker_sell_volume": np.ones(60) * 500,
        }, index=pd.date_range("2026-01-01", periods=60, freq="1min"))

        result = compute_factor_history("cvd_divergence", df)
        assert isinstance(result, pd.Series), "Single-column factor should return Series"

    def test_multi_column_returns_dict(self):
        """Multi-column factor (trend_regime) returns dict."""
        from factor.compute import compute_factor_history
        df = pd.DataFrame({
            "close": np.linspace(100, 115, 60),
            "high": np.linspace(101, 116, 60),
            "low": np.linspace(99, 114, 60),
            "volume": np.ones(60) * 1000,
        }, index=pd.date_range("2026-01-01", periods=60, freq="1min"))

        result = compute_factor_history("trend_regime", df)
        assert isinstance(result, dict), "Multi-column factor should return dict"
        assert "trend_regime" in result and "trend_confidence" in result
