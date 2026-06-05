"""
Factor:    trend_regime
Type:      Trend / Gate
Purpose:   Absolute price trend direction via rolling linear regression
           on 30-bar 1m closing prices. Returns -1/0/+1.

Algorithm:
  1. For each bar i >= 30, fit y = slope*x + intercept on closes[i-29:i+1]
  2. Compute R-squared of the fit
  3. If R2 >= 0.4 and mean(close) > 0: classify trend
  4. Normalize slope by mean price, clip to [-1, 1], return sign

Parameters:
  WINDOW = 30          regression window (30 minutes)
  R2_THRESHOLD = 0.4   minimum fit quality for valid signal

Output: pd.Series of -1 (downtrend), 0 (ranging/noisy), +1 (uptrend)

Use Case:
  In SignalComposer, used as GATE factor to restrict allowed trade direction.
  In single-factor mode, acts as both entry signal and directional bias.

Dependencies: numpy, pandas (from sandbox namespace)
Author:    nt-base / trading-v2
Version:   1.0.0
"""
"""Trend Regime Factor -- absolute price trend direction.

Returns a Series of -1/0/+1 for each bar, computed via rolling linear
regression on 30-bar 1m closing prices.
"""
import numpy as np
import pandas as pd
WINDOW = 30
R2_THRESHOLD = 0.4


def factor_trend_regime(df, timescale="1min"):
    closes = df["close"].astype(float)
    result = pd.Series(0.0, index=df.index, name="trend_regime")

    if len(closes) < WINDOW:
        return result

    y_vals = closes.values
    x = np.arange(WINDOW, dtype=float)
    n = float(WINDOW)
    sx = x.sum()
    sxx = (x * x).sum()
    denom = n * sxx - sx * sx

    for i in range(WINDOW - 1, len(y_vals)):
        yw = y_vals[i - WINDOW + 1 : i + 1]
        y_mean = yw.mean()
        slope = ((x * yw).sum() * n - sx * yw.sum()) / denom if abs(denom) > 1e-12 else 0.0
        ss_tot = ((yw - y_mean) ** 2).sum()
        if ss_tot < 1e-12:
            continue
        intercept = (yw.sum() - slope * sx) / n
        y_pred = slope * x + intercept
        ss_res = ((yw - y_pred) ** 2).sum()
        r2 = 1.0 - ss_res / ss_tot
        if r2 < R2_THRESHOLD:
            continue
        if y_mean <= 0:
            continue
        norm_slope = slope / (y_mean + 1e-9)
        norm_slope = float(np.clip(norm_slope * 100, -1.0, 1.0))
        result.iloc[i] = float(np.sign(norm_slope))

    return result
