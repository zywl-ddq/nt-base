"""
Factor:    trend_regime
Type:      Trend / Gate
Purpose:   Absolute price trend direction via rolling linear regression
           on 30-bar 1m closing prices. Returns direction (-1/0/+1) and
           confidence (R-squared normalized to [0,1]).

Output: pd.DataFrame with columns 'direction' and 'confidence'
"""
import numpy as np
import pandas as pd
WINDOW = 30
R2_THRESHOLD = 0.40   # minimum R-squared for valid trend detection
R2_CEILING = 0.6      # R-squared above this = full confidence


def factor_trend_regime(df, timescale="1min"):
    closes = df["close"].astype(float)
    n_bars = len(closes)

    direction = pd.Series(0.0, index=df.index, name="trend_regime")
    confidence = pd.Series(0.0, index=df.index, name="trend_confidence")

    if n_bars < WINDOW:
        return pd.DataFrame({"trend_regime": direction, "trend_confidence": confidence})

    y_vals = closes.values
    x = np.arange(WINDOW, dtype=float)
    n = float(WINDOW)
    sx = x.sum()
    sxx = (x * x).sum()
    denom = n * sxx - sx * sx

    if abs(denom) < 1e-12:
        return pd.DataFrame({"trend_regime": direction, "trend_confidence": confidence})

    for i in range(WINDOW - 1, n_bars):
        yw = y_vals[i - WINDOW + 1 : i + 1]
        y_mean = yw.mean()
        if y_mean <= 0:
            continue

        sy = yw.sum()
        sxy = (x * yw).sum()
        slope = (n * sxy - sx * sy) / denom

        intercept = (sy - slope * sx) / n
        y_pred = slope * x + intercept
        ss_res = ((yw - y_pred) ** 2).sum()
        ss_tot = ((yw - y_mean) ** 2).sum()

        if ss_tot < 1e-12:
            continue

        r2 = 1.0 - ss_res / ss_tot
        if r2 < R2_THRESHOLD:
            continue

        norm_slope = slope / (y_mean + 1e-9)
        norm_slope = float(np.clip(norm_slope * 100, -1.0, 1.0))

        direction.iloc[i] = float(np.sign(norm_slope))
        confidence.iloc[i] = min(r2 / R2_CEILING, 1.0)

    return pd.DataFrame({"trend_regime": direction, "trend_confidence": confidence})
