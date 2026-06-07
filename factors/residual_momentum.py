"""
Factor:    residual_momentum
Type:      Cross-Sectional
Purpose:   SOL alpha vs BTC benchmark. Removes market (BTC) beta from SOL
           returns to isolate idiosyncratic alpha via rolling OLS.

Algorithm:
  1. Resample 1m bars to 5m
  2. Compute log returns for SOL and BTC
  3. Rolling OLS: SOL_ret = alpha + beta * BTC_ret over ROLLING_BETA bars
  4. Residual = actual SOL_ret - predicted SOL_ret
  5. Z-score residuals over Z_HISTORY bars -> momentum signal

Parameters:
  ROLLING_BETA = 60   OLS window (~5h at 5m)
  MOM_WINDOW = 12      momentum lookback
  Z_HISTORY = 60      Z-score normalization window

Output: pd.Series of residual momentum z-score values

Pre-conditions:
  df must have 'btc_close' column (BTC 1m close prices joined to SOL bars).
  At least ROLLING_BETA bars required for valid beta estimation.

Edge Cases:
  - Missing btc_close column: returns all-zero series
  - Collinear BTC returns: lstsq may produce degenerate fit (handled by numpy)

Dependencies: numpy, pandas (from sandbox namespace)
Author:    nt-base / trading-v2
Version:   1.1.0
"""
import numpy as np
import pandas as pd

ROLLING_BETA = 60
MOM_WINDOW = 12
Z_HISTORY = 60


def factor_residual_momentum(df, timescale="5min"):
    if "btc_close" not in df.columns:
        return pd.Series(0.0, index=df.index, name="residual_momentum")

    df_sol = df[["close"]].copy()
    df_sol.columns = ["sol_close"]
    df_sol["btc_close"] = df["btc_close"]
    df_sol.dropna(inplace=True)

    if len(df_sol) < ROLLING_BETA:
        return pd.Series(0.0, index=df.index, name="residual_momentum")

    # Resample 1m to 5m
    if timescale == "5min":
        df_sol = df_sol.resample("5min").last().dropna()

    if len(df_sol) < ROLLING_BETA:
        return pd.Series(0.0, index=df.index, name="residual_momentum")

    df_sol["sol_ret"] = np.log(df_sol["sol_close"] / df_sol["sol_close"].shift(1))
    df_sol["btc_ret"] = np.log(df_sol["btc_close"] / df_sol["btc_close"].shift(1))
    df_sol.dropna(inplace=True)

    if len(df_sol) < ROLLING_BETA:
        return pd.Series(0.0, index=df.index, name="residual_momentum")

    n = len(df_sol)
    window = min(ROLLING_BETA, n)
    resids = np.full(n, np.nan)

    sol_ret = df_sol["sol_ret"].values
    btc_ret = df_sol["btc_ret"].values

    for i in range(window - 1, n):
        xi = btc_ret[i - window + 1 : i + 1]
        yi = sol_ret[i - window + 1 : i + 1]
        X = np.vstack([np.ones(len(xi)), xi]).T
        coeff, _, _, _ = np.linalg.lstsq(X, yi, rcond=None)
        resids[i] = yi[-1] - (coeff[0] + coeff[1] * xi[-1])

    residual_series = pd.Series(resids, index=df_sol.index, name="residual_momentum")

    # Z-score normalization over Z_HISTORY bars
    rolling_mean = residual_series.rolling(Z_HISTORY, min_periods=10).mean()
    rolling_std = residual_series.rolling(Z_HISTORY, min_periods=10).std().replace(0, np.nan)
    z_scored = (residual_series - rolling_mean) / rolling_std

    # Reindex back to original 1m index, forward-fill 5m values
    result = z_scored.reindex(df.index).ffill()
    return result
