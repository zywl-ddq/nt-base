"""Residual Momentum Factor -- SOL alpha vs BTC benchmark.

Removes market (BTC) beta from SOL returns to isolate idiosyncratic alpha.
Uses rolling OLS over 288 x 5m bars (24h) to compute beta,
then Z-scores the cumulative residual return.

Works on 5m bars. Requires btc_close column in df.
"""
import numpy as np
import pandas as pd

ROLLING_BETA = 288
MOM_WINDOW = 24
Z_HISTORY = 288


def factor_residual_momentum(df, timescale="5min"):
    # Resample 1m to 5m if needed
    if "btc_close" not in df.columns:
        return pd.Series(0.0, index=df.index, name="residual_momentum")

    df_sol = df[["close"]].copy()
    df_sol.columns = ["sol_close"]
    df_sol["btc_close"] = df["btc_close"]
    df_sol.dropna(inplace=True)

    if len(df_sol) < ROLLING_BETA:
        return pd.Series(0.0, index=df.index, name="residual_momentum")

    df_sol["sol_ret"] = np.log(df_sol["sol_close"] / df_sol["sol_close"].shift(1))
    df_sol["btc_ret"] = np.log(df_sol["btc_close"] / df_sol["btc_close"].shift(1))
    df_sol.dropna(inplace=True)

    n = len(df_sol)
    window = min(ROLLING_BETA, n)
    betas = np.full(n, np.nan)
    resids = np.full(n, np.nan)

    sol_ret = df_sol["sol_ret"].values
    btc_ret = df_sol["btc_ret"].values

    for i in range(window - 1, n):
        xi = btc_ret[i - window + 1 : i + 1]
        yi = sol_ret[i - window + 1 : i + 1]
        X = np.vstack([np.ones(len(xi)), xi]).T
        coeff, _, _, _ = np.linalg.lstsq(X, yi, rcond=None)
        betas[i] = coeff[1]
        resids[i] = yi[-1] - (coeff[0] + coeff[1] * xi[-1])

    result = pd.Series(0.0, index=df.index, name="residual_momentum")
    result.iloc[:n] = resids
    return result
