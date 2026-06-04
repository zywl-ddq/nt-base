"""CVD Divergence Factor -- order-flow imbalance detection.

Div_Factor = Price_Z - CVD_Z over a 60-bar rolling window.
Uses np, pd from the sandbox namespace.
"""
import numpy as np
import pandas as pd
WINDOW = 60
MAX_FFILL_GAP = 3


def factor_cvd_divergence(df, timescale="1min"):
    df = df.copy()

    if "delta" not in df.columns:
        if "taker_buy_volume" in df.columns:
            sell_vol = df["volume"].astype(float) - df["taker_buy_volume"].astype(float)
            df["delta"] = df["taker_buy_volume"].astype(float) - sell_vol
        else:
            return pd.Series(0.0, index=df.index, name="div_factor")

    df["delta"] = df["delta"].ffill(limit=MAX_FFILL_GAP).fillna(0)
    df.dropna(subset=["close"], inplace=True)

    if len(df) < WINDOW:
        return pd.Series(np.nan, index=df.index, name="div_factor")

    df["cvd"] = df["delta"].rolling(WINDOW, min_periods=10).sum()

    pm = df["close"].rolling(WINDOW, min_periods=10).mean()
    ps = df["close"].rolling(WINDOW, min_periods=10).std().replace(0, np.nan)
    df["price_z"] = (df["close"] - pm) / ps

    cm = df["cvd"].rolling(WINDOW, min_periods=10).mean()
    cs = df["cvd"].rolling(WINDOW, min_periods=10).std().replace(0, np.nan)
    df["cvd_z"] = (df["cvd"] - cm) / cs

    df["div_factor"] = df["price_z"] - df["cvd_z"]
    return df["div_factor"]
