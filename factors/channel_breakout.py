"""
Factor:    channel_breakout
Type:      Trend-Following
Purpose:   Price channel breakout detection. When close breaks above the
           highest high of the last N bars, signal LONG. When close breaks
           below the lowest low of the last N bars, signal SHORT.

Parameters:
  LOOKBACK = 20        channel lookback bars
  CONFIRMATION = 1     bars above/below channel to confirm breakout

Output: pd.Series of -1 (breakdown SHORT), 0 (no breakout), +1 (breakout LONG)
"""
import numpy as np
import pandas as pd

LOOKBACK = 20
CONFIRMATION = 1


def factor_channel_breakout(df, timescale="1min"):
    closes = df["close"].astype(float)
    highs = df["high"].astype(float)
    lows = df["low"].astype(float)

    n = len(closes)
    result = pd.Series(0.0, index=df.index, name="channel_breakout")

    if n < LOOKBACK + CONFIRMATION:
        return result

    for i in range(LOOKBACK + CONFIRMATION - 1, n):
        # Channel from bars [i-LOOKBACK-CONFIRMATION+1, i-CONFIRMATION]
        channel_start = i - LOOKBACK - CONFIRMATION + 1
        channel_end = i - CONFIRMATION

        channel_high = highs.iloc[channel_start:channel_end + 1].max()
        channel_low = lows.iloc[channel_start:channel_end + 1].min()
        current_close = closes.iloc[i]

        # CVD confirmation: breakout needs delta in same direction
        # Only confirm if delta column exists in df
        delta_ok = True
        if "delta" in df.columns:
            recent_delta_sum = df["delta"].iloc[max(0,i-2):i+1].sum()
            if current_close > channel_high and recent_delta_sum < 0:
                delta_ok = False  # upside breakout but net selling
            if current_close < channel_low and recent_delta_sum > 0:
                delta_ok = False  # downside breakout but net buying

        if delta_ok and current_close > channel_high:
            result.iloc[i] = 1.0
        elif delta_ok and current_close < channel_low:
            result.iloc[i] = -1.0

    return result
