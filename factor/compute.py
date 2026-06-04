"""Shared factor computation for backtest and live environments.

Both environments call the SAME factor code on the SAME 1min bar data,
guaranteeing identical factor values regardless of execution context.
"""
import logging

import numpy as np
import scipy
import pandas as pd

logger = logging.getLogger(__name__)

_SAFE_BUILTINS = {
    "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
    "enumerate": enumerate, "float": float, "int": int, "len": len,
    "list": list, "max": max, "min": min, "range": range, "round": round,
    "set": set, "sorted": sorted, "str": str, "sum": sum, "tuple": tuple,
    "zip": zip, "True": True, "False": False, "None": None,
    "isinstance": isinstance, "ValueError": ValueError,
    "TypeError": TypeError, "ZeroDivisionError": ZeroDivisionError,
}

_FACTOR_NAMESPACE_BASE = {"scipy": scipy, "pd": pd, "np": np, "__builtins__": __builtins__}


def _load_factor_code(code: str) -> str:
    """Accepts either a factor name (looks up in factors dir) or raw code string."""
    from pathlib import Path
    for factors_dir in (
        Path("/root/nt-base/factors"),
        Path("/root/nt-base/factors"),
    ):
        candidate = factors_dir / f"{code}.py"
        if candidate.exists():
            return candidate.read_text()
    return code


def _coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns to float64 so factor math works correctly.

    Handles Decimal (from asyncpg) and integer columns that would otherwise
    cause type errors in mixed arithmetic with numpy floats.
    """
    import decimal
    _DECIMAL = decimal.Decimal
    for col in df.columns:
        dtype = df[col].dtype
        if dtype in (np.dtype("int64"), np.dtype("int32")):
            df[col] = df[col].astype(np.float64)
        elif dtype == np.dtype("object"):
            sample = df[col].dropna()
            if len(sample) and isinstance(sample.iloc[0], _DECIMAL):
                df[col] = df[col].astype(np.float64)
    return df


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns so factor code can reference them."""
    import numpy as np
    close = df["close"].astype(float)

    for col in ["bid_ask_spread_avg", "order_book_imbalance"]:
        if col not in df: df[col] = 0.0
    if "bid_depth_weighted" not in df: df["bid_depth_weighted"] = close * 0.999
    if "ask_depth_weighted" not in df: df["ask_depth_weighted"] = close * 1.001
    for col in ["funding_rate", "funding_settle_countdown", "open_interest",
                 "oi_delta_5m", "buy_liquidation_value", "sell_liquidation_value",
                 "liquidation_imbalance", "large_trade_ratio"]:
        if col not in df: df[col] = 0.0
    if "long_short_ratio" not in df: df["long_short_ratio"] = 1.0
    if "taker_buy_volume" not in df: df["taker_buy_volume"] = df.get("volume", 0) * 0.5
    if "taker_sell_volume" not in df: df["taker_sell_volume"] = df.get("volume", 0) * 0.5
    if "buy_sell_pressure_ratio" not in df: df["buy_sell_pressure_ratio"] = 1.0
    if "avg_trade_size" not in df: df["avg_trade_size"] = df.get("volume", 0) / (df.get("trade_count", 1) + 1)
    if "realized_vol_5m" not in df: df["realized_vol_5m"] = close.pct_change().rolling(5).std().fillna(0)
    if "realized_vol_1h" not in df: df["realized_vol_1h"] = close.pct_change().rolling(60).std().fillna(0)
    if "trend_strength_1h" not in df:
        df["trend_strength_1h"] = close.diff(60).fillna(0) / (close.shift(60).fillna(close.iloc[0]) + 1e-9)
    return df


def _execute_factor(code: str, df: pd.DataFrame) -> pd.Series:
    """Execute a single factor code string against a DataFrame.

    Supports two patterns:
    1. The code defines a ``factor_*`` function that accepts ``df`` (and optionally
       ``timescale``) and returns a ``pd.Series``.
    2. The code directly produces a ``pd.Series`` whose variable name starts
       with ``factor_*``.
    """
    df_enriched = _add_derived_columns(_coerce_numeric(df.copy()))
    namespace = {**_FACTOR_NAMESPACE_BASE, "df": df_enriched}
    exec(code, namespace)

    # Pattern 1: callable functions named factor_*
    import inspect
    for name, obj in namespace.items():
        if callable(obj) and name.startswith("factor_"):
            try:
                sig = inspect.signature(obj)
                if "timescale" in sig.parameters:
                    result = obj(df=namespace["df"], timescale="1min")
                else:
                    result = obj(df=namespace["df"])
            except ValueError:
                result = obj(df=namespace["df"])
            if isinstance(result, pd.Series):
                return pd.to_numeric(result, errors="coerce")

    # Pattern 2: already-computed Series named factor_*
    for name, obj in namespace.items():
        if isinstance(obj, pd.Series) and name.startswith("factor_"):
            return pd.to_numeric(obj, errors="coerce")

    raise ValueError("No factor_* Series found in factor code namespace")


def compute_factor_history(
    factor_code: str,
    df_1min: pd.DataFrame,
) -> pd.Series:
    """Batch compute factor over full 1min history.

    Args:
        factor_code: Factor name (e.g. 'factor_cross_auto_corr') or raw Python code.
        df_1min: Full 1min bar DataFrame with DatetimeIndex.

    Returns:
        Series indexed by ts (1min bar close times), values = factor values.
    """
    code = _load_factor_code(factor_code)
    result = _execute_factor(code, df_1min)
    result.index = df_1min.index
    return result.dropna()


def compute_factor_incremental(
    factor_code: str,
    df_full: pd.DataFrame,
) -> float | None:
    """Compute factor value for the latest 1min bar only.

    Args:
        factor_code: Factor name or raw Python code.
        df_full: All available 1min bars up to and including current bar.

    Returns:
        Factor value at the latest bar, or None if computation fails.
    """
    code = _load_factor_code(factor_code)
    try:
        series = _execute_factor(code, df_full)
        if series.empty:
            return None
        return float(series.iloc[-1])
    except Exception as e:
        logger.error(f"factor_incremental failed: {e}", exc_info=True)
        return None
