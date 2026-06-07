"""FactorEngine — generic container for strategy-registered factor code.

Strategies register factor source code via gRPC. The engine compiles,
caches, and executes it against bar data. Results are attached to Bar messages.
"""
from __future__ import annotations
import logging

import numpy as np
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


class FactorEngine:
    """Compiles, caches, and executes factor code registered by strategies."""

    def __init__(self):
        self._factors: dict[str, dict] = {}

    def register(self, name: str, code: str, params: dict[str, float] | None = None):
        """Register or update a factor. Compiles the code for fast execution."""
        try:
            compiled = compile(code, f"<factor:{name}>", "exec")
        except SyntaxError as e:
            logger.error(f"Factor '{name}' syntax error: {e}")
            raise
        self._factors[name] = {"code": code, "params": params or {}, "compiled": compiled}
        logger.info(f"Factor registered: {name}")

    def unregister(self, name: str):
        if name in self._factors:
            del self._factors[name]

    def registered_names(self) -> list[str]:
        return list(self._factors.keys())

    def execute_all(self, df_bars: pd.DataFrame) -> dict[str, float]:
        """Execute all registered factors, return {name: latest_value}."""
        results = {}
        for name, meta in self._factors.items():
            val = self._execute_one(name, meta, df_bars)
            if val is not None:
                results[name] = val
        return results

    def _execute_one(self, name: str, meta: dict, df: pd.DataFrame) -> float | None:
        """Execute a single factor against the DataFrame and return the latest value."""
        namespace = {**_SAFE_BUILTINS, "np": np, "pd": pd, "df": df.copy(), **meta["params"]}
        try:
            exec(meta["compiled"], namespace)
        except Exception as e:
            logger.error(f"Factor '{name}' execution error: {e}")
            return None

        # Look for a Series matching the factor name
        for key in (name, f"factor_{name}"):
            obj = namespace.get(key)
            if isinstance(obj, pd.Series) and not obj.empty:
                val = obj.dropna()
                return float(val.iloc[-1]) if len(val) > 0 else 0.0

        # Fallback: any non-private Series
        for key, obj in namespace.items():
            if isinstance(obj, pd.Series) and not obj.empty and not key.startswith("_"):
                val = obj.dropna()
                return float(val.iloc[-1]) if len(val) > 0 else 0.0

        return 0.0

    def compute_history(self, name: str, df_bars: pd.DataFrame) -> pd.Series | None:
        """Batch compute a factor over full history (for backtest pre-computation)."""
        if name not in self._factors:
            return None
        meta = self._factors[name]
        namespace = {**_SAFE_BUILTINS, "np": np, "pd": pd, "df": df_bars.copy(), **meta["params"]}
        try:
            exec(meta["compiled"], namespace)
        except Exception as e:
            logger.error(f"Factor '{name}' batch error: {e}")
            return None

        for key in (name, f"factor_{name}"):
            obj = namespace.get(key)
            if isinstance(obj, pd.Series):
                result = pd.to_numeric(obj, errors="coerce")
                result.index = df_bars.index
                return result.dropna()

        for key, obj in namespace.items():
            if isinstance(obj, pd.Series) and not obj.empty and not key.startswith("_"):
                result = pd.to_numeric(obj, errors="coerce")
                result.index = df_bars.index
                return result.dropna()

        return None
