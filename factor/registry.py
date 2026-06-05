"""
Module:    factor/registry
Purpose:   Factor catalog: name -> definition mapping. Provides factor metadata
           and discovery for compute_factor_history().

Data Class: FactorDef
  name: str             factor identifier (e.g. "trend_regime")
  file: str             source file in factors/ directory
  windows: list[int]    required lookback windows
  output_range: tuple   expected (min, max) output range

Factory: FACTORS dict -> resolve_factor(name) -> FactorDef

Author:    nt-base / trading-v2
Version:   1.0.0
"""
from __future__ import annotations
"""Factor catalog: name -> definition mapping."""
from dataclasses import dataclass


@dataclass(frozen=True)
class FactorDef:
    name: str
    file: str
    windows: list[int]
    output_range: tuple[float, float]


FACTORS: dict[str, FactorDef] = {
    "trend_regime": FactorDef(
        name="trend_regime",
        file="trend_regime.py",
        windows=[30],
        output_range=(-1.0, 1.0),
    ),
    "cvd_divergence": FactorDef(
        name="cvd_divergence",
        file="cvd_divergence.py",
        windows=[60],
        output_range=(-3.0, 3.0),
    ),
    "residual_momentum": FactorDef(
        name="residual_momentum",
        file="residual_momentum.py",
        windows=[288],
        output_range=(-3.0, 3.0),
    ),
}


def resolve_factor(name: str) -> FactorDef:
    if name not in FACTORS:
        raise KeyError(f"Unknown factor: {name}. Available: {list(FACTORS)}")
    return FACTORS[name]


def list_factors() -> list[str]:
    return sorted(FACTORS.keys())
