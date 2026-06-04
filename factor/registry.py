"""Factor catalog: name -> definition mapping."""
from __future__ import annotations
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
