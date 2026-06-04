"""SignalStrategy protocol — interface between strategies and trading base."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol


class SignalKind:
    LONG = 1
    SHORT = -1
    FLAT = 0


@dataclass
class StrategySignal:
    direction: int    # 1=LONG, -1=SHORT, 0=FLAT
    reason: str = ""


@dataclass
class BarSubscription:
    symbol: str
    timeframe: str
    factors: list[str]

    def __hash__(self):
        return hash((self.symbol, self.timeframe))


class SignalStrategy(Protocol):
    @property
    def strategy_id(self) -> str: ...
    @property
    def subscriptions(self) -> list[BarSubscription]: ...
    def on_bar(self, bar_data: dict) -> StrategySignal | None: ...
    def on_shutdown(self) -> None: ...
    def get_diagnostics(self) -> dict: ...
