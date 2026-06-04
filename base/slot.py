"""StrategySlot — runtime state for a registered strategy."""
from __future__ import annotations
from dataclasses import dataclass, field
import time
from base.signal_protocol import SignalStrategy, BarSubscription


@dataclass
class StrategySlot:
    strategy_id: str
    strategy: SignalStrategy
    subscriptions: list[BarSubscription] = field(default_factory=list)
    stop_pct: float = 0.03
    take_pct: float = 0.06
    max_hold_sec: int = 3600
    max_daily_loss_pct: float = 0.05
    cooldown_sec: float = 60.0
    leverage: int = 2
    position_size_pct: float = 0.20

    # Runtime state
    has_position: bool = False
    entry_price: float = 0.0
    entry_side: str = ""
    entry_time: float = 0.0
    last_trade_time: float = 0.0
    daily_pnl: float = 0.0
    daily_start_equity: float = 0.0
    tripped: bool = False

    def reset_position(self):
        self.has_position = False
        self.entry_price = 0.0
        self.entry_side = ""
        self.entry_time = 0.0

    def open_position(self, side: str, price: float):
        self.has_position = True
        self.entry_side = side
        self.entry_price = price
        self.entry_time = time.time()

    @property
    def held_sec(self) -> float:
        if not self.has_position:
            return 0.0
        return time.time() - self.entry_time
