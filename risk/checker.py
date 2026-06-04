"""Risk checker — pure functions for stop/take/hold/daily checks."""
from __future__ import annotations
from dataclasses import dataclass
from base.slot import StrategySlot


@dataclass
class RiskAction:
    kind: str = ""
    reason: str = ""

    @property
    def should_exit(self) -> bool:
        return self.kind != "none"


def check_stop(slot: StrategySlot, current_price: float) -> RiskAction:
    if not slot.has_position:
        return RiskAction("none")
    pnl_pct = (current_price - slot.entry_price) / slot.entry_price
    if slot.entry_side == "SHORT":
        pnl_pct = -pnl_pct
    if pnl_pct <= -slot.stop_pct:
        return RiskAction("stop_loss", f"stop {pnl_pct:.4f}")
    return RiskAction("none")


def check_take(slot: StrategySlot, current_price: float) -> RiskAction:
    if not slot.has_position:
        return RiskAction("none")
    pnl_pct = (current_price - slot.entry_price) / slot.entry_price
    if slot.entry_side == "SHORT":
        pnl_pct = -pnl_pct
    if pnl_pct >= slot.take_pct:
        return RiskAction("take_profit", f"take {pnl_pct:.4f}")
    return RiskAction("none")


def check_hold(slot: StrategySlot) -> RiskAction:
    if not slot.has_position:
        return RiskAction("none")
    if slot.held_sec >= slot.max_hold_sec:
        return RiskAction("max_hold", f"held {slot.held_sec:.0f}s")
    return RiskAction("none")


def check_daily(slot: StrategySlot) -> RiskAction:
    if slot.daily_start_equity <= 0:
        return RiskAction("none")
    daily_ret = slot.daily_pnl / slot.daily_start_equity
    if daily_ret < -slot.max_daily_loss_pct:
        return RiskAction("daily_limit", f"daily loss {daily_ret:.4f}")
    return RiskAction("none")


def check_all(slot: StrategySlot, current_price: float) -> list[RiskAction]:
    checks = [
        check_stop(slot, current_price),
        check_take(slot, current_price),
        check_hold(slot),
        check_daily(slot),
    ]
    return [c for c in checks if c.should_exit]
