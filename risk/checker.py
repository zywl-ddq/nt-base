"""
Module:    risk/checker
Purpose:   Risk check functions for position management.
           Each function takes a StrategySlot + current price and returns
           a CheckResult indicating whether to exit the position.

Interface:
  CheckResult (dataclass)   -- should_exit: bool, reason: str
  check_trail(slot, price)  -- trailing stop check (tick-level)
  check_stop(slot, price)   -- hard stop-loss check
  check_take(slot, price)   -- take-profit check
  check_hold(slot, price)   -- max hold time check
  check_daily(slot)         -- daily loss limit check

Exit Conditions:
  check_trail:  price crosses trailing stop (based on highest/lowest since entry)
  check_stop:   current_pnl% <= -stop_pct   (e.g. price dropped 3% from entry)
  check_take:   current_pnl% >= take_pct    (e.g. price rose 6% from entry)
  check_hold:   held_sec >= max_hold_sec    (position held too long)
  check_daily:  daily_pnl <= -max_daily_loss_pct * daily_start_equity

Pre-condition:
  All checks assume slot.has_position == True (caller guarantees this).

Author:    nt-base system
Version:   1.1.0
"""
from __future__ import annotations
"""Risk checker -- pure functions for trail/stop/take/hold/daily checks."""
from dataclasses import dataclass
from base.slot import StrategySlot

# Binance USDT futures taker fee rate
FEE_RATE: float = 0.0004


def _fee_adj_pnl_pct(slot: StrategySlot, current_price: float) -> float:
    """Return fee-adjusted PnL% accounting for entry + exit taker fees.

    For LONG: buy at entry*(1+fee), sell at current*(1-fee).
    For SHORT: sell at entry*(1-fee), buy back at current*(1+fee).
    """
    if slot.entry_side == "LONG":
        entry_cost = slot.entry_price * (1.0 + FEE_RATE)
        exit_proceeds = current_price * (1.0 - FEE_RATE)
        return (exit_proceeds - entry_cost) / entry_cost
    else:
        entry_proceeds = slot.entry_price * (1.0 - FEE_RATE)
        exit_cost = current_price * (1.0 + FEE_RATE)
        return (entry_proceeds - exit_cost) / entry_proceeds


@dataclass
class RiskAction:
    kind: str = ""
    reason: str = ""

    @property
    def should_exit(self) -> bool:
        return self.kind != "none"


# -- Trailing Stop (tick-level) --

def check_trail(slot: StrategySlot, current_price: float) -> RiskAction:
    """Trailing stop based on highest/lowest since entry.

    Uses ATR if available, otherwise falls back to 0.15% of price.
    The trail distance is configurable via slot.stop_pct as a multiplier
    on ATR (default trail distance = stop_pct * price, same as hard stop).
    """
    if not slot.has_position:
        return RiskAction("none")

    # Use slot.current_atr if set, otherwise fallback to 0.15% of entry price
    atr = slot.current_atr if slot.current_atr > 0 else slot.entry_price * 0.0015
    trail_distance = slot.stop_pct * slot.entry_price  # use stop_pct as trail distance

    if slot.entry_side == "LONG":
        if slot.highest_since_entry <= 0:
            return RiskAction("none")
        stop_price = slot.highest_since_entry - trail_distance
        if current_price <= stop_price:
            return RiskAction(
                "trail_stop",
                f"trail LONG {current_price:.4f} <= {stop_price:.4f} (high={slot.highest_since_entry:.4f})"
            )
    else:  # SHORT
        if slot.lowest_since_entry >= float("inf") - 1:
            return RiskAction("none")
        stop_price = slot.lowest_since_entry + trail_distance
        if current_price >= stop_price:
            return RiskAction(
                "trail_stop",
                f"trail SHORT {current_price:.4f} >= {stop_price:.4f} (low={slot.lowest_since_entry:.4f})"
            )
    return RiskAction("none")


# -- Fixed Stop Loss --

def check_stop(slot: StrategySlot, current_price: float) -> RiskAction:
    if not slot.has_position:
        return RiskAction("none")
    pnl_pct = _fee_adj_pnl_pct(slot, current_price)
    if pnl_pct <= -slot.stop_pct:
        return RiskAction("stop_loss", f"stop {pnl_pct:.4f}")
    return RiskAction("none")


# -- Take Profit --

def check_take(slot: StrategySlot, current_price: float) -> RiskAction:
    if not slot.has_position:
        return RiskAction("none")
    pnl_pct = _fee_adj_pnl_pct(slot, current_price)
    if pnl_pct >= slot.take_pct:
        return RiskAction("take_profit", f"take {pnl_pct:.4f}")
    return RiskAction("none")


# -- Max Hold Time --

def check_hold(slot: StrategySlot, current_price: float = 0.0) -> RiskAction:
    # current_price is unused but required for dynamic signature matching in risk loops
    if not slot.has_position:
        return RiskAction("none")
    if slot.held_sec >= slot.max_hold_sec:
        return RiskAction("max_hold", f"held {slot.held_sec:.0f}s")
    return RiskAction("none")


# -- Daily Loss Circuit Breaker --

def check_daily(slot: StrategySlot) -> RiskAction:
    if slot.daily_start_equity <= 0:
        return RiskAction("none")
    daily_ret = slot.daily_pnl / slot.daily_start_equity
    if daily_ret < -slot.max_daily_loss_pct:
        return RiskAction("daily_limit", f"daily loss {daily_ret:.4f}")
    return RiskAction("none")


# -- Bulk check --

def check_all(slot: StrategySlot, current_price: float) -> list[RiskAction]:
    checks = [
        check_trail(slot, current_price),
        check_stop(slot, current_price),
        check_take(slot, current_price),
        check_hold(slot),
        check_daily(slot),
    ]
    return [c for c in checks if c.should_exit]
