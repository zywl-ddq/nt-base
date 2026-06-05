"""ExitManager -- 4-layer dynamic exit system.

Extracted from IntradayFactor._evaluate_dynamic_exits() (trading-infra).
Pure logic, zero NautilusTrader dependencies.

Layers:
  L1 — Smart Exit: BTC 1m shock + CVD direction reversal
  L2 — Time Decay: held too long without profit
  L3 — Breakeven Ladder: profit > threshold -> move stop to entry + fees
  L4 — ATR Trailing: profit > trigger -> trail stop at HH/LL
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass
class ExitState:
    """Mutable state tracking a single position's exit progression."""
    entry_price: float = 0.0
    bars_held: int = 0
    highest_price: float = 0.0
    lowest_price: float = float("inf")
    breakeven_activated: bool = False
    is_long: bool = True

    def reset(self):
        self.entry_price = 0.0
        self.bars_held = 0
        self.highest_price = 0.0
        self.lowest_price = float("inf")
        self.breakeven_activated = False


@dataclass
class ExitAction:
    """Signal to the strategy that a position should be closed."""
    reason: str
    urgency: str = "normal"  # "immediate" (L1) or "normal" (L2/L3/L4)


@dataclass(frozen=True)
class ExitConfig:
    """All exit parameters — fully learnable by RD-Agent."""
    # ATR
    atr_period: int = 16

    # L1: Smart Exit
    btc_shock_long: float = 0.0085   # BTC 1m drop > 0.85% -> exit long
    btc_shock_short: float = 0.0075  # BTC 1m rise > 0.75% -> exit short

    # L2: Time Decay
    time_limit_long: int = 40    # bars before time decay kicks in (long)
    time_limit_short: int = 18   # shorter for shorts
    max_hold_minutes: int = 40   # absolute max hold
    time_decay_pnl_pct: float = 0.005  # min pnl to avoid decay (0.5%)

    # L3: Breakeven Ladder
    breakeven_atr_mult: float = 1.4   # profit > 1.4x ATR -> activate
    breakeven_fee_pct: float = 0.0015  # fee cover (0.15%)

    # L4: Volatility Trailing
    trail_trigger_atr: float = 2.0    # profit > 2.0x ATR -> activate trailing
    trail_stop_atr: float = 1.0       # trail distance from HH/LL


class ExitManager:
    """Evaluate whether a position should be closed based on 4-layer logic."""

    def __init__(self, config: ExitConfig):
        self.cfg = config

    def compute_atr(self, highs: list[float], lows: list[float]) -> float:
        """Compute ATR from 1m high/low bars."""
        n = min(len(highs), len(lows), self.cfg.atr_period)
        if n < 3:
            return 0.0
        h = np.array(highs[-n:], dtype=float)
        l = np.array(lows[-n:], dtype=float)
        tr = h - l
        return float(np.mean(tr))

    def evaluate(
        self,
        current_price: float,
        current_atr: float,
        btc_ret_1m: float,
        recent_deltas: list[float],
        state: ExitState,
        regime: int = 0,  # -1=downtrend, 0=ranging, +1=uptrend
    ) -> ExitAction | None:
        """Evaluate all 4 layers. Returns ExitAction to close, or None to hold."""

        # When trending, suppress CVD reversal (microstructure noise)
        skip_cvd = (regime == 0)

        # Update state tracking
        state.bars_held += 1
        if state.is_long:
            state.highest_price = max(state.highest_price, current_price)
        else:
            state.lowest_price = min(state.lowest_price, current_price)

        atr = current_atr if current_atr > 0 else current_price * 0.0015

        if state.is_long:
            return self._evaluate_long(current_price, atr, btc_ret_1m,
                                       recent_deltas, state, skip_cvd)
        else:
            return self._evaluate_short(current_price, atr, btc_ret_1m,
                                        recent_deltas, state, skip_cvd)

    # ── LONG exits ─────────────────────────────────────────────

    def _evaluate_long(
        self, px: float, atr: float, btc_ret: float,
        deltas: list[float], s: ExitState, skip_cvd: bool = False,
    ) -> ExitAction | None:
        pnl_pct = (px - s.entry_price) / s.entry_price

        # L1: BTC shock down -> immediate exit
        if btc_ret < -self.cfg.btc_shock_long:
            return ExitAction(f"BTC shock down ({btc_ret:+.4f})", "immediate")

        # L1: CVD reversal — only in ranging, and after 3+ bars held
        if not skip_cvd and s.bars_held >= 3 and len(deltas) >= 5:
            recent = deltas[-5:]
            if sum(1 for d in recent if d < 0) >= 4:
                return ExitAction("CVD reversal (long)", "immediate")

        # L2: Time Decay
        if s.bars_held >= self.cfg.time_limit_long and pnl_pct < self.cfg.time_decay_pnl_pct:
            return ExitAction(f"Time Decay ({s.bars_held}b pnl={pnl_pct:+.4f})")

        # L2: Max Hold
        if s.bars_held >= self.cfg.max_hold_minutes:
            return ExitAction(f"MaxHold ({s.bars_held}b)")

        # L3: Breakeven Ladder activation
        breakeven_target = s.entry_price + (self.cfg.breakeven_atr_mult * atr)
        if not s.breakeven_activated and px >= breakeven_target:
            s.breakeven_activated = True

        # L4: Dynamic stop
        fee_cover = s.entry_price * (1.0 + self.cfg.breakeven_fee_pct)
        if s.breakeven_activated:
            trail_target = s.highest_price - (self.cfg.trail_stop_atr * atr)
            stop_price = max(fee_cover, trail_target)
            if px <= stop_price:
                reason = "Trailing" if s.highest_price > breakeven_target else "Breakeven SL"
                return ExitAction(f"{reason} at {stop_price:.4f}")
        else:
            stop_price = s.entry_price - (self.cfg.breakeven_atr_mult * atr)
            if px <= stop_price:
                return ExitAction(f"Hard SL at {stop_price:.4f}")

        return None

    # ── SHORT exits ────────────────────────────────────────────

    def _evaluate_short(
        self, px: float, atr: float, btc_ret: float,
        deltas: list[float], s: ExitState, skip_cvd: bool = False,
    ) -> ExitAction | None:
        pnl_pct = (s.entry_price - px) / s.entry_price

        # L1: BTC shock up -> squeeze exit
        if btc_ret > self.cfg.btc_shock_short:
            return ExitAction(f"BTC shock up ({btc_ret:+.4f})", "immediate")

        # L1: CVD reversal — only in ranging, and after 3+ bars held
        if not skip_cvd and s.bars_held >= 3 and len(deltas) >= 5:
            recent = deltas[-5:]
            if sum(1 for d in recent if d > 0) >= 4:
                return ExitAction("CVD reversal (short)", "immediate")

        # L2: Time Decay (shorter leash)
        if s.bars_held >= self.cfg.time_limit_short and pnl_pct < self.cfg.time_decay_pnl_pct:
            return ExitAction(f"Time Decay ({s.bars_held}b pnl={pnl_pct:+.4f})")

        # L2: Max Hold
        if s.bars_held >= self.cfg.max_hold_minutes:
            return ExitAction(f"MaxHold ({s.bars_held}b)")

        # L3: Breakeven Ladder activation (for shorts: price moves DOWN)
        breakeven_target = s.entry_price - (self.cfg.breakeven_atr_mult * atr)
        if not s.breakeven_activated and px <= breakeven_target:
            s.breakeven_activated = True

        # L4: Dynamic stop
        fee_cover = s.entry_price * (1.0 - self.cfg.breakeven_fee_pct)
        if s.breakeven_activated:
            trail_target = s.lowest_price + (self.cfg.trail_stop_atr * atr)
            stop_price = min(fee_cover, trail_target)
            if px >= stop_price:
                reason = "Trailing" if s.lowest_price < breakeven_target else "Breakeven SL"
                return ExitAction(f"{reason} at {stop_price:.4f}")
        else:
            stop_price = s.entry_price + (self.cfg.breakeven_atr_mult * atr)
            if px >= stop_price:
                return ExitAction(f"Hard SL at {stop_price:.4f}")

        return None
