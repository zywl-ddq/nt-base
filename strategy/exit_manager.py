"""
ExitManager v2 -- 4-layer dynamic exit with trend-strength scaling.

New in v2:
  - evaluate() accepts confidence and adaptive params
  - Stop/take/max_hold/trail parameters scale with confidence:
    Weak trend: tight stops, short holds (defensive)
    Strong trend: wide stops, long holds (let profits run)
"""
from dataclasses import dataclass
import numpy as np


@dataclass
class ExitState:
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
    reason: str
    urgency: str = "normal"


@dataclass(frozen=True)
class ExitConfig:
    atr_period: int = 16
    btc_shock_long: float = 0.0085
    btc_shock_short: float = 0.0075
    time_limit_long: int = 40
    time_limit_short: int = 18
    max_hold_minutes: int = 40
    time_decay_pnl_pct: float = 0.005
    breakeven_atr_mult: float = 1.4
    breakeven_fee_pct: float = 0.0015
    trail_trigger_atr: float = 2.0
    trail_stop_atr: float = 1.0


class ExitManager:
    def __init__(self, config: ExitConfig):
        self.cfg = config

    def compute_atr(self, highs: list[float], lows: list[float]) -> float:
        n = min(len(highs), len(lows), self.cfg.atr_period)
        if n < 3:
            return 0.0
        h = np.array(highs[-n:], dtype=float)
        low_vals = np.array(lows[-n:], dtype=float)
        return float(np.mean(h - low_vals))

    def _scale_param(self, base: float, scaling: float, conf: float, floor_ratio: float = 0.3) -> float:
        """Scale a parameter from base (at conf=1.0) down to floor (at conf=0.0)."""
        return base * (floor_ratio + (1.0 - floor_ratio) * conf * scaling)

    def evaluate(self, current_price: float, current_atr: float,
                 btc_ret_1m: float, recent_deltas: list[float],
                 state: ExitState, regime: int = 0,
                 confidence: float = 0.0, adaptive: dict | None = None) -> ExitAction | None:
        # skip CVD reversal in trending markets (fixed)
        skip_cvd = (regime != 0)

        state.bars_held += 1
        if state.is_long:
            state.highest_price = max(state.highest_price, current_price)
        else:
            state.lowest_price = min(state.lowest_price, current_price)

        atr = current_atr if current_atr > 0 else current_price * 0.0015

        # Adaptive scaling parameters
        adapt = adaptive or {}
        stop_floor = adapt.get("stop_tighten_weak", 0.5)
        take_floor = adapt.get("take_tighten_weak", 0.6)
        hold_floor = adapt.get("hold_shorten_weak", 0.5)

        if state.is_long:
            return self._evaluate_long(px=current_price, atr=atr, btc_ret=btc_ret_1m,
                                       deltas=recent_deltas, s=state, skip_cvd=skip_cvd,
                                       confidence=confidence,
                                       stop_floor=stop_floor, take_floor=take_floor,
                                       hold_floor=hold_floor)
        else:
            return self._evaluate_short(px=current_price, atr=atr, btc_ret=btc_ret_1m,
                                        deltas=recent_deltas, s=state, skip_cvd=skip_cvd,
                                        confidence=confidence,
                                        stop_floor=stop_floor, take_floor=take_floor,
                                        hold_floor=hold_floor)

    def _evaluate_long(self, px: float, atr: float, btc_ret: float,
                       deltas: list[float], s: ExitState, skip_cvd: bool = False,
                       confidence: float = 0.0,
                       stop_floor: float = 0.5, take_floor: float = 0.6,
                       hold_floor: float = 0.5) -> ExitAction | None:
        pnl_pct = (px - s.entry_price) / s.entry_price

        # L1: BTC shock
        if btc_ret < -self.cfg.btc_shock_long:
            return ExitAction(f"BTC shock down ({btc_ret:+.4f})", "immediate")

        if not skip_cvd and s.bars_held >= 3 and len(deltas) >= 5:
            recent = deltas[-5:]
            if sum(1 for d in recent if d < 0) >= 4:
                return ExitAction("CVD reversal (long)", "immediate")

        # L2: Time Decay -- scaled by confidence
        adj_time_limit = int(self.cfg.time_limit_long * (hold_floor + (1 - hold_floor) * confidence))
        if s.bars_held >= adj_time_limit and pnl_pct < self.cfg.time_decay_pnl_pct:
            return ExitAction(f"Time Decay ({s.bars_held}b pnl={pnl_pct:+.4f})")

        adj_max_hold = int(self.cfg.max_hold_minutes * (hold_floor + (1 - hold_floor) * confidence))
        if s.bars_held >= adj_max_hold:
            return ExitAction(f"MaxHold ({s.bars_held}b)")

        # L3: Breakeven Ladder
        breakeven_target = s.entry_price + (self.cfg.breakeven_atr_mult * atr)
        if not s.breakeven_activated and px >= breakeven_target:
            s.breakeven_activated = True

        # L4: Dynamic stop -- stop distance scales with confidence
        fee_cover = s.entry_price * (1.0 + self.cfg.breakeven_fee_pct)
        if s.breakeven_activated:
            trail_target = s.highest_price - (self.cfg.trail_stop_atr * atr)
            stop_price = max(fee_cover, trail_target)
            if px <= stop_price:
                reason = "Trailing" if s.highest_price > breakeven_target else "Breakeven SL"
                return ExitAction(f"{reason} at {stop_price:.4f}")
        else:
            adj_stop_mult = self.cfg.breakeven_atr_mult * (stop_floor + (1 - stop_floor) * confidence)
            stop_price = s.entry_price - (adj_stop_mult * atr)
            if px <= stop_price:
                return ExitAction(f"Hard SL at {stop_price:.4f}")

        return None

    def _evaluate_short(self, px: float, atr: float, btc_ret: float,
                        deltas: list[float], s: ExitState, skip_cvd: bool = False,
                        confidence: float = 0.0,
                        stop_floor: float = 0.5, take_floor: float = 0.6,
                        hold_floor: float = 0.5) -> ExitAction | None:
        pnl_pct = (s.entry_price - px) / s.entry_price

        if btc_ret > self.cfg.btc_shock_short:
            return ExitAction(f"BTC shock up ({btc_ret:+.4f})", "immediate")

        if not skip_cvd and s.bars_held >= 3 and len(deltas) >= 5:
            recent = deltas[-5:]
            if sum(1 for d in recent if d > 0) >= 4:
                return ExitAction("CVD reversal (short)", "immediate")

        adj_time_limit = int(self.cfg.time_limit_short * (hold_floor + (1 - hold_floor) * confidence))
        if s.bars_held >= adj_time_limit and pnl_pct < self.cfg.time_decay_pnl_pct:
            return ExitAction(f"Time Decay ({s.bars_held}b pnl={pnl_pct:+.4f})")

        adj_max_hold = int(self.cfg.max_hold_minutes * (hold_floor + (1 - hold_floor) * confidence))
        if s.bars_held >= adj_max_hold:
            return ExitAction(f"MaxHold ({s.bars_held}b)")

        breakeven_target = s.entry_price - (self.cfg.breakeven_atr_mult * atr)
        if not s.breakeven_activated and px <= breakeven_target:
            s.breakeven_activated = True

        fee_cover = s.entry_price * (1.0 - self.cfg.breakeven_fee_pct)
        if s.breakeven_activated:
            trail_target = s.lowest_price + (self.cfg.trail_stop_atr * atr)
            stop_price = min(fee_cover, trail_target)
            if px >= stop_price:
                reason = "Trailing" if s.lowest_price < breakeven_target else "Breakeven SL"
                return ExitAction(f"{reason} at {stop_price:.4f}")
        else:
            adj_stop_mult = self.cfg.breakeven_atr_mult * (stop_floor + (1 - stop_floor) * confidence)
            stop_price = s.entry_price + (adj_stop_mult * atr)
            if px >= stop_price:
                return ExitAction(f"Hard SL at {stop_price:.4f}")

        return None
