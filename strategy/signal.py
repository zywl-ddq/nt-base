"""
Module:    strategy/signal
Purpose:   Multi-factor signal composer with hierarchical trend gating.
           Rank-normalizes factor values, computes weighted composite,
           applies EMA smoothing and threshold gating.

Architecture:
  trend_regime     -> GATE    (determines allowed direction: -1/0/+1)
  cvd_divergence   -> SIGNAL  (order-flow imbalance)
  residual_momentum-> SIGNAL  (SOL alpha vs BTC)
  future factors   -> SIGNAL  (extensible)

  Key difference from v1: trend_regime does NOT vote 鈥?it gates.
  Ranging markets (regime=0): both directions pass the gate.

Class: SignalComposer
  gate_factor: str            name of the trend/regime factor used as gate
  signal_factors: list        list of (name, direction, weight) tuples

  update(factor_name, value) -> float
      Push a factor value. Returns percentile rank for signal factors,
      raw value for gate factor. Buffer-based (rolling window of 30).

  composite() -> float
      Weighted rank-normalized composite from signal factors.
      Range [-0.5, 0.5]. Negative = SHORT bias, Positive = LONG bias.
      Single factor: weight acts as amplifier (capped).
      Multi-factor: weighted average.

  direction(threshold=0.15) -> int
      EMA-smoothed composite, gated by trend regime.
      Returns -1 (SHORT), 0 (HOLD), +1 (LONG).

  regime -> int
      Current trend regime via majority vote of last 5 gate values.
      -1 = downtrend, 0 = ranging, +1 = uptrend.

Factory: build_signal_composer(gate_factor, factor_1..5, direction_1..5, weight_1..5)
      Convenience constructor mapping slot-style params to SignalComposer.

Design Decision:
  Rank normalization: each factor's value is converted to percentile rank
  within its rolling buffer (30 samples). This handles factors with different
  scales and distributions. The weight parameter controls relative influence
  in multi-factor mode and absolute amplification in single-factor mode.

Edge Cases:
  - Buffer < 5: returns default rank 0.5 (neutral)
  - Single factor: weight amplifies/dampens signal (v1.1 fix)
  - Empty gate: all directions allowed (regime=0)
  - EMA alpha=0.08: requires ~3-4 consistent bars to cross typical threshold

Author:    nt-base / trading-v2
Version:   1.1.0 (single-factor weight amplification)
"""
from __future__ import annotations
"""SignalComposer v2 — hierarchical factor gating.

Architecture:
  trend_regime     → GATE: determines allowed direction
                     -1 = downtrend (only SHORT allowed)
                      0 = ranging    (both LONG and SHORT allowed)
                     +1 = uptrend   (only LONG allowed)
  cvd_divergence   → SIGNAL: divergence-based entry timing
                     direction=-1 means: high Div_Factor→SHORT, low Div_Factor→LONG
  residual_momentum→ CONFIRM: SOL alpha vs BTC (optional)
  (future factors) → additional signals within the gate

Key difference from v1: trend_regime does NOT vote — it gates.
Ranging markets (regime=0): both directions pass the gate.
"""

from collections import deque


class SignalComposer:
    """Multi-factor signal composer with trend gate.

    trend_regime: gate factor (raw value used, not rank-normalized)
    other factors: signal factors (rank-normalized, weighted composite)
    """

    def __init__(self, gate_factor: str = "",
                 signal_factors: list[tuple[str, int, float]] | None = None):
        """
        Args:
            gate_factor: name of the trend/regime factor used as gate
            signal_factors: list of (name, direction, weight) for signal factors
        """
        self.gate_factor = gate_factor
        self.signal_factors = signal_factors or []

        # Rolling buffers for rank normalization (signal factors only)
        self._buffers: dict[str, deque[float]] = {
            fname: deque(maxlen=30) for fname, _, _ in self.signal_factors
        }

        # Raw value tracking for gate factor
        self._gate_buffer: deque[float] = deque(maxlen=10)
        self._gate_value: float = 0.0

        # EMA state for composite smoothing
        self._ema_value: float | None = None
        self._ema_alpha: float = 0.08

    # ── Properties ─────────────────────────────────────────────

    @property
    def active_names(self) -> list[str]:
        names = []
        if self.gate_factor:
            names.append(self.gate_factor)
        names.extend(f[0] for f in self.signal_factors)
        return names

    @property
    def active_count(self) -> int:
        return len(self.active_names)

    # ── Gate ───────────────────────────────────────────────────

    def update_gate(self, value: float):
        """Update the trend gate with a raw factor value (-1/0/+1)."""
        self._gate_buffer.append(value)
        self._gate_value = value

    @property
    def regime(self) -> int:
        """Current trend regime: -1=downtrend, 0=ranging, +1=uptrend.

        Uses majority vote of last 3 values to avoid flickering.
        """
        if len(self._gate_buffer) < 5:
            return 0
        recent = list(self._gate_buffer)[-5:]
        if sum(1 for v in recent if v > 0) >= 2:
            return 1
        if sum(1 for v in recent if v < 0) >= 2:
            return -1
        return 0

    def allowed_direction(self, signal: int) -> bool:
        """Check if a signal direction passes the trend gate."""
        r = self.regime
        if r == 0:
            return True           # ranging: allow both
        if r == 1 and signal > 0:
            return True           # uptrend: allow LONG
        if r == -1 and signal < 0:
            return True           # downtrend: allow SHORT
        return False              # blocked by gate

    # ── Signal factors ─────────────────────────────────────────

    def update(self, factor_name: str, value: float) -> float:
        """Push a factor value. Returns percentile rank for signal factors,
        or the raw value for the gate factor."""
        if factor_name == self.gate_factor:
            self.update_gate(value)
            return value

        buf = self._buffers.get(factor_name)
        if buf is None:
            return 0.5
        buf.append(value)
        if len(buf) < 5:
            return 0.5
        return sum(1 for v in buf if v <= value) / len(buf)

    def composite(self) -> float:
        """Weighted rank-normalized signal from signal factors (excludes gate).

        Returns value in [-0.5, 0.5].
        Negative -> SHORT bias, Positive -> LONG bias.

        Single factor: weight acts as amplifier/dampener.
        Multi-factor: weighted average across factors.
        """
        if not self.signal_factors:
            return 0.0
        total = 0.0
        total_weight = 0.0
        n_active = 0
        for fname, direction, weight in self.signal_factors:
            buf = self._buffers.get(fname)
            if not buf or len(buf) < 5:
                continue
            n_active += 1
            rank = sum(1 for v in buf if v <= buf[-1]) / len(buf)
            total += (rank - 0.5) * direction * weight
            total_weight += weight
        if total_weight == 0 or n_active == 0:
            return 0.0
        if n_active == 1:
            val = total / total_weight
            amplified = val * total_weight / 1.0
            return max(-0.5, min(0.5, amplified))
        return total / total_weight

    # ── Final signal ───────────────────────────────────────────

    def direction(self, threshold: float = 0.15) -> int:
        """EMA-smoothed composite, gated by trend regime.

        Returns: -1 (SHORT), 0 (HOLD), +1 (LONG)
        """
        raw = self.composite()

        # EMA smoothing
        if self._ema_value is None:
            self._ema_value = raw
        else:
            self._ema_value = (self._ema_alpha * raw
                               + (1 - self._ema_alpha) * self._ema_value)

        ema = self._ema_value

        # Determine raw signal direction
        if ema > threshold:
            raw_dir = 1
        elif ema < -threshold:
            raw_dir = -1
        else:
            return 0  # signal too weak

        # Apply trend gate
        if not self.allowed_direction(raw_dir):
            return 0  # blocked by gate

        return raw_dir

    # ── Diagnostics ────────────────────────────────────────────

    def get_factor_rank(self, factor_name: str) -> float:
        """Get current percentile rank for a factor."""
        buf = self._buffers.get(factor_name)
        if not buf or len(buf) < 5:
            return 0.5
        return sum(1 for v in buf if v <= buf[-1]) / len(buf)

    def get_diagnostics(self) -> dict:
        return {
            "regime": self.regime,
            "gate_value": round(self._gate_value, 4),
            "composite": round(self.composite(), 4),
            "ema": round(self._ema_value, 4) if self._ema_value else 0,
            "direction": self.direction(0.15),
            "factors": {
                fname: round(self.get_factor_rank(fname), 4)
                for fname, _, _ in self.signal_factors
            },
        }


def build_signal_composer(
    gate_factor: str = "trend_regime",
    factor_1: str = "cvd_divergence", direction_1: int = -1, weight_1: float = 1.0,
    factor_2: str = "residual_momentum", direction_2: int = -1, weight_2: float = 0.5,
    factor_3: str = "", direction_3: int = -1, weight_3: float = 0.0,
    factor_4: str = "", direction_4: int = -1, weight_4: float = 0.0,
    factor_5: str = "", direction_5: int = -1, weight_5: float = 0.0,
) -> SignalComposer:
    """Build a SignalComposer with trend gate + signal factors."""
    signal_factors: list[tuple[str, int, float]] = []
    for i in range(1, 6):
        name = locals()[f"factor_{i}"]
        direction = locals()[f"direction_{i}"]
        weight = locals()[f"weight_{i}"]
        if name and name.strip() and name != gate_factor and weight > 0:
            signal_factors.append((name.strip(), direction, weight))

    return SignalComposer(gate_factor=gate_factor, signal_factors=signal_factors)
