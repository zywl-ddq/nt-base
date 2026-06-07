"""
SignalComposer v2 -- hierarchical factor gating with trend-strength modulation.

New in v2:
  - Confidence channel: trend_regime now provides (direction, confidence)
  - Dynamic weights: cvd/residual/breakout weights scale with confidence
  - Single factor amplification preserved from v1.1

Architecture:
  trend_regime     -> GATE direction + confidence
  cvd_divergence   -> SIGNAL (weight: anti-cyclic with confidence)
  residual_momentum-> SIGNAL (weight: pro-cyclic with confidence)
  channel_breakout -> SIGNAL (weight: pro-cyclic with confidence)
"""
import logging
from collections import deque

logger = logging.getLogger("nt_base")  # use root logger with handlers


class SignalComposer:
    def __init__(self, gate_factor: str = "",
                 signal_factors: list[tuple[str, int, float]] | None = None,
                 adaptive: dict | None = None):
        self.gate_factor = gate_factor
        self.signal_factors = signal_factors or []

        # Adaptive modulation coefficients
        self.adaptive = adaptive or {}
        self._cvd_atten = self.adaptive.get("cvd_attenuation", 0.7)
        self._res_amp = self.adaptive.get("residual_amplification", 1.5)
        self._bk_amp = self.adaptive.get("breakout_amplification", 1.0)
        self._thresh_sens = self.adaptive.get("threshold_sensitivity", 0.5)

        # Rolling buffers for rank normalization
        self._buffers: dict[str, deque[float]] = {
            fname: deque(maxlen=30) for fname, _, _ in self.signal_factors
        }

        # Gate state
        self._gate_buffer: deque[float] = deque(maxlen=10)
        self._gate_value: float = 0.0
        self._confidence: float = 0.0
        self._conf_buffer: deque[float] = deque(maxlen=5)

        # EMA state
        self._ema_value: float | None = None
        self._ema_alpha: float = 1.0  # fast response (~3 bars for 60%)

    # ---- Properties ----

    @property
    def active_names(self) -> list[str]:
        names = []
        if self.gate_factor:
            names.append(self.gate_factor)
        names.extend(f[0] for f in self.signal_factors)
        return names

    @property
    def confidence(self) -> float:
        if len(self._conf_buffer) < 3:
            return 0.0
        return sum(self._conf_buffer) / len(self._conf_buffer)

    # ---- Gate ----

    def update_gate(self, value: float, confidence: float = 0.0):
        self._gate_buffer.append(value)
        self._gate_value = value
        self._conf_buffer.append(confidence)

    @property
    def regime(self) -> int:
        if len(self._gate_buffer) < 5:
            return 0
        recent = list(self._gate_buffer)[-5:]
        if sum(1 for v in recent if v > 0) >= 2:
            return 1
        if sum(1 for v in recent if v < 0) >= 2:
            return -1
        return 0

    def allowed_direction(self, signal: int) -> bool:
        r = self.regime
        if r == 0:
            return True
        if r == 1 and signal > 0:
            return True
        if r == -1 and signal < 0:
            return True
        return False

    # ---- Signal factors ----

    def update(self, factor_name: str, value: float) -> float:
        if factor_name == self.gate_factor:
            self.update_gate(value)
            return value
        # Special: trend_confidence comes from the same factor computation
        if factor_name == "trend_confidence":
            self._conf_buffer.append(value)
            return value

        buf = self._buffers.get(factor_name)
        if buf is None:
            return 0.5
        # Ternary factors: 0 is a valid signal, always append
        if factor_name in ("trend_regime", "channel_breakout"):
            buf.append(value)
        elif abs(value) < 1e-12:
            return 0.5  # zero = no signal, neutral rank
        else:
            buf.append(value)
        if len(buf) < 5:
            return 0.5
        return sum(1 for v in buf if v <= value) / len(buf)

    def dynamic_weight(self, base_weight: float, factor_name: str) -> float:
        """Compute confidence-modulated weight for a signal factor."""
        conf = self.confidence

        if "cvd" in factor_name.lower():
            # Anti-cyclic: weight decreases with confidence
            return base_weight * (1.0 - conf * self._cvd_atten)
        elif "residual" in factor_name.lower():
            # Pro-cyclic: weight increases with confidence
            return base_weight * (1.0 + conf * self._res_amp)
        elif "breakout" in factor_name.lower() or "channel" in factor_name.lower():
            # Pro-cyclic: weight increases with confidence
            return base_weight * (1.0 + conf * self._bk_amp)
        else:
            return base_weight

    def composite(self) -> float:
        if not self.signal_factors:
            return 0.0

        total = 0.0
        total_weight = 0.0
        n_active = 0

        for fname, direction, base_weight in self.signal_factors:
            buf = self._buffers.get(fname)
            if not buf or len(buf) < 5:
                continue
            n_active += 1
            adj_weight = self.dynamic_weight(base_weight, fname)

            # trend_regime is ternary (-1/0/+1): use raw value, not rank
            if fname in ("trend_regime", "channel_breakout"):
                signal_val = buf[-1] * 0.5  # ternary factor: use raw value, not rank
            elif abs(buf[-1]) < 1e-9:
                signal_val = 0.0  # neutral
            else:
                rank = sum(1 for v in buf if v <= buf[-1]) / len(buf)
                signal_val = rank - 0.5

            total += signal_val * direction * adj_weight
            total_weight += abs(adj_weight)

        if total_weight == 0 or n_active == 0:
            return 0.0

        
        
        if n_active == 1:
            val = total / total_weight
            result = max(-0.5, min(0.5, val))
        else:
            result = total / total_weight

        # Debug: log each factor contribution
        parts = []
        for fname, direction, base_weight in self.signal_factors:
            buf = self._buffers.get(fname)
            if buf and len(buf) >= 5:
                adj_w = self.dynamic_weight(base_weight, fname)
                # Use same signal_val logic as composite() calculation
                if fname in ("trend_regime", "channel_breakout"):
                    signal_val = buf[-1] * 0.5
                    show_rank = 0.5 + signal_val  # reverse-map for display
                elif abs(buf[-1]) < 1e-9:
                    signal_val = 0.0
                    show_rank = 0.5
                else:
                    rank_val = sum(1 for v in buf if v <= buf[-1]) / len(buf)
                    signal_val = rank_val - 0.5
                    show_rank = rank_val
                contrib = signal_val * direction * adj_w
                parts.append(f"{fname}={buf[-1]:.3f} rank={show_rank:.2f} dir={direction} w={adj_w:.2f} c={contrib:.3f}")
        logger.info(f"COMPOSITE debug: {', '.join(parts)} => composite={result:.3f}")

        return result

    def dynamic_threshold(self, base_threshold: float) -> float:
        conf = self.confidence
        raw = base_threshold * (1.0 + self._thresh_sens * (1.0 - conf)); return min(raw, 0.45)

    def direction(self, threshold: float = 0.15) -> int:
        raw = self.composite()

        if self._ema_value is None:
            self._ema_value = raw
        else:
            self._ema_value = (self._ema_alpha * raw
                               + (1.0 - self._ema_alpha) * self._ema_value)

        ema = self._ema_value
        adj_threshold = self.dynamic_threshold(threshold)

        if ema > adj_threshold:
            raw_dir = 1
        elif ema < -adj_threshold:
            raw_dir = -1
        else:
            return 0

        return raw_dir

    # ---- Diagnostics ----

    def get_factor_rank(self, factor_name: str) -> float:
        buf = self._buffers.get(factor_name)
        if not buf or len(buf) < 5:
            return 0.5
        return sum(1 for v in buf if v <= buf[-1]) / len(buf)

    def get_diagnostics(self) -> dict:
        return {
            "regime": self.regime,
            "confidence": round(self.confidence, 4),
            "composite": round(self.composite(), 4),
            "ema": round(self._ema_value, 4) if self._ema_value else 0,
            "direction": self.direction(0.15),
            "factors": {
                fname: {
                    "rank": round(self.get_factor_rank(fname), 4),
                    "weight": round(self.dynamic_weight(w, fname), 4),
                }
                for fname, _, w in self.signal_factors
            },
        }


def build_signal_composer(
    gate_factor: str = "trend_regime",
    factor_1: str = "cvd_divergence", direction_1: int = -1, weight_1: float = 1.0,
    factor_2: str = "residual_momentum", direction_2: int = -1, weight_2: float = 0.5,
    factor_3: str = "channel_breakout", direction_3: int = -1, weight_3: float = 1.0,
    factor_4: str = "", direction_4: int = -1, weight_4: float = 0.0,
    factor_5: str = "", direction_5: int = -1, weight_5: float = 0.0,
    adaptive: dict | None = None,
) -> SignalComposer:
    signal_factors: list[tuple[str, int, float]] = []
    for i in range(1, 6):
        name = locals()[f"factor_{i}"]
        direction = locals()[f"direction_{i}"]
        weight = locals()[f"weight_{i}"]
        if name and name.strip() and name != gate_factor and weight > 0:
            signal_factors.append((name.strip(), direction, weight))

    if gate_factor and gate_factor.strip():
        signal_factors.append((gate_factor.strip(), 1, 1.0))
    return SignalComposer(
        gate_factor="",
        signal_factors=signal_factors,
        adaptive=adaptive or {},
    )
