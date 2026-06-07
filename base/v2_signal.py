"""
AlphaSignal v3 -- with Quad-Layer Tick-Level Exit System.

New in v3:
  - TickExitManager for L1-L3 (tick-level toxic flow, trailing, breakeven)
  - L4 Time Decay in on_bar
  - on_tick() calls TickExitManager, sets pending_exit flag
  - on_bar() checks pending_exit first, then normal logic
"""
from collections import deque

from base.signal_protocol import SignalStrategy, StrategySignal
from strategy.signal import build_signal_composer
from strategy.exit_manager import ExitManager, ExitConfig, ExitState
from strategy.tick_exit import TickExitManager, TickExitAction


class AlphaSignal(SignalStrategy):
    def __init__(self,
                 gate_factor: str = "trend_regime",
                 factor_1: str = "cvd_divergence", direction_1: int = -1, weight_1: float = 1.0,
                 factor_2: str = "residual_momentum", direction_2: int = 1, weight_2: float = 0.5,
                 factor_3: str = "channel_breakout", direction_3: int = -1, weight_3: float = 1.0,
                 signal_threshold: float = 0.28,
                 atr_period: int = 30,
                 btc_shock_long: float = 0.0085, btc_shock_short: float = 0.0075,
                 time_limit_long: int = 40, time_limit_short: int = 18,
                 max_hold_minutes: int = 40,
                 breakeven_atr_mult: float = 1.4,
                 trail_trigger_atr: float = 2.0, trail_stop_atr: float = 1.0,
                 adaptive: dict | None = None,
                 # Tick exit params
                 toxic_vol_threshold: float = 500.0,
                 toxic_ratio: float = 5.0,
                 tick_trail_atr_mult: float = 4.0,
                 tick_breakeven_atr_mult: float = 1.5,
                 time_decay_bars: int = 15,
                 time_decay_min_return: float = 0.0015,
                 ):
        self._name = "AlphaSignal_v3"

        self._adaptive = adaptive or {}

        self._signal = build_signal_composer(
            gate_factor=gate_factor,
            factor_1=factor_1, direction_1=direction_1, weight_1=weight_1,
            factor_2=factor_2, direction_2=direction_2, weight_2=weight_2,
            factor_3=factor_3, direction_3=direction_3, weight_3=weight_3,
            adaptive=self._adaptive,
        )

        # Bar-level exits (L1-L4, BTC shock, CVD reversal, signal flip)
        self._exits = ExitManager(ExitConfig(
            atr_period=atr_period,
            btc_shock_long=btc_shock_long, btc_shock_short=btc_shock_short,
            time_limit_long=time_limit_long, time_limit_short=time_limit_short,
            max_hold_minutes=max_hold_minutes,
            breakeven_atr_mult=breakeven_atr_mult,
            trail_trigger_atr=trail_trigger_atr, trail_stop_atr=trail_stop_atr,
        ))
        self._sig_threshold = signal_threshold

        # Tick-level exit manager (L1-L3: toxic flow, trailing, breakeven)
        self._tick_exits = TickExitManager(
            toxic_vol_threshold=toxic_vol_threshold,
            toxic_ratio=toxic_ratio,
            trail_atr_mult=tick_trail_atr_mult,
            breakeven_atr_mult=tick_breakeven_atr_mult,
        )
        self._time_decay_bars = time_decay_bars
        self._time_decay_min_return = time_decay_min_return

        self._pending_tick_exit: str | None = None

        # Data buffers
        self.sol_1m_closes: deque[float] = deque(maxlen=60)
        self.sol_1m_highs: deque[float] = deque(maxlen=30)
        self.sol_1m_lows: deque[float] = deque(maxlen=30)
        self.btc_1m_closes: deque[float] = deque(maxlen=5)
        self.sol_1m_deltas: deque[float] = deque(maxlen=60)

        # Position tracking
        self._exit_state = ExitState()
        self._in_position = False
        self._position_side = ""
        self._entry_price = 0.0
        self._bars_held = 0
        self._bar_count = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def factor_names(self) -> list[str]:
        return self._signal.active_names

    def set_factor_value(self, name: str, ts_ns: int, value: float) -> None:
        self._signal.update(name, value)

    # ---- Tick handler (Tick-Level Exits L1-L3) ----

    def on_tick(self, price: float, size: float, is_buyer: bool, ts_ns: int, symbol: str = "") -> None:
        if not self._in_position:
            return

        result = self._tick_exits.on_tick(price, size, is_buyer, ts_ns, symbol)
        if result is not None:
            self._pending_tick_exit = result.reason

    # ---- Bar handler ----

    def on_bar(self, close: float, high: float, low: float,
               delta_buy_vol: float, delta_sell_vol: float,
               btc_close: float, ts_ns: int) -> StrategySignal:
        self._bar_count += 1

        # Check pending tick exit first
        if self._pending_tick_exit:
            reason = self._pending_tick_exit
            self._pending_tick_exit = None
            self._in_position = False
            self._tick_exits.close_position()
            self._exit_state.reset()
            return StrategySignal(direction=0, reason=reason)

        self.sol_1m_closes.append(close)
        self.sol_1m_highs.append(high)
        self.sol_1m_lows.append(low)
        self.sol_1m_deltas.append(delta_buy_vol - delta_sell_vol)
        self.btc_1m_closes.append(btc_close)

        regime = self._signal.regime
        confidence = self._signal.confidence

        if self._in_position:
            self._bars_held += 1
            current_atr = self._exits.compute_atr(
                list(self.sol_1m_highs), list(self.sol_1m_lows))
            if current_atr == 0:
                current_atr = close * 0.0015

            # Update tick exit ATR
            self._tick_exits.update_atr(current_atr)

            # L4: Tick Time Decay
            tick_decay = self._tick_exits.check_time_decay(
                self._bars_held, close,
                time_limit_bars=self._time_decay_bars,
                min_return_pct=self._time_decay_min_return,
            )
            if tick_decay:
                self._in_position = False
                self._tick_exits.close_position()
                self._exit_state.reset()
                return StrategySignal(direction=0, reason=tick_decay.reason)

            # Bar-level ExitManager (BTC shock, CVD reversal, etc.)
            btc_ret = 0.0
            if len(self.btc_1m_closes) >= 2 and self.btc_1m_closes[-2] > 0:
                btc_ret = (self.btc_1m_closes[-1] - self.btc_1m_closes[-2]) / self.btc_1m_closes[-2]

            deltas = list(self.sol_1m_deltas)[-6:]

            action = self._exits.evaluate(
                close, current_atr, btc_ret, deltas, self._exit_state,
                regime=regime, confidence=confidence, adaptive=self._adaptive,
            )

            if action is not None:
                self._in_position = False
                self._tick_exits.close_position()
                self._exit_state.reset()
                return StrategySignal(direction=0, reason=action.reason)

            # Signal flip
            dir_signal = self._signal.direction(self._sig_threshold)
            signal_flip = (self._position_side == "LONG" and dir_signal < 0) or \
                          (self._position_side == "SHORT" and dir_signal > 0)
            if signal_flip:
                self._in_position = False
                self._tick_exits.close_position()
                self._exit_state.reset()
                return StrategySignal(direction=0, reason=f"signal flip to {dir_signal}")

            return StrategySignal(direction=0, reason="hold")

        # Not in position: evaluate entry
        dir_signal = self._signal.direction(self._sig_threshold)
        if dir_signal != 0:
            self._in_position = True
            self._position_side = "LONG" if dir_signal > 0 else "SHORT"
            self._entry_price = close
            self._bars_held = 0
            self._exit_state.reset()
            self._exit_state.entry_price = close
            self._exit_state.is_long = (dir_signal > 0)

            # Activate tick exit tracking
            self._tick_exits.open_position(close, dir_signal > 0, symbol="SOLUSDT-PERP")

            return StrategySignal(
                direction=dir_signal,
                reason=f"composite={self._signal.composite():.3f} regime={regime} conf={confidence:.2f}"
            )

        return StrategySignal(direction=0, reason="no signal")

    # ---- Diagnostics ----

    def get_diagnostics(self) -> dict:
        sig_diag = self._signal.get_diagnostics()
        return {
            "bar_count": self._bar_count,
            "in_position": self._in_position,
            "side": self._position_side,
            "entry_price": round(self._entry_price, 4),
            "bars_held": self._bars_held,
            "exit_breakeven": self._exit_state.breakeven_activated,
            "regime": sig_diag["regime"],
            "confidence": sig_diag["confidence"],
            "composite": sig_diag["composite"],
            "direction": sig_diag["direction"],
        }
