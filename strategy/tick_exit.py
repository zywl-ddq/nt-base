"""
TickExitManager -- Quad-Layer Tick-Level Exit System

L1: Toxic Flow Smart Exit (1s window, 5:1 taker ratio)
L2: Tick-Level Trailing Stop (1.5x ATR)
L3: Breakeven Ladder (activated at 1.5x ATR profit)
L4: Time Decay (in on_bar, held >15 bars, return <0.15%)

All tick-level exits use the 1-second trade window deque.
"""
from collections import deque
from dataclasses import dataclass
import time


@dataclass
class TickTrade:
    price: float
    size: float
    is_buyer: bool
    ts_ns: int


@dataclass
class TickExitAction:
    reason: str
    urgency: str = "immediate"


class TickExitManager:
    def __init__(self,
                 toxic_vol_threshold: float = 500.0,  # min taker vol in 1s to trigger
                 toxic_ratio: float = 5.0,             # taker_vol / opposite_vol ratio
                 trail_atr_mult: float = 1.5,          # trailing stop ATR multiplier
                 breakeven_atr_mult: float = 1.5,      # breakeven activation ATR multiplier
                 breakeven_fee_pct: float = 0.001,     # 0.1% fee cover
                 ):
        self.toxic_vol_threshold = toxic_vol_threshold
        self.toxic_ratio = toxic_ratio
        self.trail_atr_mult = trail_atr_mult
        self.breakeven_atr_mult = breakeven_atr_mult
        self.breakeven_fee_pct = breakeven_fee_pct

        # 1-second trade window
        self._trade_window: deque[TickTrade] = deque()
        self._window_ns: int = 1_000_000_000  # 1 second in nanoseconds

        # Position state
        self._in_position = False
        self._is_long = True
        self._entry_price = 0.0
        self._highest_tick: float = 0.0
        self._lowest_tick: float = float("inf")
        self._breakeven_activated = False

        # Current ATR (updated from on_bar)
        self._current_atr: float = 0.0
        self._symbol: str = ""

    # ---- Position lifecycle ----

    def open_position(self, entry_price: float, is_long: bool, symbol: str = ""):
        self._symbol = symbol
        self._in_position = True
        self._is_long = is_long
        self._entry_price = entry_price
        self._highest_tick = entry_price if is_long else 0.0
        self._lowest_tick = entry_price if not is_long else float("inf")
        self._breakeven_activated = False
        self._trade_window.clear()

    def close_position(self):
        self._in_position = False
        self._symbol = ""
        self._trade_window.clear()

    def update_atr(self, atr: float):
        self._current_atr = atr

    @property
    def in_position(self) -> bool:
        return self._in_position

    # ---- Tick processing ----

    def on_tick(self, price: float, size: float, is_buyer: bool, ts_ns: int, symbol: str = "") -> TickExitAction | None:
        if not self._in_position:
            return None

        # Symbol validation: reject ticks from wrong instrument
        if self._symbol and symbol and symbol != self._symbol:
            return None

        # Add to window and prune old
        trade = TickTrade(price=price, size=size, is_buyer=is_buyer, ts_ns=ts_ns)
        self._trade_window.append(trade)
        self._prune_window(ts_ns)

        # Update high/low
        if self._is_long:
            self._highest_tick = max(self._highest_tick, price)
        else:
            self._lowest_tick = min(self._lowest_tick, price)

        # L1: Toxic Flow
        result = self._check_toxic_flow()
        if result:
            return result

        # L2: Tick Trailing Stop
        result = self._check_trailing_stop(price)
        if result:
            return result

        # L3: Breakeven activation
        self._check_breakeven_activation(price)

        return None

    # ---- L1: Toxic Flow Smart Exit ----

    def _prune_window(self, current_ns: int):
        cutoff = current_ns - self._window_ns
        while self._trade_window and self._trade_window[0].ts_ns < cutoff:
            self._trade_window.popleft()

    def _check_toxic_flow(self) -> TickExitAction | None:
        return None  # Disabled: threshold too sensitive for liquid markets, killed every trade in 60s
        if len(self._trade_window) < 3:
            return None

        buy_vol = sum(t.size for t in self._trade_window if t.is_buyer)
        sell_vol = sum(t.size for t in self._trade_window if not t.is_buyer)

        if self._is_long:
            if sell_vol > self.toxic_vol_threshold and sell_vol > buy_vol * self.toxic_ratio:
                return TickExitAction(
                    f"ToxicFlow: sell_vol={sell_vol:.0f} buy_vol={buy_vol:.0f} ratio={sell_vol/buy_vol:.1f}" if buy_vol > 0 else f"ToxicFlow: sell_vol={sell_vol:.0f} (no buys)",
                    "immediate"
                )
        else:
            if buy_vol > self.toxic_vol_threshold and buy_vol > sell_vol * self.toxic_ratio:
                return TickExitAction(
                    f"ToxicFlow: buy_vol={buy_vol:.0f} sell_vol={sell_vol:.0f} ratio={buy_vol/sell_vol:.1f}" if sell_vol > 0 else f"ToxicFlow: buy_vol={buy_vol:.0f} (no sells)",
                    "immediate"
                )
        return None

    # ---- L2: Tick-Level Trailing Stop ----

    def _check_trailing_stop(self, current_price: float) -> TickExitAction | None:
        atr = self._current_atr if self._current_atr > 0 else current_price * 0.0015

        if self._breakeven_activated:
            if self._is_long:
                fee_cover = self._entry_price * (1.0 + self.breakeven_fee_pct)
            else:
                fee_cover = self._entry_price * (1.0 - self.breakeven_fee_pct)
        else:
            fee_cover = 0.0

        if self._is_long:
            stop_price = max(fee_cover, self._highest_tick - self.trail_atr_mult * atr)
            if current_price <= stop_price:
                return TickExitAction(
                    f"TickTrail: long stop at {stop_price:.4f} (highest={self._highest_tick:.4f} atr={atr:.4f})",
                    "immediate"
                )
        else:
            stop_price = min(fee_cover, self._lowest_tick + self.trail_atr_mult * atr) if self._breakeven_activated else self._lowest_tick + self.trail_atr_mult * atr
            if current_price >= stop_price:
                return TickExitAction(
                    f"TickTrail: short stop at {stop_price:.4f} (lowest={self._lowest_tick:.4f} atr={atr:.4f})",
                    "immediate"
                )
        return None

    # ---- L3: Breakeven Activation ----

    def _check_breakeven_activation(self, current_price: float) -> None:
        if self._breakeven_activated:
            return

        atr = self._current_atr if self._current_atr > 0 else current_price * 0.0015

        if self._is_long:
            profit = current_price - self._entry_price
            if profit > self.breakeven_atr_mult * atr:
                self._breakeven_activated = True
        else:
            profit = self._entry_price - current_price
            if profit > self.breakeven_atr_mult * atr:
                self._breakeven_activated = True

    # ---- L4: Time Decay (called from on_bar) ----

    def check_time_decay(self, bars_held: int, current_price: float,
                         time_limit_bars: int = 15,
                         min_return_pct: float = 0.0015) -> TickExitAction | None:
        if not self._in_position or bars_held < time_limit_bars:
            return None

        if self._is_long:
            ret = (current_price - self._entry_price) / self._entry_price
        else:
            ret = (self._entry_price - current_price) / self._entry_price

        if abs(ret) < min_return_pct:
            return TickExitAction(
                f"TimeDecay: {bars_held} bars, ret={ret:.4%}",
                "normal"
            )
        return None
