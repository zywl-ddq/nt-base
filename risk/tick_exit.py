'''
TickExitManager -- Tick-Level Exit System (runs inside nt-base).

L1: Toxic Flow Smart Exit (1s window, 5:1 taker ratio) [DISABLED]
L2: Tick-Level Trailing Stop (ATR-based)
L3: Breakeven Ladder (activated at 1.5x ATR profit)

All tick-level exits use the 1-second trade window deque.
Moved from trading-v2 to nt-base because tick data lives here.
'''
from collections import deque
from dataclasses import dataclass


@dataclass
class TickTrade:
    price: float
    size: float
    is_buyer: bool
    ts_ns: int


@dataclass
class TickExitAction:
    reason: str
    urgency: str = 'immediate'


class TickExitManager:
    def __init__(self,
                 toxic_vol_threshold: float = 500.0,
                 toxic_ratio: float = 5.0,
                 trail_atr_mult: float = 4.0,
                 breakeven_atr_mult: float = 1.5,
                 breakeven_fee_pct: float = 0.001,
                 ):
        self.toxic_vol_threshold = toxic_vol_threshold
        self.toxic_ratio = toxic_ratio
        self.trail_atr_mult = trail_atr_mult
        self.breakeven_atr_mult = breakeven_atr_mult
        self.breakeven_fee_pct = breakeven_fee_pct

        # 1-second trade window
        self._trade_window: deque[TickTrade] = deque()
        self._window_ns: int = 1_000_000_000

        # Position state
        self._in_position = False
        self._is_long = True
        self._entry_price = 0.0
        self._highest_tick: float = 0.0
        self._lowest_tick: float = float('inf')
        self._breakeven_activated = False

        # Current ATR (updated from bar dispatch)
        self._current_atr: float = 0.0
        self._symbol: str = ''

    # ---- Position lifecycle ----

    def open_position(self, entry_price: float, is_long: bool, symbol: str = ''):
        self._symbol = symbol
        self._in_position = True
        self._is_long = is_long
        self._entry_price = entry_price
        self._highest_tick = entry_price if is_long else 0.0
        self._lowest_tick = entry_price if not is_long else float('inf')
        self._breakeven_activated = False
        self._trade_window.clear()

    def close_position(self):
        self._in_position = False
        self._symbol = ''
        self._trade_window.clear()

    def update_atr(self, atr: float):
        self._current_atr = atr

    @property
    def in_position(self) -> bool:
        return self._in_position

    # ---- Tick processing ----

    def on_tick(self, price: float, size: float, is_buyer: bool,
                ts_ns: int, symbol: str = '') -> TickExitAction | None:
        if not self._in_position:
            return None

        if self._symbol and symbol and symbol != self._symbol:
            return None

        trade = TickTrade(price=price, size=size, is_buyer=is_buyer, ts_ns=ts_ns)
        self._trade_window.append(trade)
        self._prune_window(ts_ns)

        if self._is_long:
            self._highest_tick = max(self._highest_tick, price)
        else:
            self._lowest_tick = min(self._lowest_tick, price)

        # L1: Toxic Flow (disabled)
        # result = self._check_toxic_flow()
        # if result: return result

        # L2: Tick Trailing Stop
        result = self._check_trailing_stop(price)
        if result:
            return result

        # L3: Breakeven activation
        self._check_breakeven_activation(price)

        return None

    # ---- L1: Toxic Flow ----

    def _prune_window(self, current_ns: int):
        cutoff = current_ns - self._window_ns
        while self._trade_window and self._trade_window[0].ts_ns < cutoff:
            self._trade_window.popleft()

    def _check_toxic_flow(self) -> TickExitAction | None:
        return None  # Disabled

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
                    f'TickTrail LONG stop={stop_price:.4f} '
                    f'high={self._highest_tick:.4f} atr={atr:.4f}',
                    'immediate'
                )
        else:
            if self._breakeven_activated:
                stop_price = min(fee_cover, self._lowest_tick + self.trail_atr_mult * atr)
            else:
                stop_price = self._lowest_tick + self.trail_atr_mult * atr
            if current_price >= stop_price:
                return TickExitAction(
                    f'TickTrail SHORT stop={stop_price:.4f} '
                    f'low={self._lowest_tick:.4f} atr={atr:.4f}',
                    'immediate'
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
