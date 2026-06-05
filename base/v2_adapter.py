"""V2SignalAdapter 鈥?wraps trading-v2 AlphaSignal in nt-base's SignalStrategy.

Bridges the protocol gap:
  nt-base  : on_bar(bar_data: dict) -> StrategySignal | None
  trading-v2: on_bar(close, high, low, delta_buy, delta_sell, btc_close, ts_ns)

Strategy code (AlphaSignal) remains pure 鈥?no DB, no NT, no I/O.
"""
from __future__ import annotations
from base.signal_protocol import SignalStrategy, StrategySignal, BarSubscription


class V2SignalAdapter:
    """Adapts trading-v2 AlphaSignal to nt-base's SignalStrategy protocol."""

    def __init__(self, alpha_signal, strategy_id: str,
                 symbol: str, timeframe: str = "1m"):
        self._signal = alpha_signal
        self._id = strategy_id
        self._symbol = symbol
        self._tf = timeframe
        self._subs = [
            BarSubscription(
                symbol=symbol,
                timeframe=timeframe,
                factors=alpha_signal.factor_names,
            )
        ]

    # 鈹€鈹€ SignalStrategy protocol 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

    @property
    def strategy_id(self) -> str:
        return self._id

    @property
    def subscriptions(self) -> list[BarSubscription]:
        return self._subs

    def on_bar(self, bar_data: dict) -> StrategySignal | None:
        """Translate nt-base bar dict 鈫?trading-v2 on_bar call."""
        close = float(bar_data.get("close", 0))
        high = float(bar_data.get("high", close))
        low = float(bar_data.get("low", close))
        ts_ns = bar_data.get("ts_ns", 0)

        # Push factor values to AlphaSignal
        factors = bar_data.get("factors", {})
        for fname, value in factors.items():
            self._signal.set_factor_value(fname, ts_ns, float(value))

        # btc_close: if nt-base doesn't provide it, use SOL close as proxy
        btc_close = float(bar_data.get("btc_close", close))

        # delta vol: nt-base doesn't track tick delta on 1m bars
        delta_buy = float(bar_data.get("delta_buy", 0.0))
        delta_sell = float(bar_data.get("delta_sell", 0.0))

        result = self._signal.on_bar(
            close=close, high=high, low=low,
            delta_buy_vol=delta_buy, delta_sell_vol=delta_sell,
            btc_close=btc_close, ts_ns=ts_ns,
        )

        if result is None:
            return None
        # Both projects use @dataclass StrategySignal(direction, reason) - compatible
        return StrategySignal(direction=result.direction, reason=result.reason)

    def on_shutdown(self) -> None:
        if hasattr(self._signal, 'on_shutdown'):
            self._signal.on_shutdown()

    def get_diagnostics(self) -> dict:
        return self._signal.get_diagnostics()

    # 鈹€鈹€ Access 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

    @property
    def inner(self):
        return self._signal
