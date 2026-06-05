'''Risk loop with Telegram notifications.'''
from __future__ import annotations
import asyncio, logging
from risk.checker import check_stop, check_take, check_hold, check_daily
from base.notify import send_message, fmt_risk_exit, fmt_daily_trip

logger = logging.getLogger(__name__)


def _notify(slot, text: str):
    if slot.telegram_bot_token and slot.telegram_chat_id:
        asyncio.ensure_future(
            send_message(slot.telegram_bot_token, slot.telegram_chat_id, text)
        )


class RiskLoop:
    def __init__(self, registry, executor, interval=1.0):
        self._registry = registry
        self._executor = executor
        self._interval = interval
        self._running = False
        self._task = None
        self._prices: dict[str, float] = {}

    def update_price(self, symbol: str, price: float):
        self._prices[symbol] = price

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass

    async def _run(self):
        while self._running:
            for slot in self._registry.get_active_slots():
                price = 0.0
                symbol = ""
                for sub in slot.subscriptions:
                    p = self._prices.get(sub.symbol, 0)
                    if p > 0:
                        price = p
                        symbol = sub.symbol
                        break
                if price <= 0:
                    continue

                daily = check_daily(slot)
                if daily.should_exit:
                    slot.tripped = True
                    self._executor.flat(slot, daily.reason)
                    _notify(slot, fmt_daily_trip(
                        slot.strategy_id, symbol,
                        slot.daily_pnl, slot.max_daily_loss_pct,
                    ))
                    continue

                for check in [check_stop, check_take, check_hold]:
                    action = check(slot, price)
                    if action.should_exit:
                        self._executor.flat(slot, action.reason)
                        _notify(slot, fmt_risk_exit(
                            slot.strategy_id, symbol,
                            slot.entry_side, price, action.reason,
                        ))
                        break

            await asyncio.sleep(self._interval)
