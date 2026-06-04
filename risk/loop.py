"""Risk loop — 1s interval check for all active strategy slots."""
from __future__ import annotations
import asyncio, logging
from risk.checker import check_stop, check_take, check_hold, check_daily

logger = logging.getLogger(__name__)


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
                price = self._prices.get("SOLUSDT-PERP.BINANCE", 0)
                if price <= 0:
                    continue

                daily = check_daily(slot)
                if daily.should_exit:
                    slot.tripped = True
                    self._executor.flat(slot, daily.reason)
                    continue

                for check in [check_stop, check_take, check_hold]:
                    action = check(slot, price)
                    if action.should_exit:
                        self._executor.flat(slot, action.reason)
                        break

            await asyncio.sleep(self._interval)
