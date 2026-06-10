"""
Module:    risk/loop
Purpose:   1-second risk monitoring loop. Iterates all active positions,
           runs trail/stop/take/hold/daily checks, triggers emergency flats.

Class: RiskLoop
  __init__(registry, executor, interval=1.0)
      registry: StrategyRegistry    -- source of active slots
      executor: OrderExecutor       -- executes emergency flats
      interval: float               -- check interval in seconds (default 1.0)

  update_price(symbol, price)       -- update latest price for a symbol
  update_atr(symbol, atr)           -- update ATR for trailing stop calc
  start() -> None                   -- begin the risk loop (asyncio task)
  stop() -> None                    -- graceful shutdown

Execution Order (per tick, per slot):
  1. check_daily(slot)   -- daily loss circuit breaker (highest priority)
  2. check_trail(slot, price)  -- trailing stop (tick-level)
  3. check_stop(slot, price)   -- fixed stop loss
  4. check_take(slot, price)   -- take profit
  5. check_hold(slot, price)   -- max hold time

  First check that triggers causes flat() and skip remaining checks.
  Daily trip sets slot.tripped = True (permanent disable until manual reset).

Telegram Integration:
  Risk exits are notified via executor.flat() -> fmt_close (single notification,
  with enriched reason string indicating the trigger).

Performance:
  O(active_slots) per tick. With typical 1-3 active slots, negligible overhead.

Author:    nt-base system
Version:   1.2.0
"""
from __future__ import annotations
'''Risk loop with tick-level exits and Telegram notifications.'''
import asyncio
import logging
from risk.checker import check_trail, check_stop, check_take, check_hold, check_daily

logger = logging.getLogger(__name__)


class RiskLoop:
    def __init__(self, registry, executor, interval=1.0):
        self._registry = registry
        self._executor = executor
        self._interval = interval
        self._running = False
        self._task = None
        self._prices: dict[str, float] = {}
        self._atrs: dict[str, float] = {}

    def update_price(self, symbol: str, price: float):
        self._prices[symbol] = price

    def update_atr(self, symbol: str, atr: float):
        self._atrs[symbol] = atr

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

                # Update slot's ATR (use per-symbol ATR or fallback to 0.15% of price)
                atr = self._atrs.get(symbol, 0.0)
                # Heartbeat: log tick-level price every 60 iterations (~60s)
                if not hasattr(self, '_hb_count'):
                    self._hb_count = 0
                self._hb_count += 1
                if self._hb_count % 60 == 1:
                    logger.info(f"RiskLoop heartbeat: price={price:.4f} "
                                f"high={slot.highest_since_entry:.4f} "
                                f"low={slot.lowest_since_entry:.4f} "
                                f"atr={slot.current_atr:.4f} "
                                f"held={slot.held_sec:.0f}s")
                if atr > 0:
                    slot.current_atr = atr

                # Update highest/lowest since entry for trailing stop
                if slot.has_position:
                    if slot.entry_side == "LONG" and price > slot.highest_since_entry:
                        slot.highest_since_entry = price
                    elif slot.entry_side == "SHORT" and price < slot.lowest_since_entry:
                        slot.lowest_since_entry = price

                daily = check_daily(slot)
                if daily.should_exit:
                    slot.tripped = True
                    self._executor.flat(slot, f"{daily.reason} | CB {slot.max_daily_loss_pct*100:.1f}% paused")
                    continue


                # 0. Bar-submitted close task (from trading-v2 bar-level exit).
                #    RiskLoop retries every second until position is fully closed.
                if slot.pending_bar_exit and slot.has_position:
                    if not self._executor.has_pending_close_for(slot):
                        self._executor.flat(slot, slot.pending_bar_exit)
                    continue  # skip other checks while bar exit task is pending

                # check_trail first (tighter than fixed stop when in profit)
                for check in [check_trail, check_stop, check_take, check_hold]:
                    action = check(slot, price)
                    if action.should_exit:
                        self._executor.flat(slot, action.reason)
                        break

            await asyncio.sleep(self._interval)
