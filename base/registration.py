"""
Module:    base/registration
Purpose:   Dynamic strategy lifecycle manager. Polls strategy_instances table
           and hot-registers/unregisters strategies without service restart.

Class: RegistrationManager
  __init__(registry, pool, symbol, timeframe)
      registry: StrategyRegistry   鈥?target registry for dynamic registration
      pool: asyncpg.Pool           鈥?DB connection pool for polling
      symbol: str                  鈥?trading pair (e.g. "SOLUSDT-PERP")
      timeframe: str               鈥?bar timeframe for subscription

  run() -> None                    鈥?main polling loop (never raises)
  stop() -> None                   鈥?graceful shutdown

Registration Flow:
  1. External system INSERTs into strategy_instances with status='pending'
  2. RegistrationManager polls every POLL_SEC (5s)
  3. On new 'pending' row: builds AlphaSignal(params) -> V2SignalAdapter -> StrategySlot
  4. Calls registry.register(slot), updates DB to status='active'
  5. Sends Telegram strategy_start notification

Unregistration Flow:
  1. External system UPDATEs status='stopping'
  2. RegistrationManager detects change -> registry.unregister(strategy_id)
  3. Updates DB to status='stopped'

Restart Recovery:
  On service restart, strategies with DB status='active' are re-registered
  in memory (the _known dict is ephemeral).

State Machine:
  pending  --[_activate]--> active  --[_deactivate]--> stopped
     |                         |
     +----[error]--> error     +----[stopping]--> stopped

Error Handling:
  Activation failures set status='error' with error_message in DB.
  The main poll loop catches all exceptions and continues (self-healing).

Author:    nt-base system
Version:   1.0.0
"""
from __future__ import annotations
"""RegistrationManager 鈥?dynamic strategy registration without restart.

Polls the strategy_instances table for new/stopping entries.
Creates AlphaSignal + V2SignalAdapter + StrategySlot on the fly.

Usage:
    reg = RegistrationManager(registry, pool, symbol="SOLUSDT-PERP")
    asyncio.create_task(reg.run())
"""


import asyncio
import json
import logging
from datetime import datetime, timezone

import asyncpg

from base.slot import StrategySlot
from base.v2_adapter import V2SignalAdapter
from base.v2_signal import AlphaSignal
from base.notify import send_message, fmt_strategy_start

logger = logging.getLogger(__name__)

POLL_SEC = 5  # check for new registrations every 5s


class RegistrationManager:
    """Watches strategy_instances table and manages runtime strategy lifecycle."""

    def __init__(self, registry, pool: asyncpg.Pool,
                 symbol: str = "SOLUSDT-PERP",
                 timeframe: str = "1m"):
        self._registry = registry
        self._pool = pool
        self._symbol = symbol
        self._timeframe = timeframe
        self._stop = asyncio.Event()
        self._known: dict[str, str] = {}  # instance_id -> status

    async def run(self):
        """Main polling loop. Never raises 鈥?logs and continues."""
        logger.info("RegistrationManager started: poll=%ss symbol=%s", POLL_SEC, self._symbol)
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception as e:
                logger.error("RegistrationManager tick error: %s", e, exc_info=True)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=POLL_SEC)
            except asyncio.TimeoutError:
                pass

    async def stop(self):
        self._stop.set()

    # 鈹€鈹€ Tick 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

    async def _tick(self):
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM strategy_instances ORDER BY id ASC"
            )

            for row in rows:
                iid = row["instance_id"]
                status = row["status"]
                prev = self._known.get(iid)

                if prev == status:
                    continue

                if status == "pending" and prev is None:
                    await self._activate(conn, row)
                elif status == "active" and prev is None:
                    # Restart recovery: re-register strategy in memory
                    await self._activate(conn, row)
                elif status == "stopping" and prev == "active":
                    await self._deactivate(conn, row)
                elif status in ("stopped", "error"):
                    self._known[iid] = status

    # 鈹€鈹€ Activate 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

    async def _activate(self, conn, row):
        iid = row["instance_id"]
        params = row["params"] if isinstance(row["params"], dict) else json.loads(row["params"] or "{}")
        token = row["telegram_bot_token"] or ""
        chat_id = row["telegram_chat_id"] or ""

        logger.info("Activating %s: params=%s", iid, json.dumps(params, default=str)[:200])

        try:
            # Build AlphaSignal from DB params
            alpha = AlphaSignal(
                gate_factor=params.get("gate_factor", "trend_regime"),
                factor_1=params.get("factor_1", "cvd_divergence"),
                direction_1=params.get("direction_1", -1),
                weight_1=params.get("weight_1", 1.0),
                factor_2=params.get("factor_2", "residual_momentum"),
                direction_2=params.get("direction_2", 1),
                weight_2=params.get("weight_2", 0.5),
                signal_threshold=params.get("signal_threshold", 0.28),
                atr_period=params.get("atr_period", 30),
                btc_shock_long=params.get("btc_shock_long", 0.0085),
                btc_shock_short=params.get("btc_shock_short", 0.0075),
                time_limit_long=params.get("time_limit_long", 40),
                time_limit_short=params.get("time_limit_short", 18),
                max_hold_minutes=params.get("max_hold_minutes", 40),
                breakeven_atr_mult=params.get("breakeven_atr_mult", 1.4),
                trail_trigger_atr=params.get("trail_trigger_atr", 2.0),
                trail_stop_atr=params.get("trail_stop_atr", 1.0),
            )

            # Wrap in adapter + slot
            adapter = V2SignalAdapter(alpha, iid, self._symbol, self._timeframe)
            slot = StrategySlot(
                strategy_id=iid,
                strategy=adapter,
                subscriptions=adapter.subscriptions,
                stop_pct=params.get("stop_pct", 0.03),
                take_pct=params.get("take_pct", 0.06),
                max_hold_sec=params.get("max_hold_sec", 3600),
                cooldown_sec=params.get("cooldown_sec", 60.0),
                leverage=params.get("leverage", 2),
                position_size_pct=params.get("position_size_pct", 0.20),
                symbol=self._symbol,
                telegram_bot_token=token,
                telegram_chat_id=chat_id,
            )

            self._registry.register(slot)
            self._known[iid] = "active"

            # Update DB
            await conn.execute(
                "UPDATE strategy_instances SET status='active', activated_at=$2 WHERE instance_id=$1",
                iid, datetime.now(timezone.utc),
            )

            # Notify
            if token and chat_id:
                await send_message(token, chat_id, fmt_strategy_start(
                    iid, self._symbol, slot.leverage, slot.position_size_pct,
                ))

            logger.info("Activated %s: factors=%s", iid, alpha.factor_names)

        except Exception as e:
            logger.error("Failed to activate %s: %s", iid, e, exc_info=True)
            self._known[iid] = "error"
            await conn.execute(
                "UPDATE strategy_instances SET status='error', error_message=$2 WHERE instance_id=$1",
                iid, str(e)[:500],
            )

    # 鈹€鈹€ Deactivate 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

    async def _deactivate(self, conn, row):
        iid = row["instance_id"]
        logger.info("Deactivating %s", iid)

        try:
            self._registry.unregister(iid)
            self._known[iid] = "stopped"
            await conn.execute(
                "UPDATE strategy_instances SET status='stopped', stopped_at=$2 WHERE instance_id=$1",
                iid, datetime.now(timezone.utc),
            )
            logger.info("Deactivated %s", iid)
        except Exception as e:
            logger.error("Failed to deactivate %s: %s", iid, e, exc_info=True)
            await conn.execute(
                "UPDATE strategy_instances SET status='error', error_message=$2 WHERE instance_id=$1",
                iid, str(e)[:500],
            )
