"""
RegistrationManager v2 -- supports factor_3 and adaptive params.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
import asyncpg

from base.notify import send_message, fmt_strategy_start

logger = logging.getLogger(__name__)

POLL_SEC = 5


class RegistrationManager:
    def __init__(self, registry, pool: asyncpg.Pool,
                 symbol: str = "SOLUSDT-PERP", timeframe: str = "1m"):
        self._registry = registry
        self._pool = pool
        self._symbol = symbol
        self._timeframe = timeframe
        self._stop = asyncio.Event()
        self._known: dict[str, str] = {}

    async def run(self):
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
                    await self._activate(conn, row)
                elif status == "stopping" and prev == "active":
                    await self._deactivate(conn, row)
                elif status in ("stopped", "error"):
                    self._known[iid] = status

    async def _activate(self, conn, row):
        iid = row["instance_id"]
        params = row["params"] if isinstance(row["params"], dict) else json.loads(row["params"] or "{}")
        token = row["telegram_bot_token"] or ""
        chat_id = row["telegram_chat_id"] or ""

        logger.info("Activating %s (gRPC path): params=%s", iid, json.dumps(params, default=str)[:200])

        try:
            await conn.execute(
                "UPDATE strategy_instances SET status='active', activated_at=$2 WHERE instance_id=$1",
                iid, datetime.now(timezone.utc),
            )

            if token and chat_id:
                await send_message(token, chat_id, fmt_strategy_start(
                    iid, self._symbol,
                    params.get("leverage", 2),
                    params.get("position_size_pct", 0.20),
                ))

            self._known[iid] = "active"
            logger.info("Activated %s (gRPC path) - start with: python run_live.py %s", iid, iid)

        except Exception as e:
            logger.error("Failed to activate %s: %s", iid, e, exc_info=True)
            self._known[iid] = "error"
            await conn.execute(
                "UPDATE strategy_instances SET status='error', error_message=$2 WHERE instance_id=$1",
                iid, str(e)[:500],
            )

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
