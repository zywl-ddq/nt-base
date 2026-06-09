"""
Telegram module — multi-bot management for trading strategies.

TelegramManager:
  - Reads active strategy_instances from DB
  - Creates one StrategyBot per instance
  - Manages lifecycle (start/stop/reload)

Singleton accessor:
  - set_manager() / get_manager() for cross-scope access
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from base.telegram.bot import StrategyBot

logger = logging.getLogger("telegram.manager")

# Module-level singleton (set by main() after construction)
_manager: TelegramManager | None = None


def set_manager(mgr):
    global _manager
    _manager = mgr


def get_manager():
    return _manager


class TelegramManager:
    """Manages all per-strategy Telegram bots."""

    def __init__(self, executor=None, registry=None, grpc_servicer=None,
                 admin_chat_id: str = ""):
        self._executor = executor
        self._registry = registry
        self._grpc = grpc_servicer
        self._admin_chat_id = admin_chat_id
        self._bots: dict[str, StrategyBot] = {}

    async def start_all(self):
        """Create and start bots for all registered strategies (from memory)."""
        from base.telegram.bot import StrategyBot

        if not self._grpc:
            logger.warning("No gRPC servicer, skipping Telegram bots")
            return

        strategies = getattr(self._grpc, '_strategies', {})
        if not strategies:
            logger.info("No registered strategies, skipping Telegram bots")
            return

        for sid, info in strategies.items():
            token = (info.get("telegram_bot_token") or "").strip()
            chat_id = (info.get("telegram_chat_id") or "").strip()

            if not token or not chat_id:
                logger.warning(f"Skipping bot for {sid}: missing token or chat_id")
                continue

            bot = StrategyBot(
                instance_id=sid,
                bot_token=token,
                operator_chat_id=chat_id,
                admin_chat_id=self._admin_chat_id,
                executor=self._executor,
                registry=self._registry,
                grpc_servicer=self._grpc,
            )
            try:
                await bot.start()
                self._bots[sid] = bot
                logger.info(f"Telegram bot started for {sid}")
            except Exception as e:
                logger.error(f"Failed to start bot for {sid}: {e}")

    async def stop_all(self):
        """Stop all bots gracefully."""
        for sid, bot in list(self._bots.items()):
            try:
                await bot.stop()
            except Exception as e:
                logger.error(f"Error stopping bot {sid}: {e}")
        self._bots.clear()
        logger.info("All Telegram bots stopped")

    async def reload(self, instance_id: str):
        """Reload a single bot (e.g., after config change)."""
        if instance_id in self._bots:
            await self._bots[instance_id].stop()
            del self._bots[instance_id]
        await self.start_all()  # re-read from DB

    def get_bot(self, instance_id: str):
        return self._bots.get(instance_id)

    def get_all_bots(self):
        return dict(self._bots)
