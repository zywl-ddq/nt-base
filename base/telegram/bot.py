"""
StrategyBot -- per-strategy Telegram Bot.

Receives structured commands via DM. Callers send events via notify(msg_type, **data)
and the bot layer handles all formatting. Callers never touch formatting logic.
"""
from __future__ import annotations

import asyncio, logging, html as _html
from typing import Callable
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from base.telegram.commands import parse_command, check_permission

logger = logging.getLogger("telegram.bot")

ALL_COMMANDS = [
    "status", "flatme", "status_all", "flat_all", "flat",
    "pause", "resume", "adj", "pauseme", "resumeme", "ping",
]
HELP_TEXT = (
    "\U0001f4cb <b>Available Commands</b>\n"
    "\n"
    "<b>Trade</b>\n"
    "  /status -- view strategy state\n"
    "  /flatme -- close position\n"
    "  /pauseme -- pause new entries\n"
    "  /resumeme -- resume entries\n"
    "  /adj key value -- adjust param\n"
    "\n"
    "<b>Admin Only</b>\n"
    "  /status_all -- all strategies\n"
    "  /flat id -- close specific\n"
    "  /flat_all -- close all\n"
    "  /pause id -- pause specific\n"
    "  /resume id -- resume specific\n"
)


class StrategyBot:
    def __init__(self, instance_id, bot_token, operator_chat_id, admin_chat_id,
                 executor=None, registry=None, grpc_servicer=None):
        self.instance_id = instance_id
        self.bot_token = bot_token
        self.operator_chat_id = operator_chat_id
        self.admin_chat_id = admin_chat_id
        self._executor = executor
        self._registry = registry
        self._grpc = grpc_servicer
        self._app = None

    # -- Lifecycle --

    async def start(self):
        self._app = Application.builder().token(self.bot_token).build()
        for cmd in ALL_COMMANDS:
            self._app.add_handler(CommandHandler(cmd, self._dispatch))
        self._app.add_handler(CommandHandler("help", self._help))
        self._app.add_handler(CommandHandler("start", self._help))
        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        logger.info(f"Bot started: {self.instance_id} operator={self.operator_chat_id}")

    async def stop(self):
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            logger.info(f"Bot stopped: {self.instance_id}")

    # -- Event notification (caller passes msg_type + data, bot formats) --

    async def notify(self, msg_type: str, **data):
        """Receive event type + data dict. Bot handles all formatting internally."""
        from base.notify import (
            fmt_entry, fmt_close, fmt_risk_exit, fmt_daily_trip,
            fmt_reverse, fmt_orphan_alert, fmt_strategy_start,
        )
        renderers = {
            "entry": lambda d: fmt_entry(d["sid"], d["symbol"], d["side"],
                d["price"], d["qty"], d["notional"], d["reason"]),
            "close": lambda d: fmt_close(d["sid"], d["symbol"], d["side"],
                d["entry_px"], d["exit_px"], d["pnl"], d["held_sec"], d["reason"]),
            "risk_exit": lambda d: fmt_risk_exit(d["sid"], d["symbol"], d["side"],
                d["price"], d["reason"], d.get("pnl", 0.0)),
            "daily_trip": lambda d: fmt_daily_trip(d["sid"], d["symbol"],
                d["daily_pnl"], d["loss_limit"]),
            "reverse": lambda d: fmt_reverse(d["sid"], d["symbol"],
                d["from_side"], d["to_side"], d["price"], d["reason"]),
            "orphan": lambda d: fmt_orphan_alert(d["sid"], d["symbol"],
                d["side"], d["entry_px"], d["held_sec"]),
            "strategy_start": lambda d: fmt_strategy_start(d["sid"], d["symbol"],
                d["leverage"], d["pos_pct"]),
        }
        fn = renderers.get(msg_type)
        if fn:
            await self.send(fn(data))

    # -- Command dispatch --

    async def _help(self, update, context):
        await update.message.reply_text(HELP_TEXT, parse_mode="HTML")

    async def _dispatch(self, update, context):
        chat_id = str(update.effective_chat.id)
        cmd_text = update.message.text.strip()
        cmd_name, args = parse_command(cmd_text)
        level = check_permission(chat_id, self.operator_chat_id, self.admin_chat_id)

        handler = _HANDLERS.get(cmd_name)
        if handler is None:
            await update.message.reply_text(
                f"❓ Unknown: /{cmd_name}\nSend /help for commands",
                parse_mode="HTML")
            return
        if level == 0:
            await update.message.reply_text(
                "\U0001f6ab No permission.", parse_mode="HTML")
            return
        try:
            reply = await handler(self, args, level, chat_id)
        except Exception as e:
            logger.error(f"Handler {cmd_name} error: {e}", exc_info=True)
            reply = f"❌ Error: {_html.escape(str(e))}"
        await update.message.reply_text(reply, parse_mode="HTML")

    # -- Raw send (for internal use by notify) --

    async def send(self, text: str):
        if self._app:
            try:
                await self._app.bot.send_message(
                    chat_id=self.operator_chat_id, text=text,
                    parse_mode="HTML", disable_web_page_preview=True)
            except Exception as e:
                logger.warning(f"Bot {self.instance_id} send failed: {e}")


# -- Handler registry --

_HANDLERS: dict[str, Callable] = {}

def register_handler(cmd_name: str):
    def decorator(fn):
        _HANDLERS[cmd_name] = fn
        return fn
    return decorator
