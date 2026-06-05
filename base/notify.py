"""
Module:    base/notify
Purpose:   Lightweight Telegram Bot API sender. Zero third-party dependencies
           (stdlib only: urllib + asyncio + html). Supports per-strategy bot tokens.

Interface:
  send_message(token, chat_id, text) -> None   fire-and-forget async send
  fmt_entry(slot_id, symbol, side, price, qty, notional, reason) -> str
  fmt_close(slot_id, symbol, side_was, entry_px, exit_px, pnl, held_sec, reason) -> str
  fmt_reverse(slot_id, symbol, from_side, to_side, price, reason) -> str
  fmt_risk_exit(slot_id, symbol, side, price, reason, pnl) -> str
  fmt_daily_trip(slot_id, symbol, daily_pnl, loss_limit) -> str
  fmt_strategy_start(slot_id, symbol, leverage, pos_pct) -> str

Design Decisions:
  - urllib (not aiohttp/httpx): zero additional deps, sufficient for low-volume alerts
  - HTML parse_mode: supports <b>/<code>/<pre> formatting
  - Fire-and-forget: notifications never block the trading loop
  - Plain text fallback: Telegram HTML parser quirks are caught silently

Security:
  - Token never logged (only used in HTTP POST)
  - HTML-escaping prevents injection via symbol names or reason strings
  - 5-second timeout prevents hanging on Telegram API outage

Performance:
  - _send_sync runs in thread pool executor (loop.run_in_executor)
  - HTTP connection not reused (simplicity over micro-optimization for alert volume)

Author:    nt-base system
Version:   1.0.0
"""
from __future__ import annotations
"""Telegram notifier - async fire-and-forget."""

import asyncio
import json
import logging
import urllib.request
import html as _html

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _send_sync(token: str, chat_id: str, text: str) -> bool:
    url = TELEGRAM_API.format(token=token)
    body = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")
        return False


async def send_message(token: str, chat_id: str, text: str) -> None:
    if not token or not chat_id:
        return
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _send_sync, token, chat_id, text)
    except Exception:
        pass


def _esc(s) -> str:
    if s is None:
        return "-"
    return _html.escape(str(s), quote=False)


def fmt_entry(slot_id: str, symbol: str, side: str, price: float,
              qty: float, notional: float, reason: str) -> str:
    emoji = "[LONG]" if side == "LONG" else "[SHORT]"
    return (
        f"{emoji} <b>Open</b> [{_esc(slot_id)}]\n"
        f"<code>{_esc(symbol)}</code> {_esc(side)}  "
        f"qty={qty:.3f}  px={price:.4f}\n"
        f"notional={notional:.2f} USDT  reason={_esc(reason)}"
    )


def fmt_close(slot_id: str, symbol: str, side_was: str,
              entry_px: float, exit_px: float, pnl: float,
              held_sec: float, reason: str) -> str:
    emoji = "[WIN]" if pnl >= 0 else "[LOSS]"
    mins = int(held_sec // 60)
    secs = int(held_sec % 60)
    return (
        f"{emoji} <b>Close</b> [{_esc(slot_id)}]\n"
        f"<code>{_esc(symbol)}</code> {_esc(side_was)}  "
        f"PnL={pnl:+.4f} USDT\n"
        f"entry={entry_px:.4f}  exit={exit_px:.4f}  "
        f"held={mins}m{secs}s\n"
        f"reason={_esc(reason)}"
    )


def fmt_reverse(slot_id: str, symbol: str, from_side: str,
                to_side: str, price: float, reason: str) -> str:
    return (
        f"<b>Reverse</b> [{_esc(slot_id)}]\n"
        f"<code>{_esc(symbol)}</code> {_esc(from_side)} -> {_esc(to_side)}  "
        f"px={price:.4f}\n"
        f"reason={_esc(reason)}"
    )


def fmt_risk_exit(slot_id: str, symbol: str, side: str,
                  price: float, reason: str, pnl: float = 0) -> str:
    return (
        f"<b>Risk Exit</b> [{_esc(slot_id)}]\n"
        f"<code>{_esc(symbol)}</code> {_esc(side)}  "
        f"px={price:.4f}  PnL={pnl:+.4f}\n"
        f"trigger={_esc(reason)}"
    )


def fmt_daily_trip(slot_id: str, symbol: str, daily_pnl: float,
                   loss_limit: float) -> str:
    return (
        f"<b>Daily Loss Trip</b> [{_esc(slot_id)}]\n"
        f"<code>{_esc(symbol)}</code>  "
        f"daily PnL={daily_pnl:+.4f}  (limit {loss_limit*100:.1f}%)"
    )


def fmt_strategy_start(slot_id: str, symbol: str, leverage: int,
                       pos_pct: float) -> str:
    return (
        f"<b>Strategy Start</b> [{_esc(slot_id)}]\n"
        f"symbol=<code>{_esc(symbol)}</code>  "
        f"leverage={leverage}x  size={pos_pct*100:.0f}%"
    )
