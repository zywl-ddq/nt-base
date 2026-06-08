"""
Telegram 通知模块 — 中文消息 + emoji 图标，HTML 格式。

图标体系:
  做多开仓 / 做空开仓 / 盈利平仓 / 亏损平仓
  风控退出 / 日亏损熔断 / 反转开仓 / 时间衰减退出
  策略启动 / 状态 / 平仓
"""
from __future__ import annotations

import asyncio, json, logging, urllib.request, html as _html

logger = logging.getLogger(__name__)
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _send_sync(token, chat_id, text):
    url = TELEGRAM_API.format(token=token)
    body = json.dumps({
        "chat_id": chat_id, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")
        return False


async def send_message(token, chat_id, text):
    if not token or not chat_id:
        return
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _send_sync, token, chat_id, text)
    except Exception as e:
        logger.warning(f"send_message failed: {e}")


def _esc(s):
    if s is None: return "-"
    return _html.escape(str(s), quote=False)


def _pnl(p):
    return f"<b>{p:+.4f}</b>"

def _held(sec):
    m, s = divmod(int(sec), 60)
    return f"{m}分{s}秒" if m > 0 else f"{s}秒"


# -- 入场 --

def fmt_entry(slot_id, symbol, side, price, qty, notional, reason):
    icon = "\U0001f7e2 做多" if side == "LONG" else "\U0001f534 做空"
    return (
        f"{icon} <b>开仓</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 方向: <b>{_esc(side)}</b>\n"
        f"├ 价格: <code>{price:.4f}</code>\n"
        f"├ 数量: {qty:.3f}  名义价值: {notional:.2f} USDT\n"
        f"└ 原因: {_esc(reason)}"
    )


# -- 平仓 --

def fmt_close(slot_id, symbol, side_was, entry_px, exit_px, pnl, held_sec, reason):
    icon = "✅ 盈利平仓" if pnl >= 0 else "❌ 亏损平仓"
    return (
        f"{icon}  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>  {_esc(side_was)}\n"
        f"├ 入场: <code>{entry_px:.4f}</code>  →  出场: <code>{exit_px:.4f}</code>\n"
        f"├ 盈亏: {_pnl(pnl)} USDT\n"
        f"├ 持仓: {_held(held_sec)}\n"
        f"└ 原因: {_esc(reason)}"
    )


# -- 反转 --

def fmt_reverse(slot_id, symbol, from_side, to_side, price, reason):
    return (
        f"\U0001f504 <b>反转</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 方向: {_esc(from_side)} → <b>{_esc(to_side)}</b>\n"
        f"├ 价格: <code>{price:.4f}</code>\n"
        f"└ 原因: {_esc(reason)}"
    )


# -- 风控退出 --

def fmt_risk_exit(slot_id, symbol, side, price, reason, pnl=0):
    return (
        f"⚠️ <b>风控退出</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>  {_esc(side)}\n"
        f"├ 价格: <code>{price:.4f}</code>  盈亏: {_pnl(pnl)}\n"
        f"└ 触发: {_esc(reason)}"
    )


# -- 日亏损熔断 --

def fmt_daily_trip(slot_id, symbol, daily_pnl, loss_limit):
    return (
        f"\U0001f6d1 <b>日亏损熔断</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 今日盈亏: {_pnl(daily_pnl)} USDT\n"
        f"└ 熔断线: {loss_limit * 100:.1f}%  已触发，今日暂停交易"
    )


# -- 策略启动 --

def fmt_strategy_start(slot_id, symbol, leverage, pos_pct):
    return (
        f"\U0001f916 <b>策略启动</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 杠杆: {leverage}x\n"
        f"└ 仓位: {pos_pct * 100:.0f}%"
    )


# -- 孤儿持仓 --

def fmt_orphan_alert(slot_id, symbol, side, entry_px, held_sec):
    return (
        f"\U0001f480 <b>孤儿持仓告警</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>  {_esc(side)}\n"
        f"├ 入场: <code>{entry_px:.4f}</code>  持仓: {_held(held_sec)}\n"
        f"└ 策略已断连超过宽限期，已自动平仓"
    )


# -- 状态条 --

def _bar(pnl_pct):
    if pnl_pct >= 0:
        n = min(int(pnl_pct * 500), 10)
        return "\U0001f7e9" * n + "⬜" * (10 - n)
    else:
        n = min(int(abs(pnl_pct) * 500), 10)
        return "\U0001f7e5" * n + "⬜" * (10 - n)
