"""
Telegram 通知模块 — 中文消息 + emoji 图标，HTML 格式。

图标体系:
  🟢 做多开仓 / 🔴 做空开仓 / ✅ 盈利平仓 / ❌ 亏损平仓
  ⚠️ 风控退出 / 🛑 日亏损熔断 / 🔄 反转开仓 / ⏱️ 时间衰减退出
  🤖 策略启动 / 💀 状态 / 🟩 平仓

所有通知消息使用 HTML parse_mode 发送，支持 <b>、<code> 等标签。
消息格式使用 Unicode 制表符（┌ ├ └）构造树型结构，层次清晰。

Author:    nt-base system
Version:   1.0.0
"""
from __future__ import annotations

import asyncio, json, logging, urllib.request, html as _html

logger = logging.getLogger(__name__)
# Telegram Bot API 的 sendMessage 接口 URL 模板
# {token} 会被替换为实际的 Bot Token
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _send_sync(token, chat_id, text):
    """
    同步发送 Telegram 消息的内部函数。

    使用 urllib.request（标准库，无外部依赖）发送 POST 请求到 Telegram API。
    消息以 JSON 格式发送，启用 HTML parse_mode 并且禁用网页预览。

    参数:
        token:   Telegram Bot Token（字符串）
        chat_id: 目标聊天 ID（整数）
        text:    消息文本（HTML 格式字符串）

    返回:
        bool: True 表示发送成功（HTTP 200），False 表示发送失败。

    异常处理:
        捕获所有异常（网络超时、连接拒绝等），记录 WARNING 日志，
        不会向上抛出异常，确保通知发送失败不影响主业务流程。
    """
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
    """
    异步发送 Telegram 消息。

    将同步的 _send_sync 调用委托给线程池执行器（run_in_executor），
    避免阻塞事件循环。这是异步代码调用同步阻塞 I/O 的标准模式。

    参数:
        token:   Telegram Bot Token（字符串）
        chat_id: 目标聊天 ID（整数）
        text:    消息文本（HTML 格式字符串）

    安全防护:
        如果 token 或 chat_id 为空，直接返回（静默跳过），
        避免在配置不完整时抛出异常。

    注意:
        使用 asyncio.get_running_loop() 获取当前事件循环，
        因此必须在异步上下文中调用本函数。
    """
    if not token or not chat_id:
        return
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _send_sync, token, chat_id, text)
    except Exception as e:
        logger.warning(f"send_message failed: {e}")


def _esc(s):
    """
    HTML 转义辅助函数。

    对字符串中的 HTML 特殊字符（< > &）进行转义，防止 XSS 或格式破坏。
    同时对 None 值提供安全的默认显示 "-"。

    参数:
        s: 原始字符串（可能为 None）

    返回:
        str: HTML 转义后的字符串，None 输入返回 "-"
    """
    if s is None: return "-"
    return _html.escape(str(s), quote=False)


def _pnl(p):
    """
    盈亏数值格式化辅助函数。

    使用 <b> 标签加粗显示，带符号（+/-），保留 4 位小数。

    参数:
        p: 盈亏数值（浮点数）

    返回:
        str: HTML 格式的盈亏文本，如 <b>+12.3456</b> 或 <b>-3.1415</b>
    """
    return f"<b>{p:+.4f}</b>"

def _held(sec):
    """
    持仓时间格式化辅助函数。

    将秒数转换为"X分Y秒"或"Y秒"的中文可读格式。

    参数:
        sec: 持仓秒数（整数或浮点数）

    返回:
        str: 格式化后的持仓时间文本，如 "5分30秒" 或 "45秒"
    """
    m, s = divmod(int(sec), 60)
    return f"{m}分{s}秒" if m > 0 else f"{s}秒"


# -- 入场通知 --

def fmt_entry(slot_id, symbol, side, price, qty, notional, reason):
    """
    开仓通知消息格式化。

    根据多空方向选择不同的 emoji 图标：
      - LONG: 🟢 做多 （绿色圆圈）
      - SHORT: 🔴 做空（红色圆圈）

    消息结构（树型格式）：
      第1行: [图标] 开仓 [策略槽ID]
      第2行: ┌ 品种: [品种代码]
      第3行: ├ 方向: [LONG/SHORT]
      第4行: ├ 价格: [入场价格]
      第5行: ├ 数量: [数量]  名义价值: [价值] USDT
      第6行: └ 原因: [开仓原因描述]

    参数:
        slot_id:  策略槽 ID（字符串），标识哪个策略实例
        symbol:   交易品种代码，如 SOLUSDT
        side:     交易方向，LONG（做多）或 SHORT（做空）
        price:    入场价格（浮点数）
        qty:      开仓数量（浮点数）
        notional: 名义价值 = 数量 * 价格（浮点数），单位 USDT
        reason:   开仓原因描述（字符串）

    返回:
        str: HTML 格式的通知消息
    """
    icon = "\U0001f7e2 做多" if side == "LONG" else "\U0001f534 做空"
    return (
        f"{icon} <b>开仓</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 方向: <b>{_esc(side)}</b>\n"
        f"├ 价格: <code>{price:.4f}</code>\n"
        f"├ 数量: {qty:.3f}  名义价值: {notional:.2f} USDT\n"
        f"└ 原因: {_esc(reason)}"
    )


# -- 平仓通知 --

def fmt_close(slot_id, symbol, side_was, entry_px, exit_px, pnl, held_sec, reason):
    """
    平仓通知消息格式化。

    根据盈亏正负选择不同的图标：
      - pnl >= 0: ✅ 盈利平仓（绿色勾）
      - pnl < 0:  ❌ 亏损平仓（红色叉）

    消息结构：
      第1行: [图标] [策略槽ID]
      第2行: ┌ 品种: [品种代码] [原方向]
      第3行: ├ 入场: [入场价格]  →  出场: [出场价格]
      第4行: ├ 盈亏: [+/-数值] USDT
      第5行: ├ 持仓: [时长]
      第6行: └ 原因: [平仓原因描述]

    参数:
        slot_id:  策略槽 ID
        symbol:   交易品种代码
        side_was: 平仓前的持仓方向（LONG/SHORT）
        entry_px: 入场价格（浮点数）
        exit_px:  出场价格（浮点数）
        pnl:      盈亏金额（浮点数，正数为盈利，负数为亏损），单位 USDT
        held_sec: 持仓时长（秒数）
        reason:   平仓原因描述

    返回:
        str: HTML 格式的通知消息
    """
    icon = "✅ 盈利平仓" if pnl >= 0 else "❌ 亏损平仓"
    return (
        f"{icon}  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>  {_esc(side_was)}\n"
        f"├ 入场: <code>{entry_px:.4f}</code>  →  出场: <code>{exit_px:.4f}</code>\n"
        f"├ 盈亏: {_pnl(pnl)} USDT\n"
        f"├ 持仓: {_held(held_sec)}\n"
        f"└ 原因: {_esc(reason)}"
    )


# -- 反转通知 --

def fmt_reverse(slot_id, symbol, from_side, to_side, price, reason):
    """
    反转开仓通知消息格式化。

    当策略信号从多转空或从空转多时发送此通知。
    使用 🔄（反转）emoji 标识。

    消息结构：
      第1行: 🔄 反转 [策略槽ID]
      第2行: ┌ 品种: [品种代码]
      第3行: ├ 方向: [原方向] → [新方向]
      第4行: ├ 价格: [当前价格]
      第5行: └ 原因: [反转原因]

    参数:
        slot_id:  策略槽 ID
        symbol:   交易品种代码
        from_side: 原持仓方向（LONG/SHORT）
        to_side:   新持仓方向（LONG/SHORT）
        price:     反转时的市场价格
        reason:    反转原因描述

    返回:
        str: HTML 格式的通知消息
    """
    return (
        f"\U0001f504 <b>反转</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 方向: {_esc(from_side)} → <b>{_esc(to_side)}</b>\n"
        f"├ 价格: <code>{price:.4f}</code>\n"
        f"└ 原因: {_esc(reason)}"
    )


# -- 风控退出通知 --

def fmt_risk_exit(slot_id, symbol, side, price, reason, pnl=0):
    """
    风控退出通知消息格式化。

    当风控系统（止损、止盈、持仓时间限制等）强制平仓时发送。
    使用 ⚠️（警告）emoji 标识风险事件。

    消息结构：
      第1行: ⚠️ 风控退出 [策略槽ID]
      第2行: ┌ 品种: [品种代码] [方向]
      第3行: ├ 价格: [退出价格]  盈亏: [+/-数值]
      第4行: └ 触发: [风控原因描述]

    参数:
        slot_id:  策略槽 ID
        symbol:   交易品种代码
        side:     退出时的持仓方向（LONG/SHORT）
        price:    退出价格（浮点数）
        reason:   风控触发原因，如"止损触发"、"持仓超时"等
        pnl:      盈亏金额（浮点数，可选，默认 0）

    返回:
        str: HTML 格式的通知消息
    """
    return (
        f"⚠️ <b>风控退出</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>  {_esc(side)}\n"
        f"├ 价格: <code>{price:.4f}</code>  盈亏: {_pnl(pnl)}\n"
        f"└ 触发: {_esc(reason)}"
    )


# -- 日亏损熔断通知 --

def fmt_daily_trip(slot_id, symbol, daily_pnl, loss_limit):
    """
    日亏损熔断通知消息格式化。

    当当日亏损达到设定比例上限时触发熔断，发送此通知。
    使用 🛑（禁止标志）emoji 标识熔断事件。
    熔断后该策略今日将暂停所有交易。

    消息结构：
      第1行: 🛑 日亏损熔断 [策略槽ID]
      第2行: ┌ 品种: [品种代码]
      第3行: ├ 今日盈亏: [+/-数值] USDT
      第4行: └ 熔断线: [比例]%  已触发，今日暂停交易

    参数:
        slot_id:   策略槽 ID
        symbol:    交易品种代码
        daily_pnl: 当日累计盈亏（浮点数），单位 USDT
        loss_limit: 熔断触发比例阈值（浮点数，如 0.05 表示 5%）

    返回:
        str: HTML 格式的通知消息
    """
    return (
        f"\U0001f6d1 <b>日亏损熔断</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 今日盈亏: {_pnl(daily_pnl)} USDT\n"
        f"└ 熔断线: {loss_limit * 100:.1f}%  已触发，今日暂停交易"
    )


# -- 策略启动通知 --

def fmt_strategy_start(slot_id, symbol, leverage, pos_pct):
    """
    策略启动通知消息格式化。

    当策略实例成功加载并开始运行时发送此通知。
    使用 🤖（机器人）emoji 标识策略启动事件。

    消息结构：
      第1行: 🤖 策略启动 [策略槽ID]
      第2行: ┌ 品种: [品种代码]
      第3行: ├ 杠杆: [倍数]x
      第4行: └ 仓位: [百分比]%

    参数:
        slot_id:   策略槽 ID
        symbol:    交易品种代码
        leverage:  杠杆倍数（整数），如 3
        pos_pct:   仓位比例（浮点数，0~1），如 0.2 表示 20%

    返回:
        str: HTML 格式的通知消息
    """
    return (
        f"\U0001f916 <b>策略启动</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>\n"
        f"├ 杠杆: {leverage}x\n"
        f"└ 仓位: {pos_pct * 100:.0f}%"
    )


# -- 孤儿持仓告警通知 --

def fmt_orphan_alert(slot_id, symbol, side, entry_px, held_sec):
    """
    孤儿持仓告警通知消息格式化。

    当策略客户端（trading-v2）与 nt-base 的 gRPC 连接断开超过宽限期，
    系统自动平仓时发送此通知。持仓因策略断连而变为"孤儿"状态。
    使用 💀（骷髅）emoji 标识严重告警事件。

    消息结构：
      第1行: 💀 孤儿持仓告警 [策略槽ID]
      第2行: ┌ 品种: [品种代码] [方向]
      第3行: ├ 入场: [入场价格]  持仓: [时长]
      第4行: └ 策略已断连超过宽限期，已自动平仓

    参数:
        slot_id:  策略槽 ID
        symbol:   交易品种代码
        side:     持仓方向（LONG/SHORT）
        entry_px: 入场价格（浮点数）
        held_sec: 持仓时长（秒数）

    返回:
        str: HTML 格式的通知消息
    """
    return (
        f"\U0001f480 <b>孤儿持仓告警</b>  {_esc(slot_id)}\n"
        f"┌ 品种: <code>{_esc(symbol)}</code>  {_esc(side)}\n"
        f"├ 入场: <code>{entry_px:.4f}</code>  持仓: {_held(held_sec)}\n"
        f"└ 策略已断连超过宽限期，已自动平仓"
    )


# -- 状态条 --

def _bar(pnl_pct):
    """
    盈亏状态条（进度条/温度计）辅助函数。

    将盈亏比例可视化为一个 10 格的柱状条：
      - 盈利时使用绿色方块 🟩，百分比越高绿色方块越多。
      - 亏损时使用红色方块 🟥，百分比越高红色方块越多。
      - 未占用的位置用白方块 ⬜ 填充。

    每个方块代表的盈亏比例为 0.2%（1/500），
    因此 10 个方块满格对应 2% 的盈亏比例。

    参数:
        pnl_pct: 盈亏比例（浮点数），如 0.01 表示 +1%

    返回:
        str: 带 emoji 的进度条字符串

    示例:
        _bar(0.005)  → "🟩🟩🟩⬜⬜⬜⬜⬜⬜⬜"（+0.5%，2.5格≈3格）
        _bar(-0.01)  → "🟥🟥🟥🟥🟥⬜⬜⬜⬜⬜"（-1.0%，5格）
    """
    if pnl_pct >= 0:
        n = min(int(pnl_pct * 500), 10)
        return "\U0001f7e9" * n + "⬜" * (10 - n)
    else:
        n = min(int(abs(pnl_pct) * 500), 10)
        return "\U0001f7e5" * n + "⬜" * (10 - n)
