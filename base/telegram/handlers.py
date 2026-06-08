"""
Path A 命令处理 — 直接操作 nt-base（不经过策略客户端）。
"""
from __future__ import annotations
import logging, time
from base.notify import _esc

logger = logging.getLogger("telegram.handlers")


async def handle_status(bot, args, level, chat_id):
    sid = bot.instance_id
    reg = bot._registry
    slots = reg.get_slots("SOLUSDT-PERP", "1m") if reg else []
    lines = [f"\U0001f4ca <b>{_esc(sid)}</b>", ""]

    for slot in slots:
        if slot.strategy_id != sid:
            continue
        if slot.tripped:
            lines.append("\U0001f6d1 状态: <b>已熔断</b>（日亏损超限）")
        elif slot.has_position:
            held = f"{slot.held_sec:.0f}秒" if slot.held_sec < 60 else f"{slot.held_sec / 60:.1f}分"
            lines.append(f"\U0001f4c8 持仓: <b>{_esc(slot.entry_side)}</b>  入场价 {slot.entry_price:.4f}  已持 {held}")
        else:
            lines.append("\U0001f3c1 持仓: <b>空仓</b>")
        if slot.last_trade_time > 0:
            lines.append(f"⏱️ 上次交易: {time.time() - slot.last_trade_time:.0f}秒前")
        break

    grpc_info = bot._grpc._strategies.get(sid) if bot._grpc else None
    if grpc_info:
        disc = grpc_info.get("disconnected_at")
        if disc:
            ago = time.time() - disc
            lines.append(f"\U0001f4e1 gRPC: <b>已断连</b> ({ago:.0f}秒前)")
        else:
            lines.append("\U0001f4e1 gRPC: <b>已连接</b>")
    else:
        lines.append("\U0001f4e1 gRPC: 未注册")

    return "\n".join(lines)


async def handle_flatme(bot, args, level, chat_id):
    if level < 1:
        return "\U0001f6ab 无权限"
    return await _do_flat(bot, bot.instance_id)


async def handle_flat(bot, args, level, chat_id):
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    target = args.get("target", bot.instance_id)
    return await _do_flat(bot, target)


async def handle_flat_all(bot, args, level, chat_id):
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    exc = bot._executor
    reg = bot._registry
    if not exc or not reg:
        return "⚠️ 执行器未就绪"
    slots = reg.get_slots("SOLUSDT-PERP", "1m")
    if not slots:
        return "\U0001f4ca 无活跃策略"
    results = []
    for slot in slots:
        if slot.has_position:
            ok = exc.flat(slot, "operator_flat_all")
            icon = "✅" if ok else "❌"
            results.append(f"  {icon} {_esc(slot.strategy_id)}: {_esc(slot.entry_side)} → 已平仓")
        else:
            results.append(f"  \U0001f3c1 {_esc(slot.strategy_id)}: 空仓")
    return "\U0001f3c1 <b>全部平仓</b>\n" + "\n".join(results)


async def handle_status_all(bot, args, level, chat_id):
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    grpc = bot._grpc
    reg = bot._registry
    lines = ["\U0001f4ca <b>全局状态</b>", ""]
    if grpc:
        for sid, info in grpc._strategies.items():
            disc = info.get("disconnected_at")
            icon = "\U0001f4e1" if disc else "✅"
            status = "已断连" if disc else "在线"
            lines.append(f"  {icon} <b>{_esc(sid)}</b> [{status}]")
    if reg:
        for slot in reg.get_slots("SOLUSDT-PERP", "1m"):
            icon = "\U0001f4c8" if slot.has_position else "\U0001f3c1"
            has_pos = "持仓中" if slot.has_position else "空仓"
            lines.append(f"  {icon} {_esc(slot.strategy_id)} [{has_pos}]")
    if len(lines) == 2:
        lines.append("  （无注册策略）")
    return "\n".join(lines)


async def _do_flat(bot, target_id):
    exc = bot._executor
    reg = bot._registry
    if not exc or not reg:
        return "⚠️ 执行器未就绪"
    slots = reg.get_slots("SOLUSDT-PERP", "1m")
    for slot in slots:
        if slot.strategy_id == target_id:
            if not slot.has_position:
                return f"\U0001f3c1 {_esc(target_id)} 当前空仓，无需平仓"
            side = slot.entry_side
            price = slot.entry_price
            ok = exc.flat(slot, "operator_flat")
            if ok:
                return (
                    f"\U0001f3c1 <b>平仓完成</b>  {_esc(target_id)}\n"
                    f"┌ 方向: {_esc(side)}  入场价: {price:.4f}\n"
                    f"└ 状态: ✅ 已执行"
                )
            else:
                return f"❌ {_esc(target_id)} 平仓失败"
    return f"⚠️ 未找到策略 {_esc(target_id)}"


from base.telegram.bot import register_handler

@register_handler("status")
async def _s(bot, a, l, c): return await handle_status(bot, a, l, c)

@register_handler("flatme")
async def _fm(bot, a, l, c): return await handle_flatme(bot, a, l, c)

@register_handler("flat")
async def _f(bot, a, l, c): return await handle_flat(bot, a, l, c)

@register_handler("flat_all")
async def _fa(bot, a, l, c): return await handle_flat_all(bot, a, l, c)

@register_handler("status_all")
async def _sa(bot, a, l, c): return await handle_status_all(bot, a, l, c)
