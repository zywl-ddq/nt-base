"""
Path A 命令处理 — 直接操作 nt-base（不经过策略客户端 trading-v2）。

与 Path B（control.py）的区别：
  - Path A 直接在 nt-base 进程内执行操，通过 OrderExecutor 平仓、
    通过 StrategyRegistry 查询状态，不依赖 gRPC ControlStream
  - Path B 通过 gRPC 将指令推送到 trading-v2 策略客户端

Path A 处理以下命令：
  - status: 查询单个策略状态（持仓方向、入场价、持有时长、gRPC连接状态）
  - flatme: 操作者平自己的仓
  - flat: 管理员平指定策略
  - flat_all: 管理员全平所有策略
  - status_all: 全局状态概览
"""
from __future__ import annotations
import logging, time
from base.notify import _esc

logger = logging.getLogger("telegram.handlers")


async def handle_status(bot, args, level, chat_id):
    """
    查询单个策略的运行时状态。

    返回信息：
      1. 熔断状态（日亏损超限时显示）
      2. 持仓信息：方向、入场价、持有时长（秒/分钟）
      3. 上次交易距现在的秒数
      4. gRPC 连接状态（已连接/已断连+秒数/未注册）

    通过 bot._registry.get_slots() 获取策略槽位信息，
    通过 bot._grpc._strategies 获取 gRPC 连接状态。
    """
    sid = bot.instance_id
    reg = bot._registry
    slots = reg.get_slots("SOLUSDT-PERP", "1m") if reg else []
    lines = [f"\U0001f4ca <b>{_esc(sid)}</b>", ""]

    for slot in slots:
        if slot.strategy_id != sid:
            continue
        if slot.tripped:
            # 日亏损超过熔断阈值，策略已自动停止开仓
            lines.append("\U0001f6d1 状态: <b>已熔断</b>（日亏损超限）")
        elif slot.has_position:
            # 有持仓：显示方向、入场价、持有时长
            held = f"{slot.held_sec:.0f}秒" if slot.held_sec < 60 else f"{slot.held_sec / 60:.1f}分"
            lines.append(f"\U0001f4c8 持仓: <b>{_esc(slot.entry_side)}</b>  入场价 {slot.entry_price:.4f}  已持 {held}")
        else:
            lines.append("\U0001f3c1 持仓: <b>空仓</b>")
        if slot.last_trade_time > 0:
            lines.append(f"⏱️ 上次交易: {time.time() - slot.last_trade_time:.0f}秒前")
        break

    # 查询 gRPC 连接状态
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
    """
    处理 /flatme 命令 — 操作者平掉自己的仓位。

    权限要求：L1（操者）及以上
    目标自动设为 bot.instance_id，不可指定其他策略。
    实际平仓逻辑委托给 _do_flat。
    """
    if level < 1:
        return "\U0001f6ab 无权限"
    return await _do_flat(bot, bot.instance_id)


async def handle_flat(bot, args, level, chat_id):
    """
    处理 /flat 命令 — 管理员平掉指定策略的仓位。

    权限要求：L2（管理员）及以上
    允许指定目标策略ID，不指定时默认为当前Bot绑定的策略。
    实际平仓逻辑委托给 _do_flat。
    """
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    target = args.get("target", bot.instance_id)
    return await _do_flat(bot, target)


async def handle_flat_all(bot, args, level, chat_id):
    """
    处理 /flat_all 命令 — 管理员平掉所有策略的仓位。

    权限要求：L2（管理员）及以上
    遍历所有策略槽位，对有持仓的逐个执行平仓。
    返回每条策略的执行结果（成功/失败/空仓）。
    """
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
    """
    处理 /status_all 命令 — 全局状态概览。

    权限要求：L2（管理员）及以上
    返回信息：
      1. 所有已注册策略的 gRPC 连接状态（在线/已断连）
      2. 所有策略槽位的持仓状态（持仓中/空仓）

    从 grpc._strategies 获取连接信息，
    从 registry.get_slots() 获取策略槽位信息。
    """
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
    """
    平仓执行逻辑（内部函数，不直接绑定命令）。

    流程：
      1. 检查执行器和注册表是否就绪
      2. 在策略槽位列表中查找目标策略
      3. 如果已有仓位，调用 executor.flat() 执行平仓
      4. 如果空仓，提示无需平仓
      5. 如果未找到策略，提示未注册

    参数:
      bot: StrategyBot 实例
      target_id: 目标策略实例ID

    返回:
      HTML格式的回复文本
    """
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


# -- 命令注册 --
# 将 handle_* 函数通过 register_handler 装饰器绑定到对应命令名称。
# 这里使用简短的包装函数（_s, _fm, _f, _fa, _sa）而非直接装饰
# handle_* 函数，是为了保持函数命名清晰的同时支持注册别名。

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
