"""
Path B 命令处理 — 通过 gRPC ControlStream 推送指令到策略客户端。
"""
from __future__ import annotations
import logging, time
import trading_base_pb2 as pb
from base.notify import _esc
from base.telegram.bot import register_handler

logger = logging.getLogger("telegram.control")
TYPE = pb.ControlCommand


def _build(cmd_type, reason="", params=None):
    return pb.ControlCommand(
        type=cmd_type, reason=reason,
        params=dict(params or {}),
        ts_ns=int(time.time() * 1_000_000_000),
    )


def _push(bot, target, cmd):
    grpc = bot._grpc
    if not grpc:
        return False, "⚠️ gRPC 服务未就绪"
    if target not in grpc._strategies:
        return False, f"⚠️ 策略 {_esc(target)} 未注册"
    if not grpc.push_control(target, cmd):
        return False, "⚠️ 指令队列满，发送失败"
    return True, ""


# -- /pauseme --

@register_handler("pauseme")
async def handle_pauseme(bot, args, level, chat_id):
    if level < 1:
        return "\U0001f6ab 无权限"
    ok, err = _push(bot, bot.instance_id, _build(TYPE.PAUSE, "用户暂停"))
    if ok:
        return f"⏸️ <b>已暂停</b>  {_esc(bot.instance_id)}\n└ 策略将停止开新仓，已有仓位继续管理"
    return err


# -- /resumeme --

@register_handler("resumeme")
async def handle_resumeme(bot, args, level, chat_id):
    if level < 1:
        return "\U0001f6ab 无权限"
    ok, err = _push(bot, bot.instance_id, _build(TYPE.RESUME, "用户恢复"))
    if ok:
        return f"▶️ <b>已恢复</b>  {_esc(bot.instance_id)}\n└ 策略将恢复正常开仓"
    return err


# -- /pause --

@register_handler("pause")
async def handle_pause(bot, args, level, chat_id):
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    target = args.get("target", bot.instance_id)
    ok, err = _push(bot, target, _build(TYPE.PAUSE, "管理员暂停"))
    if ok:
        return f"⏸️ <b>已暂停</b>  {_esc(target)}\n└ 管理员操作，策略停止开新仓"
    return err


# -- /resume --

@register_handler("resume")
async def handle_resume(bot, args, level, chat_id):
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    target = args.get("target", bot.instance_id)
    ok, err = _push(bot, target, _build(TYPE.RESUME, "管理员恢复"))
    if ok:
        return f"▶️ <b>已恢复</b>  {_esc(target)}\n└ 管理员操作，策略恢复正常"
    return err


# -- /adj --

@register_handler("adj")
async def handle_adj(bot, args, level, chat_id):
    if level < 1:
        return "\U0001f6ab 无权限"
    params = {k: v for k, v in args.items() if k != "target"}
    if not params:
        return "\U0001f527 用法: <code>/adj 参数名 参数值</code>\n示例: <code>/adj signal_threshold 0.35</code>"
    target = args.get("target", bot.instance_id)
    if level < 2 and target != bot.instance_id:
        return "\U0001f6ab L1 只能调整自己的策略，请使用 <code>/adj 参数 值</code>"
    ok, err = _push(bot, target, _build(TYPE.ADJUST_PARAM, "参数调整", {k: str(v) for k, v in params.items()}))
    if ok:
        lines = [f"\U0001f527 <b>参数调整</b>  {_esc(target)}"]
        for k, v in params.items():
            lines.append(f"├ {_esc(k)}: → <code>{_esc(v)}</code>")
        lines.append("└ 已推送至策略客户端，热加载生效")
        return "\n".join(lines)
    return err


# -- /ping --

@register_handler("ping")
async def handle_ping(bot, args, level, chat_id):
    if level < 2:
        return "\U0001f6ab 无权限"
    ok, err = _push(bot, bot.instance_id, _build(TYPE.PING, "连通测试"))
    if ok:
        return f"\U0001f4e1 <b>连通测试</b>\n└ ControlStream → {_esc(bot.instance_id)}: ✅ 指令已送达"
    return err
