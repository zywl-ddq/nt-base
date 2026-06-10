"""
Path B 命令处理 — 通过 gRPC ControlStream 推送指令到策略客户端（trading-v2）。

与 Path A（handlers.py）的区别：
  - Path A 直接在 nt-base 本地执行操（平仓、查询）
  - Path B 通过 gRPC ControlStream 将指令推送到 trading-v2 策略客户端，
    由策略客户端处理暂停/恢复/参数调整等逻辑

ControlStream 是单向推送通道（nt-base -> trading-v2），
使用 protobuf 定义的 ControlCommand 消息格式。
"""
from __future__ import annotations
import logging, time
import trading_base_pb2 as pb
from base.notify import _esc
from base.telegram.bot import register_handler

logger = logging.getLogger("telegram.control")
# Protobuf ControlCommand 消息类型别名，减少重复引用
TYPE = pb.ControlCommand


def _build(cmd_type, reason="", params=None):
    """
    构造 ControlCommand protobuf 消息。

    参数:
      cmd_type: ControlCommand 类型枚举（PAUSE, RESUME, ADJUST_PARAM, PING 等）
      reason: 操原因描述字符串
      params: 额外参数键值对字典（如 adj 的参数字典）
      ts_ns: 当前时间戳（纳秒精度），用于消息排序和去重

    返回:
      pb.ControlCommand 实例
    """
    return pb.ControlCommand(
        type=cmd_type, reason=reason,
        params=dict(params or {}),
        ts_ns=int(time.time() * 1_000_000_000),
    )


def _push(bot, target, cmd):
    """
    将 ControlCommand 推送到目标策略的 ControlStream。

    流程：
      1. 检查 gRPC 服务端是否就绪（bot._grpc）
      2. 检查目标策略是否已在 _strategies 中注册
      3. 调用 grpc.push_control() 将指令加入策略的推送队列
      4. 如果队列满（缓冲区溢出），返回失败

    参数:
      bot: StrategyBot 实例
      target: 目标策略实例ID
      cmd: pb.ControlCommand 消息

    返回:
      (True, "") 或 (False, "错误消息")
    """
    grpc = bot._grpc
    if not grpc:
        return False, "⚠️ gRPC 服务未就绪"
    if target not in grpc._strategies:
        return False, f"⚠️ 策略 {_esc(target)} 未注册"
    if not grpc.push_control(target, cmd):
        return False, "⚠️ 指令队列满，发送失败"
    return True, ""


# -- /pauseme（操作者：暂停自己的策略） --

@register_handler("pauseme")
async def handle_pauseme(bot, args, level, chat_id):
    """
    处理 /pauseme 命令。

    权限要求：L1（操者）及以上
    效果：向目标策略推送 PAUSE 指令，策略停止开新仓，已有仓位继续管理。
    目标自动设为 bot.instance_id（操者自己的策略），不可指定其他策略。
    """
    if level < 1:
        return "\U0001f6ab 无权限"
    ok, err = _push(bot, bot.instance_id, _build(TYPE.PAUSE, "用户暂停"))
    if ok:
        return f"⏸️ <b>已暂停</b>  {_esc(bot.instance_id)}\n└ 策略将停止开新仓，已有仓位继续管理"
    return err


# -- /resumeme（操作者：恢复自己的策略） --

@register_handler("resumeme")
async def handle_resumeme(bot, args, level, chat_id):
    """
    处理 /resumeme 命令。

    权限要求：L1（操者）及以上
    效果：向目标策略推送 RESUME 指令，策略恢复正常开仓。
    /pauseme 的逆操。
    """
    if level < 1:
        return "\U0001f6ab 无权限"
    ok, err = _push(bot, bot.instance_id, _build(TYPE.RESUME, "用户恢复"))
    if ok:
        return f"▶️ <b>已恢复</b>  {_esc(bot.instance_id)}\n└ 策略将恢复正常开仓"
    return err


# -- /pause（管理员：暂停指定策略） --

@register_handler("pause")
async def handle_pause(bot, args, level, chat_id):
    """
    处理 /pause 命令。

    权限要求：L2（管理员）及以上
    允许指定目标策略ID，不指定时默认为当前Bot绑定的策略。
    管理员可以暂停任意已注册的策略。
    """
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    target = args.get("target", bot.instance_id)
    ok, err = _push(bot, target, _build(TYPE.PAUSE, "管理员暂停"))
    if ok:
        return f"⏸️ <b>已暂停</b>  {_esc(target)}\n└ 管理员操作，策略停止开新仓"
    return err


# -- /resume（管理员：恢复指定策略） --

@register_handler("resume")
async def handle_resume(bot, args, level, chat_id):
    """
    处理 /resume 命令。

    权限要求：L2（管理员）及以上
    /pause 的逆操，管理员可以恢复任意已注册的策略。
    """
    if level < 2:
        return "\U0001f6ab 无权限，仅管理员可操作"
    target = args.get("target", bot.instance_id)
    ok, err = _push(bot, target, _build(TYPE.RESUME, "管理员恢复"))
    if ok:
        return f"▶️ <b>已恢复</b>  {_esc(target)}\n└ 管理员操作，策略恢复正常"
    return err


# -- /adj（热调整策略参数） --

@register_handler("adj")
async def handle_adj(bot, args, level, chat_id):
    """
    处理 /adj 命令 — 参数热调整。

    权限要求：
      - L1（操者）：只能调整自己的策略参数（不指定 target）
      - L2（管理员）：可以调整任意策略参数

    效果：将键值对参数封装为 ADJUST_PARAM 指令，通过 ControlStream
    推送到策略客户端。策略客户端收到后热加载参数，无需重启。

    示例:
      /adj signal_threshold 0.35          （操作者调整自己的策略）
      /adj AlphaV2-005 stop_pct 0.02      （管理员调整指定策略）
    """
    if level < 1:
        return "\U0001f6ab 无权限"
    # 从 args 中提取参数键值对，排除 target（如果存在）
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


# -- /ping（连通性测试） --

@register_handler("ping")
async def handle_ping(bot, args, level, chat_id):
    """
    处理 /ping 命令 — 连通性测试。

    权限要求：L2（管理员）
    效果：向目标策略推送 PING 指令，验证 ControlStream 是否通畅。
    策略客户端收到 PING 后通常会回复 PONG，可用于诊断 gRPC 连接状态。
    """
    if level < 2:
        return "\U0001f6ab 无权限"
    ok, err = _push(bot, bot.instance_id, _build(TYPE.PING, "连通测试"))
    if ok:
        return f"\U0001f4e1 <b>连通测试</b>\n└ ControlStream → {_esc(bot.instance_id)}: ✅ 指令已送达"
    return err
