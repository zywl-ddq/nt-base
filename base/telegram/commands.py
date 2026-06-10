"""
Command parsing and permission checking for Telegram strategy bots.

Telegram命令解析与权限检查模块。

Permission Levels（三级权限模型）:
  0 = none       （陌生人/未授权 — 全部拒绝）
  1 = operator   （策略自己的 chat_id — 可操自己的策略）
  2 = admin      （全局管理员，从 .env 配置 — 可操任意策略）
"""
from __future__ import annotations


def parse_command(text: str) -> tuple[str, dict[str, str]]:
    """
    解析结构化命令文本，返回 (命令名, 参数字典)。

    支持的格式:
      /flat                          -> ("flat", {})
      /flat AlphaV2-005              -> ("flat", {"target": "AlphaV2-005"})
      /adj threshold 0.35            -> ("adj", {"threshold": "0.35"})
      /adj stop_pct 0.02 leverage 2  -> ("adj", {"stop_pct": "0.02", "leverage": "2"})

    解析规则：
      - 通用命令（flatme, status 等）：无参数，直接返回空字典
      - flat命令：第一个参数作为 target（策略ID）
      - adj命令：解析为 key1 val1 key2 val2 格式的键值对序列
      - pause/resume 命令：可选参数作为 target
      - flat_all/status_all：不接受参数
    """
    parts = text.strip().split()
    cmd = parts[0].lstrip("/").lower() if parts else ""
    args = parts[1:]

    kwargs: dict[str, str] = {}
    if cmd in ("flat",) and args:
        # /flat <target_id> —— 第一个参数为策略ID
        kwargs["target"] = args[0]
    elif cmd in ("adj", "adjust"):
        # /adj key1 val1 key2 val2 —— 成对解析键值
        i = 0
        while i + 1 < len(args):
            kwargs[args[i]] = args[i + 1]
            i += 2
    elif cmd in ("flat_all", "status_all"):
        pass  # 全局命令，不需要额外参数
    elif cmd in ("flatme", "status", "pauseme", "resumeme", "mystatus"):
        pass  # 自身操命令，不需要参数
    elif cmd in ("pause", "resume", "deactivate", "activate"):
        # 管理员命令，第一个参数为可选的策略ID
        if args:
            kwargs["target"] = args[0]

    return cmd, kwargs


def check_permission(chat_id: str, operator_chat_id: str, admin_chat_id: str) -> int:
    """
    判断给定 chat_id 的权限级别。

    实现：
      1. 如果等于 admin_chat_id -> 返回 2（管理员）
      2. 如果等于 operator_chat_id -> 返回 1（策略操者）
      3. 否则 -> 返回 0（无权限）

    注意：admin_chat_id 同时拥有 L2 权限，也会通过 L1 检查；
    但 L1 通道不会获得 L2 权限。业务逻辑在 handlers.py 和 control.py 中
    通过 level < 1 / level < 2 进行细粒度控制。
    """
    if chat_id == admin_chat_id:
        return 2
    if chat_id == operator_chat_id:
        return 1
    return 0
