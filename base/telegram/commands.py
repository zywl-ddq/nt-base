"""
Command parsing and permission checking for Telegram strategy bots.

Permission Levels:
  0 = none       (stranger — rejected)
  1 = operator   (strategy's own chat_id — can operate own strategy)
  2 = admin      (global admin from .env — can operate any strategy)
"""
from __future__ import annotations


def parse_command(text: str) -> tuple[str, dict[str, str]]:
    """
    Parse a structured command into (name, kwargs).

    Supported formats:
      /flat                          → ("flat", {})
      /flat AlphaV2-005              → ("flat", {"target": "AlphaV2-005"})
      /adj threshold 0.35            → ("adj", {"threshold": "0.35"})
      /adj stop_pct 0.02 leverage 2  → ("adj", {"stop_pct": "0.02", "leverage": "2"})
    """
    parts = text.strip().split()
    cmd = parts[0].lstrip("/").lower() if parts else ""
    args = parts[1:]

    kwargs: dict[str, str] = {}
    if cmd in ("flat",) and args:
        kwargs["target"] = args[0]
    elif cmd in ("adj", "adjust"):
        # Parse key-value pairs: /adj key1 val1 key2 val2
        i = 0
        while i + 1 < len(args):
            kwargs[args[i]] = args[i + 1]
            i += 2
    elif cmd in ("flat_all", "status_all"):
        pass  # no positional args
    elif cmd in ("flatme", "status", "pauseme", "resumeme", "mystatus"):
        pass  # no args
    elif cmd in ("pause", "resume", "deactivate", "activate"):
        if args:
            kwargs["target"] = args[0]

    return cmd, kwargs


def check_permission(chat_id: str, operator_chat_id: str, admin_chat_id: str) -> int:
    """
    Determine permission level for a chat_id.

    Returns: 0 = no permission, 1 = operator, 2 = admin
    """
    if chat_id == admin_chat_id:
        return 2
    if chat_id == operator_chat_id:
        return 1
    return 0
