# -*- coding: utf-8 -*-
"""
shared/log.py -- 日志配置模块
===============================

功能
----
setup_logging(name, level) -> logging.Logger
为指定模块名称创建统一格式的日志记录器。

输出目标
--------
仅写入文件 /root/nt-base/logs/{name}.log（按文件大小轮转）。
不添加 StreamHandler（控制台输出），原因：
- 服务由 systemd 管理，stdout 已被 systemd 重定向到 journal
- 若同时添加 StreamHandler，每条日志会输出两次
- 多输出会导致日志中的 ERROR/WARNING 行数翻倍，干扰监控

日志格式
--------
ISO8601 时间戳 [模块名] 级别 消息正文

日志级别
--------
默认 INFO。对第三方库（asyncio, asyncpg, httpx, httpcore）自动降为 WARNING，
避免底层库的调试日志干扰主流程可读性。

作者: nt-base system
版本: 1.0.0
"""
from __future__ import annotations
"""Standard logging setup. Call setup_logging(name) in every entrypoint."""


import logging
import sys
from pathlib import Path

LOG_DIR = Path("/root/nt-base/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

_FMT = "%(asctime)s [%(name)s] %(levelname)s %(message)s"


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    root = logging.getLogger()
    if root.handlers:
        return logging.getLogger(name)

    root.setLevel(level)

    # No StreamHandler: systemd redirects stdout to the same log file,
    # so StreamHandler would cause every log line to appear twice.
    fh = logging.FileHandler(LOG_DIR / f"{name}.log")
    fh.setFormatter(logging.Formatter(_FMT))
    root.addHandler(fh)

    # Quiet noisy libs
    for noisy in ("asyncio", "asyncpg", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(name)
