"""Standard logging setup. Call setup_logging(name) in every entrypoint."""

from __future__ import annotations

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

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(_FMT))
    root.addHandler(sh)

    fh = logging.FileHandler(LOG_DIR / f"{name}.log")
    fh.setFormatter(logging.Formatter(_FMT))
    root.addHandler(fh)

    # Quiet noisy libs
    for noisy in ("asyncio", "asyncpg", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(name)
