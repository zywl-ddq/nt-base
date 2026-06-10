# -*- coding: utf-8 -*-
"""
============================================================================
模块:    base/atr
用途:    ATR (Average True Range) 计算工具
============================================================================

提供标准 True Range 公式的 ATR 计算：
  True Range = max(high - low, abs(high - prev_close), abs(prev_close - low))
  ATR       = mean(True Range[-period:])

与旧实现的区别：
  - 旧: mean(high - low) -- 仅使用 bar 柱体高度，遗漏 gap 成分
  - 新: mean(True Range) -- 包含 bar 间跳空，更准确反映真实波动率

使用场景：
  - main.py 中每根 1m bar 计算一次，同步给 TickExitManager、RiskLoop、策略端
  - 替代 trading-v2 ExitManager 中原有的 compute_atr() 重复计算

作者:    nt-base system
版本:    1.0.0
============================================================================
"""
from __future__ import annotations

import numpy as np


def compute_atr(bar_buffer: list[dict], period: int = 30) -> float:
    """计算最近 period 根 bar 的平均真实波幅 (ATR)。

    算法:
      对每根 bar（从第 2 根开始需要前一 bar 的 close）:
        tr = max(
            high - low,              # 当前 bar 的柱体高度
            abs(high - prev_close),  # 当前最高价相对前收的跳空
            abs(prev_close - low),   # 当前最低价相对前收的跳空
        )
      ATR = mean(tr[-period:])

    Args:
        bar_buffer: bar 数据列表，每个元素为 dict，包含 close/high/low 键
        period: 计算周期 (默认 30，即 30 根 1m bar)

    Returns:
        float: ATR 值。bar 数量不足 2 时返回 0.0。

    注意:
      - 此函数依赖 numpy 计算均值，调用方需确保已 import numpy
      - bar_buffer 按时间升序排列 (旧->新)，取最后 period 根
    """
    n = min(len(bar_buffer), period)
    if n < 2:
        return 0.0

    buf = list(bar_buffer)[-n:]

    tr_values = []
    prev_close = float(buf[0]['close'])

    for b in buf[1:]:
        high = float(b['high'])
        low = float(b['low'])
        close = float(b['close'])

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(prev_close - low),
        )
        tr_values.append(tr)
        prev_close = close

    if not tr_values:
        return 0.0

    return float(np.mean(tr_values))
