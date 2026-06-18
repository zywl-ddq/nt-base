# -*- coding: utf-8 -*-
"""
base/sec_factors/cvd.py — Cumulative Volume Delta（成交量失衡）秒级因子
======================================================================
周期内累积主动买入量 - 主动卖出量（BUYER 流入为正，SELLER 流出为负），
compute 返回本周期净流入并清零累积器（即秒级 CVD 增量）。

示例因子（验证框架用），可替换/删除。
"""
from __future__ import annotations


class CVDFactor:
    def __init__(self, interval_sec: int = 1):
        self.name = "cvd"
        self.interval_sec = interval_sec
        self._flow: float = 0.0  # 周期内净流入累积

    def on_tick(self, tick) -> None:
        try:
            size = float(tick.size)
            if tick.aggressor_side.name == "BUYER":
                self._flow += size
            else:
                self._flow -= size
        except Exception:
            pass

    def on_book(self, books: dict) -> None:
        pass  # CVD 基于成交，不用盘口

    def compute(self) -> float:
        v = self._flow
        self._flow = 0.0  # 清零，下周期重新累积（输出的是周期增量）
        return v
