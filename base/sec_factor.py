# -*- coding: utf-8 -*-
"""
base/sec_factor.py — 秒级公共因子框架（注册制 / HOOK）
=====================================================

设计要点：
  1. 因子可插拔：register/unregister，每个因子声明自己的计算周期 interval_sec。
  2. 与 1m bar 对齐：样本按“产生时刻”的日历分钟分桶（ts_ns // 60e9）。
  3. 集成进分钟级数据流：aggregate() 把分钟桶内每因子的样本统计量（mean/std/
     min/max/skew/kurt）扁平化为 {name_{interval}s_{stat}: val}，由 main.py
     并入每根 1m Bar 的 factors map，随现有 gRPC Bar 流推送（proto 零改动）。

骨架照搬 risk/loop.py 的 RiskLoop（asyncio 单线程、_running 标志、start/stop）。
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Protocol

import numpy as np
from scipy import stats as sp_stats

logger = logging.getLogger(__name__)

NS_PER_SEC = 1_000_000_000
NS_PER_MIN = 60 * NS_PER_SEC
# 6 个统计量；样本不足时偏度/峰度填 0（protobuf float 不支持 NaN）
STATS = ("mean", "std", "min", "max", "skew", "kurt")


class SecFactor(Protocol):
    """秒级公共因子接口（结构化子类型，注册制）。

    每个因子只需实现这个“长得像”的接口，无需继承。框架通过 register 注册。
    """
    name: str               # 因子标识，如 "obi"（最终 bar 字段为 obi_3s_mean）
    interval_sec: int       # 计算周期（1/3/5...），每 N 秒 compute 一次

    def on_tick(self, tick) -> None:
        """每笔成交回调（可选，CVD 等累积型因子用）。"""
        ...

    def on_book(self, books: dict) -> None:
        """每周期前的 L2 快照回调（books = dm_actor._books，dict[sid, {bids,asks}]）。
        因子自行取自己 symbol 的盘口。"""
        ...

    def compute(self) -> float:
        """周期到达时返回当前因子值。"""
        ...


def summarize(xs: list[float]) -> dict[str, float]:
    """对样本列表算 6 个统计量；样本不足或 σ=0（全相同）时偏度/峰度填 0.0
    （scipy 对 σ=0 返回 nan，会破坏 protobuf 序列化与下游计算）。"""
    n = len(xs)
    if n == 0:
        return {s: 0.0 for s in STATS}
    arr = np.asarray(xs, dtype=float)
    std = float(arr.std()) if n >= 2 else 0.0
    # σ=0（全相同样本）或样本不足 → skew/kurt 无定义；计算后若仍为 nan 也归零
    if std == 0.0 or n < 3:
        skew = 0.0
    else:
        skew = float(sp_stats.skew(arr))
        if skew != skew:  # nan check
            skew = 0.0
    if std == 0.0 or n < 4:
        kurt = 0.0
    else:
        kurt = float(sp_stats.kurtosis(arr))
        if kurt != kurt:
            kurt = 0.0
    return {
        "mean": float(arr.mean()),
        "std": std,
        "min": float(arr.min()),
        "max": float(arr.max()),
        "skew": skew,
        "kurt": kurt,
    }


class SecFactorLoop:
    """每秒调度的秒级公共因子循环。

    用法（main.py）：
        loop = SecFactorLoop(interval=1.0)
        loop.register(OBIFactor(interval_sec=3, symbol="SOLUSDT-PERP"))
        loop.register(CVDFactor(interval_sec=1))
        loop.bind_books(lambda: dm_actor._books)
        asyncio.create_task(loop.start())
        # tick 回调里：loop.on_tick(tick)
        # 1m bar 到达时：factors.update(loop.aggregate(bar.ts_event))
    """

    def __init__(self, interval: float = 1.0):
        self._interval = interval
        self._running = False
        self._task = None
        self._factors: dict[str, SecFactor] = {}
        self._buckets: dict[int, dict[str, list[float]]] = {}
        self._tick_count = 0
        self._now_ns: int = 0
        self._get_books: Callable[[], dict] | None = None

    # ---- 注册制 ----
    def register(self, factor: SecFactor) -> None:
        self._factors[factor.name] = factor
        logger.info(f"SecFactor 注册: {factor.name} (interval={factor.interval_sec}s)")

    def unregister(self, name: str) -> None:
        self._factors.pop(name, None)
        logger.info(f"SecFactor 注销: {name}")

    def registered(self) -> list[str]:
        return list(self._factors.keys())

    def bind_books(self, get_books: Callable[[], dict]) -> None:
        self._get_books = get_books

    # ---- 数据入口（main.py tick 回调调用）----
    def on_tick(self, tick) -> None:
        """每笔成交：更新当前时间 + 分发给所有因子的 on_tick。"""
        try:
            self._now_ns = int(tick.ts_event)
        except Exception:
            self._now_ns = time.time_ns()
        for f in list(self._factors.values()):
            try:
                f.on_tick(tick)
            except Exception as e:
                logger.warning(f"SecFactor {f.name}.on_tick error: {e}")

    # ---- 单步调度（_run 调用 + 测试直接驱动，避免依赖真实 sleep）----
    def _tick(self, now_ns: int | None = None) -> None:
        self._tick_count += 1
        now = now_ns if now_ns is not None else (self._now_ns or time.time_ns())
        minute = now // NS_PER_MIN
        books = {}
        if self._get_books is not None:
            try:
                books = self._get_books() or {}
            except Exception as e:
                logger.warning(f"SecFactor 读 L2 失败: {e}")
        for name, f in list(self._factors.items()):
            interval = max(int(getattr(f, "interval_sec", 1)), 1)
            if self._tick_count % interval != 0:
                continue
            if books:
                try:
                    f.on_book(books)
                except Exception as e:
                    logger.warning(f"SecFactor {name}.on_book error: {e}")
            try:
                value = float(f.compute())
            except Exception as e:
                logger.warning(f"SecFactor {name}.compute error: {e}")
                continue
            self._buckets.setdefault(minute, {}).setdefault(name, []).append(value)

    # ---- 分钟桶聚合（main.py 1m bar 分支调用）----
    def aggregate(self, minute_ts_ns: int) -> dict[str, float]:
        """返回扁平 {name_{interval}s_{stat}: val}；聚合后删除该分钟桶。

        bar.ts_event 通常是 bar 结束时间（close），内容覆盖前一分钟；
        优先取 bar_minute-1，为空时回退 bar_minute（兼容 open-time 语义）。
        """
        bar_minute = int(minute_ts_ns) // NS_PER_MIN
        bucket = self._buckets.pop(bar_minute - 1, None)
        if bucket is None:
            bucket = self._buckets.pop(bar_minute, {})
        logger.info(
            f"[SecFactor] aggregate bar_min={bar_minute} "
            f"hit={ {k: len(v) for k, v in bucket.items()} }"
        )
        flat: dict[str, float] = {}
        for name, xs in bucket.items():
            f = self._factors.get(name)
            interval = max(int(getattr(f, "interval_sec", 1)), 1) if f else 1
            key = f"{name}_{interval}s"
            for stat, val in summarize(xs).items():
                flat[f"{key}_{stat}"] = val
        return flat

    # ---- 生命周期（照搬 RiskLoop）----
    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while self._running:
            try:
                self._tick()
            except Exception as e:
                logger.warning(f"SecFactorLoop _tick error: {e}")
            await asyncio.sleep(self._interval)
