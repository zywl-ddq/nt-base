# -*- coding: utf-8 -*-
"""
tests/test_sec_factor.py — 秒级公共因子框架测试
================================================
覆盖：注册/周期调度、日历分钟桶对齐、聚合统计正确性、样本不足降级、
桶清理、注销、on_tick 分发。
直接驱动 _tick / aggregate，不跑真实 sleep，稳定快速。
"""
import pytest

from base.sec_factor import SecFactorLoop, summarize, NS_PER_MIN
from base.sec_factors.obi import OBIFactor
from base.sec_factors.cvd import CVDFactor


class _CounterFactor:
    """测试用因子：compute 每次返回递增值，记录调用次数。"""
    def __init__(self, name, interval_sec):
        self.name = name
        self.interval_sec = interval_sec
        self.calls = 0
        self.ticks = 0
        self.books_seen = 0

    def on_tick(self, tick):
        self.ticks += 1

    def on_book(self, books):
        self.books_seen += 1

    def compute(self):
        self.calls += 1
        return float(self.calls)


def test_register_unregister():
    loop = SecFactorLoop()
    f = _CounterFactor("t", 3)
    loop.register(f)
    assert "t" in loop.registered()
    loop.unregister("t")
    assert "t" not in loop.registered()


def test_period_scheduling():
    """interval=3：6 个 tick（_tick_count 1..6）→ 仅 3、6 命中 → compute 2 次。"""
    loop = SecFactorLoop()
    f = _CounterFactor("t", 3)
    loop.register(f)
    base = 1000 * NS_PER_MIN
    for i in range(6):
        loop._tick(now_ns=base + i * 1_000_000_000)
    assert f.calls == 2
    minute = base // NS_PER_MIN
    assert loop._buckets[minute]["t"] == [1.0, 2.0]


def test_minute_alignment():
    """跨分钟边界的样本归入不同桶。"""
    loop = SecFactorLoop()
    loop.register(_CounterFactor("t", 1))
    m0 = 5000 * NS_PER_MIN
    m1 = 5001 * NS_PER_MIN
    loop._tick(now_ns=m0 + 59 * 1_000_000_000)   # 第 5000 分钟第 59 秒
    loop._tick(now_ns=m1 + 1 * 1_000_000_000)    # 第 5001 分钟第 1 秒
    assert 5000 in loop._buckets
    assert 5001 in loop._buckets
    assert len(loop._buckets[5000]["t"]) == 1
    assert len(loop._buckets[5001]["t"]) == 1


def test_aggregate_stats():
    """样本 [1,2,3,4] 的 6 个统计量；聚合后桶被删。"""
    loop = SecFactorLoop()
    loop.register(_CounterFactor("t", 3))
    minute_key = 12345
    loop._buckets[minute_key] = {"t": [1.0, 2.0, 3.0, 4.0]}
    flat = loop.aggregate(minute_key * NS_PER_MIN)
    assert flat["t_3s_mean"] == pytest.approx(2.5)
    assert flat["t_3s_std"] == pytest.approx(1.1180, rel=1e-3)   # 总体标准差
    assert flat["t_3s_min"] == 1.0
    assert flat["t_3s_max"] == 4.0
    assert flat["t_3s_skew"] == pytest.approx(0.0, abs=1e-9)     # 对称 → 0
    assert flat["t_3s_kurt"] == pytest.approx(-1.36, abs=0.01)   # Fisher 峰度
    assert minute_key not in loop._buckets                         # 桶清理


def test_summarize_insufficient_samples():
    """样本不足：偏度/峰度填 0，不抛异常。"""
    one = summarize([1.0])
    assert one["mean"] == 1.0 and one["skew"] == 0.0 and one["kurt"] == 0.0 and one["std"] == 0.0
    two = summarize([1.0, 2.0])
    assert two["skew"] == 0.0 and two["kurt"] == 0.0
    assert summarize([]) == {s: 0.0 for s in ("mean", "std", "min", "max", "skew", "kurt")}


def test_summarize_constant_samples():
    """全相同样本（σ=0）：skew/kurt 归零，绝不返回 nan。"""
    c = summarize([-1.0, -1.0, -1.0, -1.0])
    assert c["std"] == 0.0
    assert c["skew"] == 0.0
    assert c["kurt"] == 0.0
    assert c["mean"] == -1.0
    for v in c.values():          # 无 nan（protobuf float 不支持）
        assert v == v


def test_aggregate_empty_bucket():
    loop = SecFactorLoop()
    assert loop.aggregate(99999 * NS_PER_MIN) == {}


def test_on_tick_dispatch_and_now():
    loop = SecFactorLoop()
    f = _CounterFactor("t", 1)
    loop.register(f)

    class Tick:
        ts_event = 777 * NS_PER_MIN + 5_000_000_000

    loop.on_tick(Tick())
    assert f.ticks == 1
    assert loop._now_ns == Tick.ts_event


def test_obi_factor():
    """OBI: (Σbid − Σask) / (Σbid + Σask)，取 top levels。"""
    obi = OBIFactor(interval_sec=3, levels=2)
    obi.on_book({"SOLUSDT-PERP": {"bids": {100.0: 5.0, 99.0: 3.0, 98.0: 1.0},
                                  "asks": {101.0: 4.0, 102.0: 2.0, 103.0: 1.0}}})
    # top2 bid: 5+3=8, top2 ask: 4+2=6 → (8-6)/(8+6)
    assert obi.compute() == pytest.approx(2.0 / 14.0)
    obi.on_book({})              # 空盘口
    assert obi.compute() == 0.0


def test_cvd_factor():
    """CVD: 周期内 BUYER 流入 − SELLER 流出；compute 后清零。"""
    cvd = CVDFactor(interval_sec=1)

    class Tick:
        def __init__(self, side, size):
            self.size = size
            self.aggressor_side = type("_A", (), {"name": side})()
    cvd.on_tick(Tick("BUYER", 10.0))
    cvd.on_tick(Tick("BUYER", 5.0))
    cvd.on_tick(Tick("SELLER", 4.0))
    assert cvd.compute() == pytest.approx(11.0)   # 10+5−4
    assert cvd.compute() == 0.0                    # 已清零
