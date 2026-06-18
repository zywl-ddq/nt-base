# -*- coding: utf-8 -*-
"""
tests/test_risk_loop.py -- RiskLoop 风控循环单元测试
=====================================================

覆盖 P0-1：熔断(tripped)但仍有持仓的 slot 必须被兜底平仓。

背景：get_active_slots() 过滤掉 tripped slot，若日亏损熔断触发时的 flat()
失败（仓位残留），该 slot.tripped=True 且 has_position=True，会被风控循环
永久跳过——止损/止盈不再监控，仓位脱离风控。兜底逻辑每秒对这类 slot
重发平仓，确保最终被平掉。
"""
import asyncio
import pytest

from risk.loop import RiskLoop
from base.slot import StrategySlot


class FakeStrategy:
    strategy_id = "test"
    subscriptions = []

    def on_bar(self, d):
        return None

    def on_shutdown(self):
        pass

    def get_diagnostics(self):
        return {}


def make_slot(**kw):
    return StrategySlot(strategy_id="test", strategy=FakeStrategy(), **kw)


class FakeExecutor:
    """记录 flat() 调用；has_pending_close_for 始终返回 False（模拟平仓未在途）。"""

    def __init__(self):
        self.flats = []  # [(strategy_id, reason), ...]

    def flat(self, slot, reason=""):
        self.flats.append((slot.strategy_id, reason))

    def has_pending_close_for(self, slot):
        return False


class FakeRegistry:
    """get_active_slots() 过滤 tripped（与真实 StrategyRegistry 一致）；all_slots() 返回全部。"""

    def __init__(self, slots):
        self._slots = slots

    def get_active_slots(self):
        return [s for s in self._slots if s.has_position and not s.tripped]

    def all_slots(self):
        return list(self._slots)


@pytest.mark.asyncio
async def test_tripped_open_position_is_flattened_by_fallback():
    """P0-1：熔断后仍有持仓的 slot 必须被兜底重发平仓，不脱离风控。"""
    s = make_slot(stop_pct=0.03, take_pct=0.06, max_hold_sec=3600,
                  symbol="SOLUSDT-PERP")
    s.open_position("LONG", 100.0)
    s.tripped = True  # 已熔断，但仓位仍在（flat 失败残留）

    reg = FakeRegistry([s])
    exe = FakeExecutor()
    loop = RiskLoop(reg, exe, interval=0.01)

    await loop.start()
    await asyncio.sleep(0.05)
    await loop.stop()

    flats_for_test = [r for (sid, r) in exe.flats if sid == "test"]
    assert len(flats_for_test) >= 1, "熔断后仍有持仓的仓位未被兜底平仓（丢止损风险）"
