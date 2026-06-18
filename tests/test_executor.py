# -*- coding: utf-8 -*-
"""
tests/test_executor.py -- OrderExecutor 单元测试
==================================================

测试目标
--------
base/executor.py 中的 OrderExecutor 类（maker 限价单 + max_concurrent=1 语义）：
- 入场执行（LONG 信号）→ 提交 maker 限价单；has_position 由 on_fill 回调设置（deferred）
- 已有持仓时拒绝新入场（max_concurrent=1）
- 平仓（flat）→ 提交反向 maker 限价单；on_fill 后 slot 退出持仓

注意事项
--------
- MockInstrument 需提供 make_price / make_qty（maker 限价单路径会调用）
- MockInstrument.create_order 返回 MockOrder（带 client_order_id，executor 据此追踪 _pending）
- 持仓状态由 on_fill 回调驱动（deferred 通知机制），测试需手动调用 on_fill 模拟成交
"""
"""Unit tests for OrderExecutor."""
import pytest
from base.executor import OrderExecutor
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal


class MockOrder:
    """模拟 NT 订单对象，提供 client_order_id 供 executor 追踪 _pending。"""
    _counter = 0

    def __init__(self):
        MockOrder._counter += 1
        self.client_order_id = f"mock-{MockOrder._counter}"


class MockInstrument:
    def __init__(self, last_price=100.0):
        self.last_price = last_price

    def create_order(self, **kwargs):
        return MockOrder()

    def make_qty(self, qty):
        return qty

    def make_price(self, price):
        # maker 限价单路径会调用以量化到 tick；测试用固定值，直接返回。
        return price


class MockPositionSide:
    def __init__(self, name="LONG"):
        self.name = name


class MockQuantity:
    def __init__(self, val=1.0):
        self._val = val

    def as_decimal(self):
        return self._val


class MockPosition:
    def __init__(self, side_name="LONG", qty=1.0):
        self.side = MockPositionSide(side_name)
        self.quantity = MockQuantity(qty)
        self.avg_px_open = 100.0


class MockCache:
    def __init__(self, instrument):
        self._instrument = instrument
        self.positions = []

    def instrument(self, instrument_id):
        return self._instrument

    def positions_open(self, instrument_id):
        return self.positions


class MockBalance:
    def as_decimal(self):
        return 1000.0


class MockAccount:
    def balance_total(self):
        return MockBalance()


class MockPortfolio:
    def account(self, venue):
        return MockAccount()


class FakeStrategy:
    strategy_id = "test"
    subscriptions = []

    def on_bar(self, d):
        return None

    def on_shutdown(self):
        pass

    def get_diagnostics(self):
        return {}


def test_order_executor_entry_and_flat():
    submitted_orders = []

    def submit_order(order):
        submitted_orders.append(order)

    instrument = MockInstrument(last_price=100.0)
    cache = MockCache(instrument)
    portfolio = MockPortfolio()

    executor = OrderExecutor(
        sol_id="SOLUSDT-PERP",
        venue="BINANCE",
        portfolio=portfolio,
        submit_order=submit_order,
        cache=cache,
        order_factory=None,  # 回退到 MockInstrument.create_order
    )

    slot = StrategySlot(
        strategy_id="test-strategy",
        strategy=FakeStrategy(),
        position_size_pct=0.20,
        leverage=2,
        cooldown_sec=0.0,
    )

    # 1. 无持仓 → 入场 LONG（提交 maker 限价单）
    sig = StrategySignal(direction=1, reason="Test entry long")
    res = executor.execute(slot, sig, current_price=100.0)
    assert "entry" in res
    assert len(submitted_orders) == 1
    # deferred：订单已提交，has_position 等 on_fill 回调确认
    cid_entry = submitted_orders[0].client_order_id
    assert cid_entry in executor._pending
    assert executor._pending[cid_entry]["type"] == "entry"
    assert executor.instance_for_cid(cid_entry) == "test-strategy"

    # 2. 模拟成交回调 → slot 进入持仓（真实 VWAP = 100.0）
    cache.positions = [MockPosition("LONG", 1.0)]
    executor.on_fill(cid_entry, last_px=100.0, last_qty=1.0, commission=0.0)
    assert slot.has_position
    assert slot.entry_side == "LONG"
    assert slot.entry_price == 100.0

    # 3. 已有持仓 → 拒绝新入场（max_concurrent=1）
    res_rejected = executor.execute(slot, sig, current_price=101.0)
    assert res_rejected == "rejected: position exists (max_concurrent=1)"
    assert len(submitted_orders) == 1  # 未新增订单

    # 4. 平仓（提交反向 maker 限价单）
    flattened = executor.flat(slot, reason="Stop triggered", price=101.0)
    assert flattened
    assert len(submitted_orders) == 2
    cid_close = submitted_orders[1].client_order_id
    assert executor._pending[cid_close]["type"] == "close"

    # 5. 模拟平仓成交 → slot 退出持仓
    cache.positions = []
    executor.on_fill(cid_close, last_px=101.0, last_qty=1.0, commission=0.0)
    assert not slot.has_position
