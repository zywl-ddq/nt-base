# -*- coding: utf-8 -*-
"""
tests/test_grpc_server.py -- gRPC 服务端单元测试
=================================================

覆盖：
  P0-2 — SubmitSignal 在执行上下文未就绪时返回 accepted=False（不静默丢失）
  P0-3 — GetState/ClosePosition 空桩安全处理（不返回误导性默认值）
"""
import pytest

import trading_base_pb2 as pb
from base.grpc_server import TradingBaseServicer
from base.slot import StrategySlot


class _FakeStrategy:
    strategy_id = "X"
    subscriptions = []

    def on_bar(self, d):
        return None

    def on_shutdown(self):
        pass

    def get_diagnostics(self):
        return {}


@pytest.mark.asyncio
async def test_submit_signal_rejected_when_executor_not_ready():
    """P0-2: executor/registry 未就绪 → accepted=False + 非空 reject_reason。"""
    servicer = TradingBaseServicer("tok", "chat", pool=None)
    servicer._executor = None
    servicer._registry = None
    servicer._get_price = None

    req = pb.Signal(strategy_id="S1", direction=1, reason="test")
    ack = await servicer.SubmitSignal(req, context=None)

    assert ack.accepted is False
    assert ack.reject_reason  # 非空，说明未执行原因


@pytest.mark.asyncio
async def test_close_position_not_fake_success():
    """P0-3: ClosePosition 未实现 → ok=False（不假装成功，避免残留仓位误判）。"""
    servicer = TradingBaseServicer("tok", "chat", pool=None)
    ack = await servicer.ClosePosition(pb.CloseRequest(), context=None)
    assert ack.ok is False


@pytest.mark.asyncio
async def test_get_state_reflects_circuit_breaker():
    """P0-3: GetState circuit_breaker 反映 registry 真实熔断状态。"""

    class _Reg:
        def all_slots(self):
            s = StrategySlot(strategy_id="X", strategy=_FakeStrategy(), symbol="SOLUSDT-PERP")
            s.tripped = True
            return [s]

    servicer = TradingBaseServicer("tok", "chat", pool=None)
    servicer._registry = _Reg()
    resp = await servicer.GetState(pb.StateRequest(), context=None)
    assert resp.circuit_breaker is True


@pytest.mark.asyncio
async def test_get_state_no_circuit_breaker_when_registry_none():
    """P0-3: registry 未就绪时 GetState 安全默认 circuit_breaker=False。"""
    servicer = TradingBaseServicer("tok", "chat", pool=None)
    servicer._registry = None
    resp = await servicer.GetState(pb.StateRequest(), context=None)
    assert resp.circuit_breaker is False
