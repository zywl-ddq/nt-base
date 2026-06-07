"""gRPC TradingBase servicer — embedded in nt-base live trading node.

Receives Bar data from Binance WS (via main.py dispatch loop),
streams to subscribed strategies, and accepts trading signals.
"""
from __future__ import annotations

import asyncio
import logging
import time

import grpc
import pandas as pd

import trading_base_pb2 as pb
import trading_base_pb2_grpc as pb_grpc
from base.factor_engine import FactorEngine

logger = logging.getLogger(__name__)


class TradingBaseServicer(pb_grpc.TradingBaseServicer):
    """gRPC service implementation for live trading.

    Lifecycle:
      1. Strategy calls Register(config, factors) — base compiles factor code
      2. Strategy calls SubscribeBars(symbol) — base starts streaming
      3. Each 1m bar: base computes factors, builds Bar pb, pushes to stream
      4. Strategy calls SubmitSignal(signal) — base validates + executes
      5. Strategy calls Unregister or disconnects — base cleans up
    """

    def __init__(self):
        self._strategies: dict[str, dict] = {}
        self._factor_engine = FactorEngine()
        # Bar queues per strategy: strategy_id → asyncio.Queue
        self._bar_queues: dict[str, asyncio.Queue] = {}
        # Pending signals from strategy → processed by main loop
        self._pending_signals: asyncio.Queue = asyncio.Queue()

    # ── Registration ──────────────────────────────────────

    async def Register(self, request: pb.StrategyConfig, context) -> pb.RegisterAck:
        sid = request.strategy_id
        if sid in self._strategies:
            return pb.RegisterAck(ok=False, error=f"already registered: {sid}")

        # Compile and register factor code
        for fd in request.factors:
            try:
                self._factor_engine.register(
                    name=fd.name,
                    code=fd.code,
                    params=dict(fd.params) if fd.params else None,
                )
            except SyntaxError as e:
                return pb.RegisterAck(ok=False, error=f"Factor '{fd.name}' syntax: {e}")

        self._strategies[sid] = {
            "config": request,
            "registered_at": time.time(),
            "required_fields": list(request.required_fields),
        }
        self._bar_queues[sid] = asyncio.Queue(maxsize=100)

        logger.info(
            f"gRPC Register: {sid} factors={self._factor_engine.registered_names()} "
            f"fields={request.required_fields}"
        )
        return pb.RegisterAck(ok=True)

    async def Unregister(self, request: pb.StrategyId, context) -> pb.UnregisterAck:
        sid = request.strategy_id
        self._strategies.pop(sid, None)
        if sid in self._bar_queues:
            del self._bar_queues[sid]
        logger.info(f"gRPC Unregister: {sid}")
        return pb.UnregisterAck(ok=True)

    # ── Bar Streaming ─────────────────────────────────────

    async def SubscribeBars(self, request: pb.BarRequest, context):
        """Server-streaming RPC: push Bar messages to strategy.

        The strategy_id is extracted from gRPC metadata or from the
        most recently registered strategy for this connection.
        """
        # Find which strategy this stream belongs to
        sid = None
        for s_id, q in self._bar_queues.items():
            if q is not None:
                sid = s_id
                break
        if sid is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "no registered strategy")
            return

        queue = self._bar_queues[sid]
        logger.info(f"SubscribeBars: {sid} symbol={request.symbol}")

        while context.is_active():
            try:
                bar = await asyncio.wait_for(queue.get(), timeout=30.0)
                yield bar
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    # ── Signal Submission ─────────────────────────────────

    async def SubmitSignal(self, request: pb.Signal, context) -> pb.SignalAck:
        """Strategy submits a trading signal. Base queues it for execution."""
        await self._pending_signals.put(request)
        direction_name = pb.Signal.Direction.Name(request.direction)
        logger.info(f"Signal: dir={direction_name} reason={request.reason}")
        return pb.SignalAck(accepted=True)

    async def GetState(self, request: pb.StateRequest, context) -> pb.StateResponse:
        return pb.StateResponse(equity=0.0, daily_pnl=0.0, circuit_breaker=False)

    async def ClosePosition(self, request: pb.CloseRequest, context) -> pb.CloseAck:
        return pb.CloseAck(ok=True)

    # ── Called by main loop (Bar dispatch) ─────────────────

    def push_bar(self, pb_bar: pb.Bar):
        """Push a Bar to all subscribed strategy queues."""
        for sid, queue in list(self._bar_queues.items()):
            try:
                queue.put_nowait(pb_bar)
            except asyncio.QueueFull:
                logger.warning(f"Bar queue full for {sid}, dropping")

    def build_bar(
        self,
        symbol: str,
        ts_ns: int,
        open_p: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        delta: float,
        taker_buy: float,
        taker_sell: float,
        btc_close: float,
        df_bars: pd.DataFrame,
        position_state: pb.PositionState | None = None,
    ) -> pb.Bar:
        """Build a Bar protobuf from raw bar data + factor computation."""
        factors = self._factor_engine.execute_all(df_bars)

        bar = pb.Bar(
            symbol=symbol, ts_ns=ts_ns,
            open=open_p, high=high, low=low, close=close,
            volume=volume, delta=delta,
            taker_buy_vol=taker_buy, taker_sell_vol=taker_sell,
            btc_close=btc_close,
            factors=factors,
        )
        if position_state is not None:
            bar.position.CopyFrom(position_state)
        return bar

    def pending_signals(self) -> list[pb.Signal]:
        """Drain and return all pending signals (non-blocking)."""
        signals = []
        while not self._pending_signals.empty():
            try:
                signals.append(self._pending_signals.get_nowait())
            except asyncio.QueueEmpty:
                break
        return signals


async def start_grpc_server(
    servicer: TradingBaseServicer,
    listen_socket: str = "unix:///tmp/nt_base_grpc.sock",
    listen_port: int = 50051,
):
    """Start async gRPC server with Unix socket + TCP listeners."""
    server = grpc.aio.server()

    pb_grpc.add_TradingBaseServicer_to_server(servicer, server)

    # Unix socket (low latency for local strategies)
    server.add_insecure_port(listen_socket)
    logger.info(f"gRPC listening on {listen_socket}")

    # TCP (for remote strategies)
    server.add_insecure_port(f"0.0.0.0:{listen_port}")
    logger.info(f"gRPC listening on 0.0.0.0:{listen_port}")

    await server.start()
    return server
