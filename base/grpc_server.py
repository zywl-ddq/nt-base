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
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal, BarSubscription

import trading_base_pb2 as pb
import trading_base_pb2_grpc as pb_grpc
from base.factor_engine import FactorEngine

logger = logging.getLogger(__name__)



class _GrpcSlotStrategy:
    """Minimal SignalStrategy stub for gRPC-managed slots."""
    def __init__(self, sid: str):
        self.strategy_id = sid
    def on_bar(self, bar_data): return None
    def on_shutdown(self): pass
    def get_diagnostics(self): return {}

class TradingBaseServicer(pb_grpc.TradingBaseServicer):
    """gRPC service implementation for live trading.

    Lifecycle:
      1. Strategy calls Register(config, factors) — base compiles factor code
      2. Strategy calls SubscribeBars(symbol) — base starts streaming
      3. Each 1m bar: base computes factors, builds Bar pb, pushes to stream
      4. Strategy calls SubmitSignal(signal) — base validates + executes
      5. Strategy calls Unregister or disconnects — base cleans up
    """


    # ---- Execution context (wired by main.py after executor ready) ----
    _executor = None
    _registry = None
    _get_price = None

    def set_execution_context(self, executor, registry, get_price=None):
        """Called by BaseStrategy.on_start() once the executor is ready."""
        self._executor = executor
        self._registry = registry
        self._get_price = get_price
        logger.info("gRPC execution context set")

    def __init__(self, telegram_bot_token: str = "", telegram_chat_id: str = ""):
        self._default_bot_token = telegram_bot_token
        self._default_chat_id = telegram_chat_id
        self._strategies: dict[str, dict] = {}
        self._factor_engine = FactorEngine()
        # Bar queues per strategy: strategy_id → asyncio.Queue
        self._bar_queues: dict[str, asyncio.Queue] = {}
        # Control queues per strategy: Telegram -> gRPC push
        self._control_queues: dict[str, asyncio.Queue] = {}
        # Pending signals from strategy → processed by main loop
        self._pending_signals: asyncio.Queue = asyncio.Queue()

    # ── Registration ──────────────────────────────────────

    async def Register(self, request: pb.StrategyConfig, context) -> pb.RegisterAck:
        sid = request.strategy_id
        if sid in self._strategies:
            # Reconnect: clear disconnect marker, allow re-subscription
            self._strategies[sid]["disconnected_at"] = None
            logger.info(f"gRPC Re-register (reconnect): {sid}")
            return pb.RegisterAck(ok=True)

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

        # Telegram credentials: from env cfg (single-bot, shared by all strategies)
        from shared.env import cfg
        token = cfg.telegram.bot_token
        chat_id = str(cfg.telegram.admin_chat_id)
        logger.info(f"gRPC Register {sid}: telegram configured (chat={chat_id})")

        self._strategies[sid] = {
            "config": request,
            "registered_at": time.time(),
            "required_fields": list(request.required_fields),
            "telegram_bot_token": token,
            "telegram_chat_id": chat_id,
            "disconnected_at": None,
            "grace_period_sec": 60,
        }
        self._bar_queues[sid] = asyncio.Queue(maxsize=100)
        self._control_queues[sid] = asyncio.Queue(maxsize=50)

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
        logger.info(f"[SUB] SubscribeBars ENTER: queues={list(self._bar_queues.keys())}")
        sid = list(self._bar_queues.keys())[-1] if self._bar_queues else None
        if sid is None:
            logger.error("[SUB] no registered strategy")
            await context.abort(grpc.StatusCode.NOT_FOUND, "no registered strategy")
            return

        queue = self._bar_queues[sid]
        logger.info(f"[SUB] Starting stream for {sid} qsize={queue.qsize()}")

        bar_count = 0
        try:
            while True:
                try:
                    logger.info(f"[SUB] waiting (count={bar_count})...")
                    bar = await asyncio.wait_for(queue.get(), timeout=30.0)
                    bar_count += 1
                    logger.info(f"[SUB] yielding bar #{bar_count}")
                    yield bar
                    logger.info(f"[SUB] after yield #{bar_count}")
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    logger.info(f"[SUB] cancelled after {bar_count} bars")
                    return
        except Exception as e:
            logger.error(f"[SUB] crash: {type(e).__name__}: {e}", exc_info=True)
        finally:
            if sid and sid in self._strategies:
                self._strategies[sid]["disconnected_at"] = time.time()
                logger.warning(f"[SUB] Strategy {sid} DISCONNECTED, grace={self._strategies[sid]['grace_period_sec']}s")
        logger.info(f"[SUB] EXIT after {bar_count} bars")
    # ---- Control Streaming ----

    async def SubscribeControl(self, request: pb.ControlRequest, context):
        """Server-pushed control commands to strategy client."""
        sid = request.strategy_id
        if sid not in self._control_queues:
            logger.error(f"[CTL] unknown strategy: {sid}")
            await context.abort(grpc.StatusCode.NOT_FOUND, f"unknown strategy: {sid}")
            return

        queue = self._control_queues[sid]
        logger.info(f"[CTL] Control stream started for {sid}")

        cmd_count = 0
        try:
            while True:
                try:
                    cmd = await asyncio.wait_for(queue.get(), timeout=60.0)
                    cmd_count += 1
                    logger.info(f"[CTL] pushing command #{cmd_count}: type={cmd.type} to {sid}")
                    yield cmd
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    logger.info(f"[CTL] cancelled after {cmd_count} commands")
                    return
        except Exception as e:
            logger.error(f"[CTL] crash: {type(e).__name__}: {e}", exc_info=True)
        logger.info(f"[CTL] Control stream ended for {sid} after {cmd_count} commands")

    # ---- Signal Submission ----

    async def SubmitSignal(self, request: pb.Signal, context) -> pb.SignalAck:
        """Strategy submits a trading signal. Execute directly if context ready."""
        direction_name = pb.Signal.Direction.Name(request.direction)
        reason = request.reason
        logger.info(f"Signal: dir={direction_name} reason={reason}")
        await self._pending_signals.put(request)

        executor = self._executor
        registry = self._registry
        if executor is None or registry is None:
            return pb.SignalAck(accepted=True)

        pb_dir = request.direction
        gdir = 1 if pb_dir == 1 else (-1 if (pb_dir < 0 or pb_dir > 1) else 0)
        price = self._get_price() if self._get_price else 0.0

        for sid, info in self._strategies.items():
            if info.get("disconnected_at"):
                continue
            slot = registry.get_slot(sid)
            if slot is None:
                cfg = info["config"]
                subs = [BarSubscription(symbol="SOLUSDT-PERP", timeframe="1m", factors=[])]
                slot = StrategySlot(
                    strategy_id=sid, strategy=_GrpcSlotStrategy(sid),
                    subscriptions=subs, stop_pct=0.03, take_pct=0.06,
                    max_hold_sec=3600, cooldown_sec=60.0,
                    leverage=int(cfg.max_leverage) if cfg.max_leverage else 2,
                    position_size_pct=float(cfg.max_position_pct) if cfg.max_position_pct else 0.20,
                    symbol="SOLUSDT-PERP",
                    telegram_bot_token=info.get("telegram_bot_token") or self._default_bot_token,
                    telegram_chat_id=info.get("telegram_chat_id") or self._default_chat_id,
                )
                registry.register(slot)
                logger.info(f"Slot created for gRPC strategy: {sid}")

            sig = StrategySignal(direction=gdir, reason=reason, position_size_pct=request.position_size_pct)
            if sig.direction != 0:
                result = executor.execute(slot, sig, price)
            elif sig.reason == "hold":
                result = "hold"
            else:
                # Queue bar-level exit as a pending task for RiskLoop to execute per-second.
                # RiskLoop will retry every second until the position is fully closed.
                if slot.has_position:
                    slot.pending_bar_exit = sig.reason
                    logger.info(f"Bar exit queued: {sid} reason={sig.reason}")
                    result = f"queued: {sig.reason}"
                else:
                    result = "no position"
            logger.info(f"gRPC Signal: {sid} dir={sig.direction} result={result}")
            break

        return pb.SignalAck(accepted=True)
    async def GetState(self, request: pb.StateRequest, context) -> pb.StateResponse:
        return pb.StateResponse(equity=0.0, daily_pnl=0.0, circuit_breaker=False)

    async def ClosePosition(self, request: pb.CloseRequest, context) -> pb.CloseAck:
        return pb.CloseAck(ok=True)

    # ── Called by main loop (Bar dispatch) ─────────────────

    def push_bar(self, pb_bar: pb.Bar, position_states: dict | None = None):
        """Push a Bar to all subscribed strategy queues.

        If position_states is provided, each strategy receives a Bar clone
        with its own PositionState attached, so trading-v2 knows the
        authoritative position state (managed by nt-base tick exits).
        """
        for sid, queue in list(self._bar_queues.items()):
            bar_to_send = pb_bar
            if position_states and sid in position_states:
                bar_to_send = pb.Bar()
                bar_to_send.CopyFrom(pb_bar)
                bar_to_send.position.CopyFrom(position_states[sid])
            try:
                queue.put_nowait(bar_to_send)
            except asyncio.QueueFull:
                logger.warning(f"Bar queue full for {sid}, dropping")

    def push_control(self, sid: str, cmd: pb.ControlCommand):
        """Push a ControlCommand to a specific strategy's control queue."""
        if sid not in self._control_queues:
            logger.warning(f"[CTL] No control queue for {sid}, dropping command")
            return False
        try:
            self._control_queues[sid].put_nowait(cmd)
            logger.info(f"[CTL] Queued command type={cmd.type} for {sid}")
            return True
        except asyncio.QueueFull:
            logger.warning(f"[CTL] Control queue full for {sid}, dropping command")
            return False

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
        factors: dict[str, float] | None = None,
        position_state: pb.PositionState | None = None,
    ) -> pb.Bar:
        """Build a Bar protobuf from raw bar data + factor computation.

        If *factors* is None they are computed via FactorEngine;
        pass pre-computed factors to avoid double execution.
        """
        if factors is None:
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

    def registered_factor_names(self) -> set:
        return set(self._factor_engine.registered_names())

    def pending_signals(self) -> list[pb.Signal]:
        """Drain and return all pending signals (non-blocking)."""
        signals = []
        while not self._pending_signals.empty():
            try:
                signals.append(self._pending_signals.get_nowait())
            except asyncio.QueueEmpty:
                break
        return signals

    def orphaned_strategies(self) -> list[str]:
        """Return strategy IDs that have been disconnected past their grace period."""
        orphans = []
        now = time.time()
        for sid, info in list(self._strategies.items()):
            disc_at = info.get("disconnected_at")
            if disc_at is None:
                continue
            grace = info.get("grace_period_sec", 60)
            if now - disc_at > grace:
                orphans.append(sid)
        return orphans

    def cleanup_strategy(self, sid: str):
        """Remove a strategy registration and bar queue after orphan flat."""
        self._strategies.pop(sid, None)
        self._bar_queues.pop(sid, None)
        self._control_queues.pop(sid, None)
        logger.info(f"gRPC Cleanup: removed {sid}")


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
