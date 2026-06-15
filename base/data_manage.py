# -*- coding: utf-8 -*-
"""
===========================================================
模块:    base/data_manage
模块名:  数据管理模块
===========================================================
用途:    系统中唯一的行情订阅者，负责所有行情数据的订阅和 TimescaleDB 持久化。
         订阅所有 bar 类型 (1s/5s/1m)、逐笔成交 (trade tick)、
         订单薄 (L2)、资金费率 (funding rate) 和持仓量 (OI)。

类: DataManageActor (继承 NT Actor)
  职责:
    1. Bar 订阅 (1s/5s INTERNAL, 1m INTERNAL 从 tick 聚合)
    2. 逐笔成交接收和 CVD 差值计算
    3. 订单薄增量订阅 (每 1 秒生成一次 L2 快照)
    4. 资金费率轮询 (两级：结算/快照)
    5. 持仓量轮询 (每 30 秒)
    6. 批量持久化到 TimescaleDB (bars, ticks, funding, L2, OI)
    7. Bar 发布到消息总线 (msgbus) 供策略被动消费

配置: DataManageConfig
    instrument_ids: tuple[str]         交易品种列表
    tick_instrument_ids: tuple[str]    逐笔成交订阅品种 (默认同 instrument_ids)
    bar_timeframes: tuple[str]         bar 聚合周期 (默认 "1-SECOND", "5-SECOND", "1-MINUTE")
    flush_interval_sec: float          数据库写入批次间隔 (默认 5 秒)
    max_buffer: int                    最大缓冲行数，超过即强制刷写 (默认 1000)
    collect_l2: bool                   是否采集 L2 订单薄 (默认 True)
    l2_snapshot_interval_sec: float    L2 快照生成间隔 (默认 1 秒)
    collect_oi: bool                   是否采集持仓量 (默认 True)
    oi_poll_interval_sec: float        OI 轮询间隔 (默认 30 秒)

架构说明:
  本 Actor 是所有 bar 类型的唯一活跃订阅者。
  策略通过消息总线 (msgbus) 被动监听 bar 主题 —— 不存在订阅冲突。
  main.py 中通过猴子补丁 (monkey-patch) 拦截 on_bar 回调，
  用于因子计算和策略信号调度。

Bar 数据来源:
  v1.0: 分钟级以上 bar 使用 EXTERNAL (Binance REST) 来源，
         但测试网没有 REST 客户端可用。
  v1.1: 所有 bar 使用 INTERNAL 聚合，
         从 WebSocket 逐笔成交 (trade tick) 聚合得到 (已修复)。

Bar 类型标签:
  _bar_tf_label(bar_type) -> "1s", "5s", "1m" —— 入库友好的时间周期字符串

作者:    nt-base 系统
版本:    1.1.0 (INTERNAL bar 来源修复)
===========================================================
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
from nautilus_trader.common.actor import Actor, ActorConfig
from nautilus_trader.model.data import Bar, BarType, TradeTick
from nautilus_trader.model.events import (
    OrderEvent,
    OrderFilled,
    PositionClosed,
    PositionEvent,
    PositionOpened,
)
from nautilus_trader.model.identifiers import InstrumentId

from shared.env import cfg

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 配置类
# ═══════════════════════════════════════════════════════════════


class DataManageConfig(ActorConfig, frozen=True):
    """数据管理配置：品种、时间周期、刷写设置。

    DataManageActor 是所有 bar 类型的唯一活跃订阅者。
    策略通过消息总线被动监听 —— 不存在订阅冲突。
    """

    # 交易品种列表，格式如 "SOLUSDT-PERP.BINANCE"
    instrument_ids: tuple[str, ...]
    # 逐笔成交订阅品种，默认等于 instrument_ids
    tick_instrument_ids: tuple[str, ...] | None = None
    # Bar 聚合周期列表 (支持 SECOND, MINUTE, HOUR 等)
    bar_timeframes: tuple[str, ...] = ("1-SECOND", "5-SECOND", "1-MINUTE")
    # 数据库批量写入间隔 (秒)
    flush_interval_sec: float = 5.0
    # 缓冲行数上限，超过即强制刷写
    max_buffer: int = 1000
    # 是否采集 L2 订单薄
    collect_l2: bool = True
    # L2 快照生成间隔 (秒)
    l2_snapshot_interval_sec: float = 1.0
    # 是否采集持仓量
    collect_oi: bool = True
    # OI 轮询间隔 (秒)
    oi_poll_interval_sec: float = 30.0


# ═══════════════════════════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════════════════════════


def _ns_to_dt(ns: int) -> datetime:
    """将纳秒级 Unix 时间戳转换为 datetime，避免浮点精度丢失。

    对于 19 位纳秒级时间戳，若使用浮点数除法（IEEE 754 双精度约 15~16 位有效数字），
    会丢失微秒精度。本函数通过整数运算精确转换：

    1. 纳秒 // 1_000_000_000 -> 秒
    2. (纳秒 % 1_000_000_000) // 1_000 -> 微秒
    3. 用 .replace(microsecond=微秒) 恢复精度
    """
    seconds = ns // 1_000_000_000
    micros = (ns % 1_000_000_000) // 1_000
    return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(microsecond=micros)


# 时间周期缩写映射：NT 的 AGG 枚举 -> DB 友好的短字符串
_AGG_SUFFIX = {
    "SECOND": "s", "MINUTE": "m", "HOUR": "h", "DAY": "d",
    "MILLISECOND": "ms", "WEEK": "w", "MONTH": "M", "YEAR": "y",
}


def _bar_tf_label(bar_type: BarType) -> str:
    """将 BarType 转换为人类友好的时间周期字符串。

    例如: '1s', '5s', '1m'。

    解析逻辑：取 bar_type.spec 的字符串表示 (格式为 '<步长>-<聚合类型>-<价格类型>')，
    将聚合类型映射为简短后缀，与步长拼接返回。
    """
    s = str(bar_type.spec)  # 例如 '5-SECOND-LAST'
    parts = s.split("-")
    if len(parts) >= 2:
        step, agg = parts[0], parts[1]
        return f"{step}{_AGG_SUFFIX.get(agg, agg.lower()[:1])}"
    return s


def _safe_json(obj: Any) -> str:
    """安全地将任意对象序列化为 JSON 字符串。

    如果直接 json.dumps 失败，则先转为字符串再序列化。
    用于存储原始事件的 raw 字段。
    """
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return json.dumps(str(obj))


# ═══════════════════════════════════════════════════════════════
# 数据管理 Actor
# ═══════════════════════════════════════════════════════════════


class DataManageActor(Actor):
    """将 NautilusTrader 事件持久化到 TimescaleDB。

    这是系统中唯一的行情数据 Actor，职责涵盖：
    - 订阅并持久化 bar / tick / L2 / OI / 资金费率
    - 处理订单和持仓事件并写入 events 表
    - 通过消息总线向策略分发 bar 数据
    """

    def __init__(self, config: DataManageConfig) -> None:
        super().__init__(config)
        self._cfg: DataManageConfig = config

        # ── 数据库连接 ──────────────────────────────────────────────
        self._pool: asyncpg.Pool | None = None       # asyncpg 连接池
        self._loop: asyncio.AbstractEventLoop | None = None  # 事件循环
        self._flush_task: asyncio.Task | None = None  # 定期刷写任务
        self._funding_task: asyncio.Task | None = None  # 资金费率轮询任务
        self._running = False                          # 运行状态标志

        # ── 数据缓冲区 ──────────────────────────────────────────────
        # 所有数据先写入缓冲区，由 _flush_loop 定期批量写入 DB
        self._bar_buf: list[tuple] = []        # Bar 缓冲区
        self._tick_buf: list[tuple] = []       # Tick 缓冲区
        self._funding_buf: list[tuple] = []    # 资金费率缓冲区
        self._l2_buf: list[tuple] = []         # L2 快照缓冲区
        self._oi_buf: list[tuple] = []         # 持仓量缓冲区

        # ── 计数器 (用于 on_stop 时统计各类型数据量) ──────────────
        self._n_bars = 0
        self._n_ticks = 0
        self._n_funding = 0
        self._n_order_events = 0
        self._n_position_events = 0
        self._n_l2 = 0
        self._n_oi = 0

        # ── L2 / OI 状态 ────────────────────────────────────────────
        self._books: dict[str, dict] = {}                      # L2 订单薄内存状态
        self._l2_snapshot_task: asyncio.Task | None = None     # L2 快照循环任务
        self._oi_task: asyncio.Task | None = None              # OI 轮询循环任务
        self._position_write_lock = asyncio.Lock()             # 持仓写入互斥锁
        self._base_strategy = None  # BaseStrategy 引用（bind_base_strategy 注入），落库时按 cid 反查 instance

    # ═══════════════════════════════════════════════════════════
    # 生命周期
    # ═══════════════════════════════════════════════════════════

    def on_start(self) -> None:
        """Actor 启动时的回调方法。

        执行以下初始化：
        1. 获取事件循环引用
        2. 创建异步初始化任务 (建立数据库连接池、启动刷写循环等)
        3. 订阅逐笔成交 (trade tick)
        4. 订阅 Bar (INTERNAL 聚合方式)
        5. 订阅消息总线的订单/持仓事件
        6. 如果启用，订阅 L2 订单薄增量
        """
        self.log.info(f"DataManageActor starting (instruments={self._cfg.instrument_ids})")
        self._loop = asyncio.get_event_loop()
        self._running = True

        # 安排异步初始化：创建 DB 连接池并启动后台循环任务
        self._loop.create_task(self._async_init())

        # ── 订阅 TICK ──────────────────────────────────────────────
        # 逐笔成交通过标准 API 订阅。每个订阅者获得独立分发，不会产生竞争。
        tick_ids = self._cfg.tick_instrument_ids or self._cfg.instrument_ids
        for s in tick_ids:
            iid = InstrumentId.from_str(s)
            self.subscribe_trade_ticks(iid)

        # ── 订阅 BAR ──────────────────────────────────────────────
        # 关键说明：不要直接调用 subscribe_bars() 订阅 bar。
        # NT 1.227 将每个 BarType 绑定到唯一的聚合器 (Aggregator)，
        # 第二个订阅者会收到 "currently in use" 警告并静默丢弃 bar。
        #
        # 修复方式：本 Actor 仍然调用 subscribe_bars() 以启动聚合器
        # (Controller 无法直接调用)，但所有消费者（包括本 Actor 自己）
        # 通过消息总线 (msgbus) 被动获取 bar 副本。
        #
        # 所有 bar 使用 INTERNAL 聚合（从 trade tick 聚合），
        # 不依赖 Binance REST 的 kline 数据。
        for s in self._cfg.instrument_ids:
            for tf in self._cfg.bar_timeframes:
                tf_u = tf.upper()
                # INTERNAL: NT 从 trade ticks 聚合
                # EXTERNAL: Binance 提供预构建的 klines
                src = "INTERNAL"  # 全部使用 tick 聚合
                bt_str = f"{s}-{tf_u}-LAST-{src}"
                bt = BarType.from_str(bt_str)
                try:
                    self.subscribe_bars(bt)
                    self.log.info(f"subscribed to bars: {bt}")
                except Exception as e:
                    self.log.warning(f"subscribe_bars {bt} failed: {e}")

        # ── 订阅订单/持仓事件 ──────────────────────────────────────
        # 通过消息总线通配符订阅，获取所有订单和持仓事件
        try:
            self.msgbus.subscribe("events.order.*", self._on_order_event)
            self.msgbus.subscribe("events.position.*", self._on_position_event)
        except Exception as e:
            self.log.warning(f"msgbus wildcard subscribe failed: {e}; falling back to on_event")

        # ── 订阅 L2 订单薄增量 ──────────────────────────────────────
        if self._cfg.collect_l2:
            for s in self._cfg.instrument_ids:
                iid = InstrumentId.from_str(s)
                self._books[s] = {"bids": {}, "asks": {}}
                self.subscribe_order_book_deltas(iid)
                self.log.info(f"L2 subscribed: {iid}")

    def on_stop(self) -> None:
        """Actor 停止时的回调方法。

        输出各计数器统计信息，触发最终刷写并关闭数据库连接池。
        """
        self.log.info(
            f"DataManageActor stopping. Counters: "
            f"bars={self._n_bars} ticks={self._n_ticks} "
            f"funding={self._n_funding} l2={self._n_l2} "
            f"oi={self._n_oi} orders={self._n_order_events} "
            f"positions={self._n_position_events}"
        )
        self._running = False
        if self._flush_task and self._loop:
            self._loop.create_task(self._final_flush_and_close())

    # ═══════════════════════════════════════════════════════════
    # 异步初始化
    # ═══════════════════════════════════════════════════════════

    async def _async_init(self) -> None:
        """异步初始化：建立数据库连接池并启动后台循环任务。

        1. 创建 asyncpg 连接池 (2~8 个连接)
        2. 测试连接可用性
        3. 启动定期刷写循环 (_flush_loop)
        4. 启动资金费率轮询循环 (_funding_poll_loop)
        5. 如果启用，启动 L2 快照循环和 OI 轮询循环
        """
        try:
            self._pool = await asyncpg.create_pool(
                dsn=cfg.timescale.dsn, min_size=2, max_size=8
            )
            # 测试连接是否可用
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")
            self.log.info("DataManage asyncpg pool ready")
        except Exception as e:
            self.log.error(f"DataManage pool init failed: {e}")
            return

        # 启动对账：清理上次运行残留的僵尸仓位（sandbox 重启 NT cache 必空）
        await self._reconcile_startup_positions()

        # 启动各后台循环
        self._flush_task = self._loop.create_task(self._flush_loop())
        self._funding_task = self._loop.create_task(self._funding_poll_loop())
        if self._cfg.collect_l2:
            self._l2_snapshot_task = self._loop.create_task(self._l2_snapshot_loop())
        if self._cfg.collect_oi:
            self._oi_task = self._loop.create_task(self._oi_poll_loop())

    async def _reconcile_startup_positions(self) -> None:
        """启动对账：sandbox 重启后 NT cache 必空，DB 中残留的 closed_at IS NULL
        仓位已无实际头寸（NT 不会再发 PositionClosed）。标记关闭，防止僵尸累积。

        前提：sandbox SandboxExecutionClient 非持久化，重启 cache 清空。
        本方法在 _async_init 早期（pool ready 后、任何交易前）执行，此时
        closed_at IS NULL 全是上次运行的残留。
        """
        if not self._pool:
            return
        try:
            async with self._pool.acquire() as conn:
                status = await conn.execute(
                    "UPDATE positions SET closed_at = now(), "
                    "raw = COALESCE(raw, '{}'::jsonb) || "
                    "jsonb_build_object('reconcile', 'startup_orphan', "
                    "'note', 'closed at startup: NT cache lost on restart') "
                    "WHERE closed_at IS NULL"
                )
            self.log.info(f"startup reconcile: {status}")
        except Exception as e:
            self.log.error(f"startup reconcile failed: {e}")

    # ═══════════════════════════════════════════════════════════
    # 资金费率轮询 (两级)
    # ═══════════════════════════════════════════════════════════

    async def _funding_poll_loop(self) -> None:
        """两级资金费率轮询，语义上清晰分离：

        A 级 (每 5 分钟) —— 获取已结算的历史费率 (8 小时间隔边界)
            通过 Binance 的 fundingRateHistory API 获取。
            写入时 kind='settled'，这是回测和资金账户计算 PnL 的唯一权威数据。

        B 级 (每 60 秒) —— 获取当前溢价指数快照 (前瞻性观察，并非结算)
            写入时 kind='snapshot'，仅用于监控。

        注意：在 P0-3 版本之前，循环每 60 秒将溢价指数点写入资金费率表，
        污染了数据序列，导致回测中资金费率成本估算严重偏高。
        """
        try:
            import ccxt.async_support as ccxt_async
        except Exception as e:
            self.log.warning(f"ccxt not available, skipping funding poller: {e}")
            return
        ex = ccxt_async.binance({"options": {"defaultType": "future"}})
        # 将 NT 品种格式映射为 (ccxt_symbol, db_symbol)
        ccxt_symbols: dict[str, tuple[str, str]] = {}
        for s in self._cfg.instrument_ids:
            base = s.split("-PERP")[0]                   # "SOLUSDT"
            if base.endswith("USDT"):
                ccxt_symbols[s] = (f"{base[:-4]}/USDT:USDT", s.split(".")[0])

        last_settled_fetch = 0.0  # 上次获取结算费率的时间 (Unix 秒)
        SETTLED_INTERVAL = 300.0  # A 级拉取间隔：5 分钟

        try:
            while self._running:
                await asyncio.sleep(60)
                if not self._pool:
                    continue
                now_epoch = asyncio.get_event_loop().time()

                # ── A 级：已结算费率历史 ──────────────────────────────
                if now_epoch - last_settled_fetch >= SETTLED_INTERVAL:
                    last_settled_fetch = now_epoch
                    settled_rows: list[tuple] = []
                    for nt_sym, (csym, db_sym) in ccxt_symbols.items():
                        try:
                            # 获取过去 24 小时的费率历史，limit=10 覆盖超过 24 小时的 8h 边界
                            since_ms = int(
                                (datetime.now(timezone.utc).timestamp() - 86400) * 1000
                            )
                            hist = await ex.fetch_funding_rate_history(
                                csym, since=since_ms, limit=10
                            )
                            for h in hist:
                                ts_ms = h.get("timestamp")
                                rate = h.get("fundingRate")
                                if ts_ms is None or rate is None:
                                    continue
                                ts = datetime.fromtimestamp(
                                    ts_ms / 1000.0, tz=timezone.utc
                                )
                                settled_rows.append(
                                    (db_sym, ts, Decimal(str(rate)),
                                     None, None, None, "settled")
                                )
                        except Exception as e:
                            self.log.warning(
                                f"funding history {csym} failed: {e}"
                            )
                    if settled_rows:
                        try:
                            async with self._pool.acquire() as conn:
                                await conn.executemany(
                                    """INSERT INTO funding
                                         (symbol,ts,rate,mark_price,index_price,
                                          next_funding_time,kind)
                                       VALUES ($1,$2,$3,$4,$5,$6,$7)
                                       ON CONFLICT (symbol,ts,kind) DO NOTHING""",
                                    settled_rows,
                                )
                            self._n_funding += len(settled_rows)
                        except Exception as e:
                            self.log.error(f"settled funding insert failed: {e}")

                # ── B 级：快照 (溢价指数) ────────────────────────────
                snap_rows: list[tuple] = []
                for nt_sym, (csym, db_sym) in ccxt_symbols.items():
                    try:
                        fr = await ex.fetch_funding_rate(csym)
                        ts = datetime.fromtimestamp(
                            (fr.get("timestamp") or 0) / 1000.0, tz=timezone.utc
                        ) if fr.get("timestamp") else datetime.now(timezone.utc)
                        rate = Decimal(str(fr.get("fundingRate") or 0))
                        mark = Decimal(str(fr.get("markPrice") or 0)) if fr.get("markPrice") else None
                        idx = Decimal(str(fr.get("indexPrice") or 0)) if fr.get("indexPrice") else None
                        nft = fr.get("fundingDatetime")
                        nft_dt = None
                        if nft:
                            try:
                                nft_dt = datetime.fromisoformat(nft.replace("Z", "+00:00"))
                            except Exception:
                                nft_dt = None
                        snap_rows.append(
                            (db_sym, ts, rate, mark, idx, nft_dt, "snapshot")
                        )
                    except Exception as e:
                        self.log.warning(f"funding snapshot {csym} failed: {e}")
                if snap_rows:
                    try:
                        async with self._pool.acquire() as conn:
                            await conn.executemany(
                                """INSERT INTO funding
                                     (symbol,ts,rate,mark_price,index_price,
                                      next_funding_time,kind)
                                   VALUES ($1,$2,$3,$4,$5,$6,$7)
                                   ON CONFLICT (symbol,ts,kind) DO NOTHING""",
                                snap_rows,
                            )
                    except Exception as e:
                        self.log.error(f"snapshot funding insert failed: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            try:
                await ex.close()
            except Exception:
                pass

    # ═══════════════════════════════════════════════════════════
    # 刷写循环
    # ═══════════════════════════════════════════════════════════

    async def _flush_loop(self) -> None:
        """定期刷写循环：每隔 flush_interval_sec 秒将缓冲区数据写入数据库。"""
        while self._running:
            try:
                await asyncio.sleep(self._cfg.flush_interval_sec)
                await self._flush_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"flush_loop error: {e}")

    async def _final_flush_and_close(self) -> None:
        """最终刷写：停止后台任务，刷写所有剩余数据，关闭数据库连接池。"""
        # 先取消资金费率轮询，防止在池关闭后写入
        for t in (self._funding_task, self._flush_task):
            if t is not None and not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        await self._flush_all()
        if self._pool:
            await self._pool.close()
        self.log.info("DataManage pool closed")

    # ═══════════════════════════════════════════════════════════
    # 数据处理器
    # ═══════════════════════════════════════════════════════════

    def on_bar(self, bar: Bar) -> None:
        """NT 回调：收到新的 Bar 时触发。

        将 Bar 转换为元组格式存入 _bar_buf 缓冲区。
        如果缓冲区达到 max_buffer 上限，触发异步刷写。

        Bar 数据由 NT DataEngine 自动发布到消息总线，
        策略通过自己的消息总线订阅接收。不需要手动重发（会导致递归）。
        """
        try:
            tf = _bar_tf_label(bar.bar_type)                          # 时间周期标签 (如 "1m")
            symbol = bar.bar_type.instrument_id.symbol.value          # 品种代码 (如 "SOLUSDT-PERP")
            ts = _ns_to_dt(bar.ts_event)                              # 时间戳
            self._bar_buf.append(
                (
                    symbol,
                    tf,
                    ts,
                    Decimal(str(bar.open)),
                    Decimal(str(bar.high)),
                    Decimal(str(bar.low)),
                    Decimal(str(bar.close)),
                    Decimal(str(bar.volume)),
                    None,  # quote_volume: Bar 对象不含此字段
                    None,  # trades: Bar 对象不含此字段
                )
            )
            self._n_bars += 1
            if len(self._bar_buf) >= self._cfg.max_buffer and self._loop:
                self._loop.create_task(self._flush_bars())
        except Exception as e:
            self.log.error(f"on_bar error: {e}")

    def on_trade_tick(self, tick: TradeTick) -> None:
        """NT 回调：收到新的逐笔成交时触发。

        将 TradeTick 转换为元组格式存入 _tick_buf 缓冲区。
        包含 trade_id、价格、数量、吃单方、时间戳等信息。
        trade_id 尽量用原始数值，非数字则取哈希值。
        """
        try:
            symbol = tick.instrument_id.symbol.value
            self._tick_buf.append(
                (
                    symbol,
                    int(tick.trade_id.value) if str(tick.trade_id.value).isdigit() else hash(tick.trade_id.value) & 0x7FFFFFFFFFFFFFFF,
                    Decimal(str(tick.price)),
                    Decimal(str(tick.size)),
                    "BUY" if tick.aggressor_side.name == "BUYER" else "SELL",
                    _ns_to_dt(tick.ts_event),
                    _ns_to_dt(tick.ts_init),
                )
            )
            self._n_ticks += 1
            if len(self._tick_buf) >= self._cfg.max_buffer and self._loop:
                self._loop.create_task(self._flush_ticks())
        except Exception as e:
            self.log.error(f"on_trade_tick error: {e}")

    def on_funding_rate(self, fr) -> None:
        """NT 回调：收到新的资金费率更新时触发。

        BinanceDataClient 在 8 小时间隔边界发出此事件。
        以 kind='settled' 写入数据库（PnL 计算的权威来源）。
        """
        try:
            symbol = fr.instrument_id.symbol.value
            self._funding_buf.append(
                (
                    symbol,
                    _ns_to_dt(fr.ts_event),
                    Decimal(str(fr.rate)),
                    None,  # mark_price
                    None,  # index_price
                    None,  # next_funding_time (并非总是可用)
                    "settled",
                )
            )
            self._n_funding += 1
        except Exception as e:
            self.log.error(f"on_funding_rate error: {e}")

    def on_event(self, event) -> None:
        """NT 回调：通用事件处理，作为 msgbus 通配符订阅的兜底。

        如果 msgbus 订阅未生效，通过此回调捕获订单和持仓事件。
        """
        try:
            if isinstance(event, OrderEvent):
                self._enqueue_order_event(event)
            elif isinstance(event, PositionEvent):
                if self._loop:
                    self._loop.create_task(self._enqueue_position_event(event))
        except Exception as e:
            self.log.error(f"on_event error: {e}")

    def _on_order_event(self, event) -> None:
        """消息总线回调：订单事件。"""
        try:
            self._enqueue_order_event(event)
        except Exception as e:
            self.log.error(f"_on_order_event error: {e}")

    def _on_position_event(self, event) -> None:
        """消息总线回调：持仓事件。"""
        try:
            if self._loop:
                self._loop.create_task(self._enqueue_position_event(event))
        except Exception as e:
            self.log.error(f"_on_position_event error: {e}")

    # ═══════════════════════════════════════════════════════════
    # 事件 → 数据库写入
    # ═══════════════════════════════════════════════════════════

    def _enqueue_order_event(self, event: OrderEvent) -> None:
        """将订单事件加入异步写入队列。"""
        if not self._pool or not self._loop:
            return
        self._n_order_events += 1
        self._loop.create_task(self._write_order_event(event))

    async def _enqueue_position_event(self, event: PositionEvent) -> None:
        """将持仓事件加入异步写入队列（带互斥锁保护）。

        使用 _position_write_lock 防止 NETTING 模式下同一持仓 ID
        的并发写入导致的竞争条件。
        """
        if not self._pool or not self._loop:
            return
        self._n_position_events += 1
        async with self._position_write_lock:
            await self._write_position_event(event)

    def bind_base_strategy(self, base_strat) -> None:
        """注入 BaseStrategy 引用；落库时按 cid 反查策略实例（延迟取 executor，避开 on_start 时序）。"""
        self._base_strategy = base_strat

    def _instance_for_order(self, cid) -> str | None:
        """client_order_id -> 策略实例ID（AlphaV2-005/MacroV3-001）。无映射或未注入时返回 None。"""
        if not cid or not self._base_strategy:
            return None
        try:
            ex = self._base_strategy.get_executor()
            return ex.instance_for_cid(str(cid)) if ex else None
        except Exception:
            return None

    async def _emit_event_row(self, level: str, kind: str, payload: dict) -> None:
        """向 events 表插入一行事件记录，供 Telegram 事件监听器拾取。

        Args:
            level: 日志级别 (INFO / WARNING / ERROR)
            kind: 事件类型 (trade_fill / position_open / position_close)
            payload: 事件负载字典，将被序列化为 JSONB
        """
        if not self._pool:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO events (level,kind,component,payload)
                       VALUES ($1,$2,'nautilus',$3::jsonb)""",
                    level, kind, _safe_json(payload),
                )
        except Exception as e:
            self.log.error(f"emit_event {kind} failed: {e}")

    async def _write_order_event(self, event: OrderEvent) -> None:
        """将订单事件写入数据库。

        维护 orders 表和 fills 表：
        - orders: 包含订单的生命周期状态，使用 ON CONFLICT UPDATE 保持最新
        - fills: 包含成交明细，使用 ON CONFLICT DO NOTHING 防止重复

        对于 OrderFilled 事件，还会向 events 表写入 trade_fill 事件，
        供 Telegram 通知使用。
        """
        try:
            order = self.cache.order(event.client_order_id) if hasattr(self, "cache") else None
            if order is None:
                return
            symbol = order.instrument_id.symbol.value
            order_id = str(order.client_order_id)
            status = order.status.name
            side = order.side.name
            otype = order.order_type.name
            qty = Decimal(str(order.quantity))
            raw_price = getattr(order, "price", None)  # 市价单没有 price 字段
            price = Decimal(str(raw_price)) if raw_price is not None else None
            ts_sub = _ns_to_dt(order.ts_init)
            ts_upd = _ns_to_dt(event.ts_event)

            # 按 client_order_id 反查策略实例（AlphaV2-005/MacroV3-001）
            inst = self._instance_for_order(event.client_order_id)
            # ── 写入 orders 表 ──────────────────────────────────────
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO orders (order_id, client_id, instance_id, symbol, side, type,
                                         quantity, price, status, ts_submitted, ts_updated, raw)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
                    ON CONFLICT (order_id) DO UPDATE SET
                       status=EXCLUDED.status, ts_updated=EXCLUDED.ts_updated,
                       price=COALESCE(EXCLUDED.price, orders.price),
                       instance_id=COALESCE(EXCLUDED.instance_id, orders.instance_id),
                       raw=EXCLUDED.raw
                    """,
                    order_id, str(event.strategy_id) if event.strategy_id else None, inst,
                    symbol, side, otype, qty, price, status, ts_sub, ts_upd,
                    _safe_json({"event": type(event).__name__, "ts": ts_upd.isoformat()}),
                )

            # ── 如果已成交，写入 fills 表 ──────────────────────────
            if isinstance(event, OrderFilled):
                fill_id = str(event.trade_id) if event.trade_id else f"{order_id}-{event.ts_event}"
                fill_price = Decimal(str(event.last_px))
                fill_qty = Decimal(str(event.last_qty))
                fee = Decimal(str(event.commission.as_decimal())) if event.commission else Decimal(0)
                fee_ccy = event.commission.currency.code if event.commission else None
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO fills (fill_id, order_id, symbol, side, price,
                                            quantity, fee, fee_currency, ts_event, raw)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
                        ON CONFLICT (fill_id) DO NOTHING
                        """,
                        fill_id, order_id, symbol, side,
                        fill_price, fill_qty, fee, fee_ccy,
                        _ns_to_dt(event.ts_event),
                        _safe_json({"liquidity": event.liquidity_side.name if event.liquidity_side else None}),
                    )
                # 向 Telegram 推送成交事件
                await self._emit_event_row("INFO", "trade_fill", {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(fill_qty),
                    "price": str(fill_price),
                    "notional": str(fill_qty * fill_price),
                    "fee": str(fee),
                    "fee_ccy": fee_ccy,
                    "instance_id": inst,
                    "strategy_id": str(event.strategy_id) if event.strategy_id else None,
                    "liquidity": event.liquidity_side.name if event.liquidity_side else None,
                })
        except Exception as e:
            self.log.error(f"write_order_event failed: {e}")

    async def _write_position_event(self, event: PositionEvent) -> None:
        """将持仓事件写入数据库。

        数据来源策略 (v2, 2026-06-10):
        - PositionOpened:  从缓存 (cache) 读取 —— 新生命周期，不会有后续事件覆盖
        - PositionChanged: 从事件 (event) 读取 —— 缓存可能已被后续 PositionOpened 更新
                            (NETTING 复用同一 nt_position_id)
        - PositionClosed:  从事件 (event) 读取 —— 同上竞态条件。
                           事件携带不可变的生命周期快照 (ts_opened, ts_closed,
                           avg_px_close, realized_pnl)

        NETTING 模式陷阱：
        NETTING OMS 下，同一 NT position_id 在每次 FLAT 后会被重用。
        PositionOpened 每次触发新的生命周期，因此必须用
        (nt_position_id, opened_at) 复合键区分。
        """
        try:
            pos = self.cache.position(event.position_id) if hasattr(self, "cache") else None
            if pos is None:
                return

            nt_pos_id = str(event.position_id)
            is_opened = isinstance(event, PositionOpened)

            # ── 解析数据来源 ─────────────────────────────────────────
            if is_opened:
                # 从缓存读取：新生命周期，不可能被覆盖
                symbol = pos.instrument_id.symbol.value
                side = pos.side.name
                qty = Decimal(str(pos.quantity))
                avg_px = Decimal(str(pos.avg_px_open)) if pos.avg_px_open else None
                realized = Decimal(str(pos.realized_pnl.as_decimal())) if pos.realized_pnl else Decimal(0)
                opened_at = _ns_to_dt(pos.ts_opened)
                closed_at = None
            else:
                # PositionChanged / PositionClosed:
                # 从 EVENT 读取，而非缓存。缓存可能已经反映了一个新的生命周期
                # (如果 nt_position_id 在锁获取前被重新打开)。
                # 事件携带了正被关闭/变更的生命周期的不可变快照。
                symbol = event.instrument_id.symbol.value
                side = event.side.name
                qty = Decimal(str(event.quantity))
                avg_px = Decimal(str(event.avg_px_open)) if event.avg_px_open else None
                if hasattr(event, 'realized_pnl') and event.realized_pnl is not None:
                    realized = Decimal(str(event.realized_pnl.as_decimal()))
                else:
                    realized = Decimal(0)
                opened_at = _ns_to_dt(event.ts_opened)
                closed_at = _ns_to_dt(event.ts_closed) if event.ts_closed else None

            unrealized = Decimal(0)  # 如果需要，可通过组合快照获取；事件上不提供

            # 从事件中提取策略 ID，支持按策略查询 PnL
            # 在 2026-05-23 之前，每行 strategy_id 均为 NULL
            strat_id_str = str(event.strategy_id) if event.strategy_id else None
            strat_db_id: int | None = None
            if strat_id_str:
                # Strategy.id 格式: "<名称>-<DB ID>"，DB ID 为 0 填充
                tail = strat_id_str.rsplit("-", 1)[-1]
                if tail.isdigit():
                    strat_db_id = int(tail)

            # 反查策略实例：开仓单 cid（PositionOpened）/ 平仓单 cid（PositionClosed）
            if isinstance(event, PositionOpened):
                _inst = self._instance_for_order(getattr(event, "opening_order_id", None))
            elif isinstance(event, PositionClosed):
                _inst = self._instance_for_order(getattr(event, "closing_order_id", None))
            else:
                _inst = None

            # ── 写入数据库 ──────────────────────────────────────────
            # NETTING 陷阱：同一 NT position_id 每次 FLAT 后重用。
            # PositionOpened 触发新的生命周期，所以用 (nt_position_id, opened_at)
            # 作为复合键区分。
            async with self._pool.acquire() as conn:
                if is_opened:
                    await conn.execute(
                        """
                        INSERT INTO positions
                            (nt_position_id, symbol, strategy_id, side,
                             quantity, avg_price, realized_pnl, opened_at, instance_id, raw)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
                        ON CONFLICT (nt_position_id, opened_at)
                        DO UPDATE SET
                            side=EXCLUDED.side,
                            quantity=EXCLUDED.quantity,
                            avg_price=EXCLUDED.avg_price,
                            realized_pnl=EXCLUDED.realized_pnl,
                            instance_id=COALESCE(EXCLUDED.instance_id, positions.instance_id)
                        """,
                        nt_pos_id, symbol, strat_db_id, side,
                        qty, avg_px, realized, opened_at, _inst,
                        _safe_json({"position_id": nt_pos_id,
                                    "event": "PositionOpened"}),
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE positions
                           SET side=$3, quantity=$4,
                               avg_price=COALESCE($5, avg_price),
                               realized_pnl=$6, unrealized_pnl=$7,
                               closed_at=$8
                         WHERE nt_position_id=$1 AND opened_at=$2
                        """,
                        nt_pos_id, opened_at, side, qty, avg_px,
                        realized, unrealized, closed_at,
                    )

            # ── 向 Telegram 推送事件 ─────────────────────────────────
            if isinstance(event, PositionOpened):
                await self._emit_event_row("INFO", "position_open", {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(qty),
                    "avg_price": str(avg_px) if avg_px else None,
                    "instance_id": _inst,
                    "strategy_id": str(event.strategy_id) if event.strategy_id else None,
                })
            elif isinstance(event, PositionClosed):
                evt_closed = event.ts_closed or 0
                evt_opened = event.ts_opened or 0
                duration = (
                    (evt_closed - evt_opened) / 1_000_000_000
                    if evt_closed and evt_opened else 0
                )
                await self._emit_event_row("INFO", "position_close", {
                    "symbol": symbol,
                    "side_was": event.entry.name if hasattr(event, "entry") and event.entry else None,
                    "qty_peak": str(Decimal(str(event.peak_qty))) if event.peak_qty else None,
                    "avg_open": str(Decimal(str(event.avg_px_open))) if event.avg_px_open else None,
                    "avg_close": str(Decimal(str(event.avg_px_close))) if event.avg_px_close else None,
                    "realized_pnl": str(realized),
                    "duration_sec": round(duration, 1),
                    "instance_id": _inst,
                    "strategy_id": str(event.strategy_id) if event.strategy_id else None,
                })
        except Exception as e:
            self.log.error(f"write_position_event failed: {e}")

    # ═══════════════════════════════════════════════════════════
    # 订单薄增量处理
    # ═══════════════════════════════════════════════════════════

    def on_order_book_deltas(self, deltas) -> None:
        """NT 回调：收到订单薄增量时触发。

        维护内存中的 L2 订单薄状态 (_books)：
        - deltas.deltas: list[OrderBookDelta]
        - delta.order: BookOrder(price, size, side)
        - delta.is_delete: bool (是否删除该档位)

        每 1 秒由 _l2_snapshot_loop 从 _books 生成快照写入数据库。
        """
        try:
            sid = str(deltas.instrument_id)
            book = self._books.get(sid)
            if book is None:
                return
            for delta in deltas.deltas:
                try:
                    order = delta.order
                    p, s = float(order.price), float(order.size)
                    sk = "bids" if str(order.side) == "BUY" else "asks"
                    if delta.is_delete or s == 0.0:
                        book[sk].pop(p, None)
                    else:
                        book[sk][p] = s
                except Exception:
                    pass
        except Exception as e:
            self.log.error(f"on_order_book_deltas error: {e}")

    # ═══════════════════════════════════════════════════════════
    # L2 快照循环
    # ═══════════════════════════════════════════════════════════

    async def _l2_snapshot_loop(self) -> None:
        """L2 快照生成循环。

        每隔 l2_snapshot_interval_sec 秒，从 _books 中提取 TOP 10 档
        买方和卖方深度数据，存入 _l2_buf 缓冲区。
        如果缓冲达到上限则触发刷写。
        """
        self.log.info(f"L2 snapshot loop starting (every {self._cfg.l2_snapshot_interval_sec}s)")
        try:
            from datetime import datetime, timezone
            while self._running:
                try:
                    await asyncio.sleep(self._cfg.l2_snapshot_interval_sec)
                    if not self._running:
                        break
                    now = datetime.now(timezone.utc)
                    for sid, book in self._books.items():
                        bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:10]
                        asks = sorted(book["asks"].items(), key=lambda x: x[0])[:10]
                        for i, (price, size) in enumerate(bids, 1):
                            self._l2_buf.append((now, sid, "bid", i, price, size))
                        for i, (price, size) in enumerate(asks, 1):
                            self._l2_buf.append((now, sid, "ask", i, price, size))
                        self._n_l2 += 20
                    if len(self._l2_buf) >= self._cfg.max_buffer:
                        await self._flush_l2()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.log.error(f"L2 snapshot error: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            self.log.info("L2 snapshot loop stopped.")

    # ═══════════════════════════════════════════════════════════
    # 持仓量轮询
    # ═══════════════════════════════════════════════════════════

    async def _oi_poll_loop(self) -> None:
        """持仓量轮询循环。

        每 oi_poll_interval_sec 秒直接调用 Binance REST API
        (fapi/v1/openInterest) 获取 SOLUSDT 的未平仓合约量。
        """
        self.log.info(f"OI polling starting (every {self._cfg.oi_poll_interval_sec}s)")
        try:
            import urllib.request
            while self._running:
                try:
                    await asyncio.sleep(self._cfg.oi_poll_interval_sec)
                    if not self._running:
                        break
                    url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=SOLUSDT"
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        data = json.loads(resp.read())
                    if "openInterest" in data and "time" in data:
                        ts = datetime.fromtimestamp(data["time"] / 1000, tz=timezone.utc)
                        self._oi_buf.append((ts, "SOLUSDT-PERP", float(data["openInterest"])))
                        self._n_oi += 1
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.log.error(f"OI poll error: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            self.log.info("OI polling stopped.")

    # ═══════════════════════════════════════════════════════════
    # 刷写函数
    # ═══════════════════════════════════════════════════════════

    async def _flush_l2(self) -> None:
        """将 L2 快照缓冲区写入 l2_snapshots 表。"""
        if not self._pool or not self._l2_buf:
            return
        buf = self._l2_buf
        self._l2_buf = []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO l2_snapshots (ts, symbol, side, level, price, size) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
                    buf)
        except Exception as e:
            self.log.error(f"L2 flush failed ({len(buf)} rows): {e}")

    async def _flush_oi(self) -> None:
        """将 OI 缓冲区写入 open_interest 表。"""
        if not self._pool or not self._oi_buf:
            return
        buf = self._oi_buf
        self._oi_buf = []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO open_interest (ts, symbol, value) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    buf)
        except Exception as e:
            self.log.error(f"OI flush failed ({len(buf)} rows): {e}")

    async def _flush_all(self) -> None:
        """刷写所有数据类型的缓冲区。"""
        await self._flush_bars()
        await self._flush_ticks()
        await self._flush_funding()
        if self._cfg.collect_l2:
            await self._flush_l2()
        if self._cfg.collect_oi:
            await self._flush_oi()

    async def _flush_bars(self) -> None:
        """将 Bar 缓冲区写入 bars 表。

        使用 ON CONFLICT (symbol,timeframe,ts) DO NOTHING 防止重复写入。
        """
        if not self._pool or not self._bar_buf:
            return
        batch, self._bar_buf = self._bar_buf, []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO bars (symbol,timeframe,ts,open,high,low,close,volume,quote_volume,trades)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                       ON CONFLICT (symbol,timeframe,ts) DO NOTHING""",
                    batch,
                )
        except Exception as e:
            self.log.error(f"flush_bars failed ({len(batch)} rows): {e}")

    async def _flush_ticks(self) -> None:
        """将 Tick 缓冲区写入 ticks 表。

        使用 ON CONFLICT (symbol,trade_id,ts_event) DO NOTHING 防止重复写入。
        """
        if not self._pool or not self._tick_buf:
            return
        batch, self._tick_buf = self._tick_buf, []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO ticks (symbol,trade_id,price,size,aggressor,ts_event,ts_init)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)
                       ON CONFLICT (symbol,trade_id,ts_event) DO NOTHING""",
                    batch,
                )
        except Exception as e:
            self.log.error(f"flush_ticks failed ({len(batch)} rows): {e}")

    async def _flush_funding(self) -> None:
        """将资金费率缓冲区写入 funding 表。

        使用 ON CONFLICT (symbol,ts,kind) DO NOTHING 防止重复写入。
        """
        if not self._pool or not self._funding_buf:
            return
        batch, self._funding_buf = self._funding_buf, []
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO funding
                         (symbol,ts,rate,mark_price,index_price,
                          next_funding_time,kind)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)
                       ON CONFLICT (symbol,ts,kind) DO NOTHING""",
                    batch,
                )
        except Exception as e:
            self.log.error(f"flush_funding failed ({len(batch)} rows): {e}")
