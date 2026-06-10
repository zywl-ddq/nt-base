# -*- coding: utf-8 -*-
"""
===========================================================
模块:    base/registration
模块名:  策略热注册管理器
===========================================================
版本:    v2 (支持 factor_3 和自适应参数)
===========================================================
用途:    通过轮询 strategy_instances 数据库表实现策略的热注册和热注销。
         无需重启服务即可动态添加/移除交易策略。

类: RegistrationManager
  职责:
    1. 每 5 秒轮询 strategy_instances 表 (SELECT * ORDER BY id)
    2. 检测状态变化: pending/active -> 激活策略, stopping -> 注销策略
    3. 激活时更新状态为 'active' 并发送 Telegram 通知
    4. 注销时从 StrategyRegistry 移除策略实例
    5. 维护 _known 字典跟踪已知策略的最新状态，避免重复处理

状态转换:
    pending -> active   (首次激活: 之前未见过此策略)
    active  -> stopping (先更新状态为 'stopping'，随后注销)
    stopping-> stopped  (实际注销后更新)
    any     -> error    (激活失败时标记)

数据表: strategy_instances
    - instance_id: 策略实例 ID (主键)
    - status: pending/active/stopping/stopped/error
    - params: JSON 策略参数
    - telegram_bot_token: Telegram Bot Token (可选)
    - telegram_chat_id: Telegram 聊天 ID (可选)
    - activated_at: 激活时间戳
    - stopped_at: 停止时间戳
    - error_message: 错误信息

作者:    nt-base 系统
版本:    2.0.0 (支持 factor_3 和自适应参数)
===========================================================
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
import asyncpg

from base.notify import send_message, fmt_strategy_start

logger = logging.getLogger(__name__)

# 数据库轮询间隔 (秒)
POLL_SEC = 5


class RegistrationManager:
    """策略热注册管理器。

    通过轮询数据库 strategy_instances 表，
    自动检测新的策略实例 (pending/active) 并激活，
    检测到停止请求 (stopping) 则自动注销策略。

    此类是系统动态性的核心：在生产环境中添加/修改策略配置后，
    无需重启 nt-base 服务，只需在数据库中插入/更新行即可。
    """

    def __init__(self, registry, pool: asyncpg.Pool,
                 symbol: str = "SOLUSDT-PERP", timeframe: str = "1m"):
        """初始化注册管理器。

        Args:
            registry: StrategyRegistry 实例 (用于注册/注销策略)
            pool: asyncpg 数据库连接池
            symbol: 交易品种 (默认 SOLUSDT-PERP)
            timeframe: 时间周期 (默认 1m)
        """
        self._registry = registry
        self._pool = pool
        self._symbol = symbol
        self._timeframe = timeframe
        # 停止事件: 用于优雅停止轮询循环
        self._stop = asyncio.Event()
        # _known: 缓存已处理的策略实例状态
        # key: instance_id, value: status (如 "active", "stopped")
        # 用于检测状态变化，避免重复处理
        self._known: dict[str, str] = {}

    async def run(self):
        """主循环：每隔 POLL_SEC 秒轮询 strategy_instances 表。

        检查每行实例的状态变化，执行相应的激活或注销操作。
        """
        logger.info("RegistrationManager started: poll=%ss symbol=%s", POLL_SEC, self._symbol)
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception as e:
                logger.error("RegistrationManager tick error: %s", e, exc_info=True)
            try:
                # 等待 POLL_SEC 秒或直到 stop 事件被设置
                await asyncio.wait_for(self._stop.wait(), timeout=POLL_SEC)
            except asyncio.TimeoutError:
                pass

    async def stop(self):
        """停止轮询循环。"""
        self._stop.set()

    async def _tick(self):
        """单次轮询：查询所有策略实例并处理状态变化。

        状态变化处理逻辑:
        - pending (首次见到)  -> 激活 (_activate)
        - active (首次见到)   -> 激活 (_activate)
        - stopping (之前 active) -> 注销 (_deactivate)
        - stopped / error      -> 记录到 _known 但不做操作
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM strategy_instances ORDER BY id ASC"
            )

            for row in rows:
                iid = row["instance_id"]
                status = row["status"]
                prev = self._known.get(iid)

                # 状态未变化，跳过
                if prev == status:
                    continue

                # ── 激活条件 ──────────────────────────────────────
                # pending 和 active 都是"应该运行"的状态
                if status == "pending" and prev is None:
                    await self._activate(conn, row)
                elif status == "active" and prev is None:
                    await self._activate(conn, row)
                # ── 注销条件 ──────────────────────────────────────
                # 只有从 active -> stopping 才执行注销
                elif status == "stopping" and prev == "active":
                    await self._deactivate(conn, row)
                # ── 其他状态 ──────────────────────────────────────
                # stopped / error: 只记录到 _known，不处理
                elif status in ("stopped", "error"):
                    self._known[iid] = status

    async def _activate(self, conn, row):
        """激活一个策略实例。

        流程:
        1. 解析参数 JSON
        2. 获取 Telegram 通知配置 (token + chat_id)
        3. 更新数据库状态为 'active'
        4. 发送 Telegram 通知 (策略启动消息)
        5. 记录到 _known

        注意: 实际的策略注册 (注册到 gRPC 服务器) 由外部调用者完成。
        本方法只管理数据库状态和通知。
        """
        iid = row["instance_id"]
        params = row["params"] if isinstance(row["params"], dict) else json.loads(row["params"] or "{}")
        token = row["telegram_bot_token"] or ""
        chat_id = row["telegram_chat_id"] or ""

        logger.info("Activating %s (gRPC path): params=%s", iid, json.dumps(params, default=str)[:200])

        try:
            # 更新数据库状态为 active
            await conn.execute(
                "UPDATE strategy_instances SET status='active', activated_at=$2 WHERE instance_id=$1",
                iid, datetime.now(timezone.utc),
            )

            # 如果配置了 Telegram 通知，发送策略启动消息
            if token and chat_id:
                await send_message(token, chat_id, fmt_strategy_start(
                    iid, self._symbol,
                    params.get("leverage", 2),
                    params.get("position_size_pct", 0.20),
                ))

            self._known[iid] = "active"
            logger.info("Activated %s (gRPC path) - start with: python run_live.py %s", iid, iid)

        except Exception as e:
            logger.error("Failed to activate %s: %s", iid, e, exc_info=True)
            # 标记为 error 状态
            self._known[iid] = "error"
            await conn.execute(
                "UPDATE strategy_instances SET status='error', error_message=$2 WHERE instance_id=$1",
                iid, str(e)[:500],
            )

    async def _deactivate(self, conn, row):
        """注销一个策略实例。

        流程:
        1. 从 StrategyRegistry 中注销策略 (unregister)
        2. 更新数据库状态为 'stopped'
        3. 设置 stopped_at 时间戳
        4. 记录到 _known

        注意: 注销后，策略的信号将不再被处理，现有持仓需要手动管理。
        """
        iid = row["instance_id"]
        logger.info("Deactivating %s", iid)
        try:
            # 从注册表中移除策略
            # 这会导致策略停止接收新的 bar 数据和因子值
            self._registry.unregister(iid)
            self._known[iid] = "stopped"
            await conn.execute(
                "UPDATE strategy_instances SET status='stopped', stopped_at=$2 WHERE instance_id=$1",
                iid, datetime.now(timezone.utc),
            )
            logger.info("Deactivated %s", iid)
        except Exception as e:
            logger.error("Failed to deactivate %s: %s", iid, e, exc_info=True)
            await conn.execute(
                "UPDATE strategy_instances SET status='error', error_message=$2 WHERE instance_id=$1",
                iid, str(e)[:500],
            )
