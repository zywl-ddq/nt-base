"""
Module:    shared/db
Purpose:   Database connection pool management for TimescaleDB.
           Provides singleton pool lifecycle (create/close) shared across all modules.
Interface: get_pool() -> asyncpg.Pool
           close_pool() -> None
Dependencies: asyncpg, shared.env
Author:    nt-base system
Version:   1.0.0
Security:  Pool credentials sourced from environment (cfg.timescale.dsn), never hardcoded.
"""
from __future__ import annotations
"""
共享的 asyncpg 数据库连接池模块。所有需要数据库访问的组件都通过此模块获取连接。

本模块实现了以下核心功能：
  1. 单例连接池：全局只有一个 asyncpg.Pool 实例，避免重复创建连接。
  2. 懒加载：连接池在第一次调用 get_pool() 时才真正创建，不会在导入时初始化。
  3. 线程安全：通过 asyncio.Lock 保证并发场景下不会重复创建连接池。
  4. 健康检查：提供 healthcheck() 函数验证数据库连接是否可用。
  5. 池化管理：支持配置最小/最大连接数（min_size/max_size），自动管理连接生命周期。

设计决策：
  - 不使用全局变量以外的方式管理连接池，保持简单。
  - 连接凭据从 shared.env.cfg.timescale.dsn 获取，安全可靠。
  - 日志通过标准 logging 输出，便于问题排查。
"""


import asyncio
import logging
from typing import Optional

import asyncpg

from shared.env import cfg

logger = logging.getLogger(__name__)

# 全局连接池实例，初始为 None，在第一次调用 get_pool() 时创建
# 使用模块级全局变量实现单例模式
_pool: Optional[asyncpg.Pool] = None
# 异步锁，防止多个协程同时初始化连接池（双重检查锁定模式）
_lock = asyncio.Lock()


async def get_pool(min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    """
    获取数据库连接池（懒加载单例）。

    采用双重检查锁定（Double-Checked Locking）模式：
      1. 第一次检查：如果 _pool 已存在，直接返回（无锁快速路径）。
      2. 加锁：确保只有一个协程进入初始化代码块。
      3. 第二次检查：在锁内再次检查 _pool，防止等待锁期间已经被其他协程初始化。
      4. 创建连接池：调用 asyncpg.create_pool() 建立到 TimescaleDB 的连接池。

    参数:
        min_size: 连接池最小连接数（默认: 2），连接池会始终保留至少 min_size 个连接。
        max_size: 连接池最大连接数（默认: 10），并发查询高时自动扩容到此上限。

    返回:
        asyncpg.Pool: 数据库连接池实例。

    注意:
        连接池的 DSN 从 shared.env.cfg.timescale.dsn 自动获取，
        包含 host, port, user, password, database 等完整信息。
    """
    global _pool
    if _pool is not None:
        return _pool
    async with _lock:
        if _pool is None:
            _pool = await asyncpg.create_pool(
                dsn=cfg.timescale.dsn,
                min_size=min_size,
                max_size=max_size,
            )
            logger.info(
                "asyncpg pool ready: %s:%s/%s",
                cfg.timescale.host,
                cfg.timescale.port,
                cfg.timescale.database,
            )
    return _pool


async def close_pool() -> None:
    """
    关闭数据库连接池。

    在以下场景调用：
      - 应用正常关闭时（如接收到 SIGTERM 信号）。
      - 需要重置数据库连接时（如连接池异常后重建）。

    关闭后 _pool 被置为 None，下次调用 get_pool() 时会重新创建连接池。
    多次调用是安全的（幂等操作）。
    """
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("asyncpg pool closed")


async def healthcheck() -> bool:
    """
    数据库健康检查。

    执行 SELECT 1 查询验证数据库连接是否正常：
      1. 通过 get_pool() 获取连接池（如果未初始化，会首次创建）。
      2. 从池中获取一个连接。
      3. 执行 SELECT 1 查询。
      4. 检查返回值是否为 1。

    返回:
        bool: True 表示数据库连接正常，False 表示异常。

    用途:
        - 在风控循环中定期调用，检测数据库是否可用。
        - 在系统启动时作为就绪检查的一部分。
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        v = await conn.fetchval("SELECT 1")
    return v == 1
