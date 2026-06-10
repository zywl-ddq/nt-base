# -*- coding: utf-8 -*-
"""
prefill_bar_buffer.py -- 启动预热模块
========================================

功能
----
nt-base 启动时，从 ticks 表聚合历史 1 分钟 K 线，填充到 deque 缓冲区，
使策略在启动后立即获得足够的 bar 数据用于因子计算和信号判定。

为什么需要预热？
---------------
- 大多数因子（如 channel_breakout 需要 20 根 bar，trend_regime 需要 30 根）
  依赖历史窗口计算。
- 如果冷启动，需要等待 20~30 分钟才能积累足够 bar。
- 预热后，策略在第一根实时 bar 到达时即可生成信号。

SQL 查询逻辑
-----------
1. SOL 条查询：从 ticks 表按分钟聚合，计算 open/high/low/close/volume
   以及 taker_buy_volume 和 taker_sell_volume（用于 delta 计算）。
2. BTC 条查询：取同一时间范围内 BTC 的收盘价，用于因子计算
   （residual_momentum 依赖 btc_close）。
3. 结果按时间升序排列（DB 中 DESC + reversed）。

数据对齐策略
-----------
- BTC 时间戳精确匹配
- 若某分钟缺少 BTC 数据，使用 forward-fill（前向填充）：
  用最近一个已知 BTC 价格填补空缺，避免 NaN 传播到因子计算。
- ts_event 时区处理：DB 中带时区，转成 tz-naive 兼容 live bar。

返回格式
--------
collections.deque(maxlen=n_bars)
每个元素为 dict：
    ts, open, high, low, close, volume, delta,
    taker_buy_volume, taker_sell_volume, btc_close

同时返回 latest_btc（最后一个有效的 BTC 价格），用于初始化上下文。

作者: nt-base system
版本: 1.0.0
"""
"""Prefill bar buffer from tick data on nt-base startup."""
import asyncio
import asyncpg
from collections import deque
from datetime import datetime, timezone


async def prefill_bar_buffer(pool: asyncpg.Pool, n_bars: int = 300):
    """Aggregate last N 1m bars from ticks table, return deque ready for dispatch."""

    sol_query = """
    SELECT
        date_trunc('minute', ts_event) AS ts,
        FIRST(price, ts_event) AS open,
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price, ts_event) AS close,
        SUM(size) AS volume,
        SUM(CASE WHEN aggressor = 'BUY' THEN size ELSE 0 END) AS taker_buy_volume,
        SUM(CASE WHEN aggressor = 'SELL' THEN size ELSE 0 END) AS taker_sell_volume
    FROM ticks
    WHERE symbol = 'SOLUSDT-PERP'
    GROUP BY date_trunc('minute', ts_event)
    ORDER BY ts DESC
    LIMIT $1
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(sol_query, n_bars)

    if not rows:
        print("prefill: no tick data found, starting with empty buffer")
        return deque(maxlen=n_bars), 0.0

    rows = list(reversed(rows))  # chronological order

    # Fetch BTC close prices for same time range
    btc_query = """
    SELECT
        date_trunc('minute', ts_event) AS ts,
        LAST(price, ts_event) AS close
    FROM ticks
    WHERE symbol = 'BTCUSDT-PERP'
      AND ts_event >= $1 AND ts_event < $2
    GROUP BY date_trunc('minute', ts_event)
    ORDER BY ts
    """
    btc_map = {}
    try:
        async with pool.acquire() as conn:
            ts_start = rows[0]['ts']
            ts_end = rows[-1]['ts']
            btc_rows = await conn.fetch(btc_query, ts_start, ts_end)
            btc_map = {r['ts']: float(r['close']) for r in btc_rows}
    except Exception:
        pass

    buf = deque(maxlen=n_bars)
    latest_btc = 0.0
    for r in rows:
        ts = r['ts']
        buy_vol = float(r['taker_buy_volume'] or 0)
        sell_vol = float(r['taker_sell_volume'] or 0)
        delta = buy_vol - sell_vol
        btc_close = btc_map.get(ts)
        if btc_close and btc_close > 0:
            latest_btc = btc_close
        # Forward-fill: use last known BTC to avoid NaN gaps
        btc_for_bar = btc_close if (btc_close and btc_close > 0) else (latest_btc if latest_btc > 0 else None)

        # Store as tz-naive for compatibility with live bar ts_event
        ts_naive = ts.replace(tzinfo=None)
        buf.append({
            "ts": ts_naive,
            "open": float(r['open'] or r['close'] or 0),
            "high": float(r['high'] or r['close'] or 0),
            "low": float(r['low'] or r['close'] or 0),
            "close": float(r['close'] or 0),
            "volume": float(r['volume'] or 0),
            "delta": delta,
            "taker_buy_volume": buy_vol,
            "taker_sell_volume": sell_vol,
            "btc_close": btc_for_bar,
        })

    btc_matched = sum(1 for b in buf if b['btc_close'] is not None)
    print(f"prefill: loaded {len(buf)} SOL bars, {btc_matched} with BTC match")
    return buf, latest_btc


async def main():
    pool = await asyncpg.create_pool(
        user='nautilus_admin',
        password='timescaledb_A2026cccvvv',
        database='trading_data',
        host='127.0.0.1'
    )

    import time
    t0 = time.time()
    buf, btc = await prefill_bar_buffer(pool, 300)
    elapsed = time.time() - t0

    if buf:
        print(f"First: {buf[0]['ts']} close={buf[0]['close']:.2f} btc={buf[0].get('btc_close')}")
        print(f"Last:  {buf[-1]['ts']} close={buf[-1]['close']:.2f} btc={buf[-1].get('btc_close')}")
        avg_delta = sum(b['delta'] for b in buf) / len(buf)
        print(f"Avg delta: {avg_delta:.2f}, BTC latest: {btc:.2f}")
        print(f"Query time: {elapsed:.2f}s")

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
