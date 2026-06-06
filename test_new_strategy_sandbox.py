"""Script to test order execution, position flattening, and Telegram notifications in sandbox for the newly activated AlphaV2-004."""
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from shared.env import cfg, assert_required
from shared.log import setup_logging
from shared.db import get_pool, close_pool
from base.trading_node import build_trading_node
from base.registry import StrategyRegistry
from base.registration import RegistrationManager
from main import BaseStrategy, SYMBOL


logger = setup_logging("test_new_strategy_sandbox")

async def main():
    assert_required()
    pool = await get_pool()

    logger.info("Initializing Nautilus Sandbox Trading Node for AlphaV2-004...")
    node = build_trading_node(
        api_key=cfg.binance.api_key,
        api_secret=cfg.binance.api_secret,
        leverage=3,
        initial_usdt=int(cfg.sandbox_initial_usdt),
    )

    registry = StrategyRegistry()
    base_strat = BaseStrategy(registry)
    node.trader.add_strategy(base_strat)

    node.build()

    # Load AlphaV2-004 credentials and slot setup
    logger.info("Loading AlphaV2-004 configuration from database...")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT params, telegram_bot_token, telegram_chat_id FROM strategy_instances WHERE instance_id='AlphaV2-004'"
        )

    if not row:
        logger.error("AlphaV2-004 instance not found in database!")
        await close_pool()
        return

    import json
    params = row["params"] if isinstance(row["params"], dict) else json.loads(row["params"] or "{}")
    token = row["telegram_bot_token"] or ""
    chat_id = row["telegram_chat_id"] or ""

    logger.info(f"Telegram configured: Bot Token={token[:15]}..., Chat ID={chat_id}")

    # Boot the trading node in the background
    logger.info("Starting Nautilus node engines...")
    loop = asyncio.get_running_loop()
    node_task = loop.create_task(node.run_async())

    # Wait for engines to reconcile and initialize (usually 2-3 seconds)
    await asyncio.sleep(5)

    executor = base_strat.get_executor()
    if not executor:
        logger.error("OrderExecutor not ready!")
        node.dispose()
        await close_pool()
        return

    # Build the strategy slot
    from base.v2_signal import AlphaSignal
    from base.v2_adapter import V2SignalAdapter
    from base.slot import StrategySlot

    logger.info("Constructing optimized v34 slot...")
    alpha = AlphaSignal(
        gate_factor=params.get("gate_factor", "trend_regime"),
        factor_1=params.get("factor_1", "cvd_divergence"),
        direction_1=params.get("direction_1", -1),
        weight_1=params.get("weight_1", 2.0),
        factor_2=params.get("factor_2", "residual_momentum"),
        direction_2=params.get("direction_2", 1),
        weight_2=params.get("weight_2", 0.5),
        signal_threshold=params.get("signal_threshold", 0.4),
        atr_period=params.get("atr_period", 30),
        btc_shock_long=params.get("btc_shock_long", 0.0085),
        btc_shock_short=params.get("btc_shock_short", 0.0075),
        time_limit_long=params.get("time_limit_long", 40),
        time_limit_short=params.get("time_limit_short", 18),
        max_hold_minutes=params.get("max_hold_minutes", 40),
        breakeven_atr_mult=params.get("breakeven_atr_mult", 1.4),
        trail_trigger_atr=params.get("trail_trigger_atr", 2.0),
        trail_stop_atr=params.get("trail_stop_atr", 1.0),
    )
    adapter = V2SignalAdapter(alpha, "AlphaV2-004-TEST", "SOLUSDT-PERP", "1m")
    slot = StrategySlot(
        strategy_id="AlphaV2-004-TEST",
        strategy=adapter,
        subscriptions=adapter.subscriptions,
        stop_pct=params.get("stop_pct", 0.03),
        take_pct=params.get("take_pct", 0.06),
        cooldown_sec=0.0,
        leverage=params.get("leverage", 3),
        position_size_pct=params.get("position_size_pct", 0.2),
        symbol="SOLUSDT-PERP",
        telegram_bot_token=token,
        telegram_chat_id=chat_id,
    )

    registry.register(slot)

    # 1. TRIGGER ORDER ENTRY (SHORT)
    logger.info("=" * 60)
    logger.info("STEP 1: Triggering SHORT Order Entry on AlphaV2-004 in Sandbox...")
    logger.info("=" * 60)
    from base.signal_protocol import StrategySignal
    sell_signal = StrategySignal(direction=-1, reason="composite=-0.421 regime=-1")
    
    # Execute sell order on Binance Sandbox
    res_entry = executor.execute(slot, sell_signal, current_price=140.0)
    logger.info(f"Entry Result: {res_entry}")
    logger.info("SHORT Entry complete. Check your Telegram for a notification!")

    # Wait 4 seconds for the user to check Telegram and see the buy message
    await asyncio.sleep(4)

    # 2. TRIGGER POSITION CLOSE (FLAT)
    logger.info("=" * 60)
    logger.info("STEP 2: Triggering Position Flatting (CLOSE) on AlphaV2-004 in Sandbox...")
    logger.info("=" * 60)
    
    # Execute sell/flat order on Binance Sandbox
    res_flat = executor.flat(slot, reason="Trailing Stop triggered")
    logger.info(f"Flat Result: {res_flat}")
    logger.info("Position CLOSE complete. Check your Telegram for the exit message!")

    # Wait 2 seconds
    await asyncio.sleep(2)

    logger.info("Cleaning up and stopping node...")
    node_task.cancel()
    try:
        await node_task
    except asyncio.CancelledError:
        pass

    node.dispose()
    await close_pool()
    logger.info("Test complete!")

if __name__ == "__main__":
    asyncio.run(main())
