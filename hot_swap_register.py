import asyncio
import asyncpg
import json

async def hot_swap():
    conn = await asyncpg.connect(
        user='nautilus_admin',
        password='timescaledb_A2026cccvvv',
        database='trading_data',
        host='127.0.0.1'
    )

    # Deactivate current strategy
    print("Deactivating current strategy...")
    await conn.execute("UPDATE strategy_instances SET status='stopping' WHERE status='active'")

    # New AlphaV2 v2 params with adaptive modulation + 4 factors
    params = {
        # Factor config
        "gate_factor": "trend_regime",
        "factor_1": "cvd_divergence", "direction_1": -1, "weight_1": 2.0,
        "factor_2": "residual_momentum", "direction_2": 1, "weight_2": 0.5,
        "factor_3": "channel_breakout", "direction_3": -1, "weight_3": 1.0,

        # Entry/exit params
        "signal_threshold": 0.4,
        "atr_period": 30,
        "stop_pct": 0.03, "take_pct": 0.06,
        "max_hold_sec": 3600, "cooldown_sec": 60.0,
        "leverage": 3, "position_size_pct": 0.2,

        # BTC shock gates
        "btc_shock_long": 0.0085, "btc_shock_short": 0.0075,
        "time_limit_long": 40, "time_limit_short": 18,
        "max_hold_minutes": 40,

        # Exit layers
        "breakeven_atr_mult": 1.4,
        "trail_trigger_atr": 2.0, "trail_stop_atr": 1.0,

        # Adaptive modulation (RD-Agent search space)
        "adaptive": {
            "confidence_ceiling": 0.6,
            "cvd_attenuation": 0.7,
            "residual_amplification": 1.5,
            "breakout_amplification": 1.0,
            "threshold_sensitivity": 0.5,
            "size_floor": 0.25, "size_slope": 0.75,
            "stop_tighten_weak": 0.5,
            "take_tighten_weak": 0.6,
            "hold_shorten_weak": 0.5
        }
    }

    token = '8730820649:AAGc1uH70e76480dWWcXaCrjhixmCLKDRNY'
    chat_id = '8491479697'

    print("Inserting AlphaV2-005 (v2 upgrade with adaptive + 4 factors)...")
    await conn.execute(
        "INSERT INTO strategy_instances (instance_id, strategy_name, params, telegram_bot_token, telegram_chat_id, status) "
        "VALUES ('AlphaV2-005', 'AlphaSignal', $1, $2, $3, 'pending')",
        json.dumps(params), token, chat_id
    )

    print("Hot-swap registered: AlphaV2-005 will be activated within 5 seconds")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(hot_swap())
