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
    
    print("Deactivating current strategy AlphaV2-003...")
    await conn.execute("UPDATE strategy_instances SET status='stopping' WHERE instance_id='AlphaV2-003'")
    
    print("Inserting optimized AlphaStrategy v34 parameters as pending AlphaV2-004...")
    params = {
        'gate_factor': 'trend_regime',
        'factor_1': 'cvd_divergence',
        'direction_1': -1,
        'weight_1': 2.0,
        'factor_2': 'residual_momentum',
        'direction_2': 1,
        'weight_2': 0.5,
        'signal_threshold': 0.4,
        'atr_period': 30,
        'btc_shock_long': 0.0085,
        'btc_shock_short': 0.0075,
        'time_limit_long': 40,
        'time_limit_short': 18,
        'max_hold_minutes': 40,
        'breakeven_atr_mult': 1.4,
        'trail_trigger_atr': 2.0,
        'trail_stop_atr': 1.0,
        'stop_pct': 0.03,
        'take_pct': 0.06,
        'max_hold_sec': 3600,
        'cooldown_sec': 60.0,
        'leverage': 3,
        'position_size_pct': 0.2
    }
    
    token = '8730820649:AAGc1uH70e76480dWWcXaCrjhixmCLKDRNY'
    chat_id = '8491479697'
    
    await conn.execute(
        "INSERT INTO strategy_instances (instance_id, strategy_name, params, telegram_bot_token, telegram_chat_id, status) "
        "VALUES ('AlphaV2-004', 'AlphaSignal', $1, $2, $3, 'pending')",
        json.dumps(params), token, chat_id
    )
    
    print("Hot-swap commands registered successfully!")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(hot_swap())
