import asyncio
import asyncpg
import subprocess
import time
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from base.notify import send_message

async def check_pipeline():
    # 1. Check systemctl status of nt-base
    res = subprocess.run(["systemctl", "is-active", "nt-base"], capture_output=True, text=True)
    status_service = res.stdout.strip()

    # 2. Check Database
    conn = await asyncpg.connect(
        user='nautilus_admin', password='timescaledb_A2026cccvvv',
        database='trading_data', host='127.0.0.1'
    )
    
    # Get active strategy
    strat = await conn.fetchrow(
        "SELECT instance_id, status, activated_at FROM strategy_instances WHERE status='active' LIMIT 1"
    )
    strat_text = f"• {strat['instance_id']} (Activated: {strat['activated_at'].strftime('%Y-%m-%d %H:%M:%S')})" if strat else "• None"

    # Get open positions
    positions = await conn.fetch(
        "SELECT id, symbol, side, ROUND(quantity,4) as qty, strategy_id FROM positions WHERE closed_at IS NULL"
    )
    pos_lines = []
    for p in positions:
        pos_lines.append(f"• id={p['id']} {p['symbol']} {p['side']} qty={p['qty']} (strat={p['strategy_id']})")
    pos_text = "\n".join(pos_lines) if pos_lines else "• (None)"

    # Get recent orders (last 10m)
    orders = await conn.fetch(
        "SELECT symbol, side, type, status, ROUND(quantity,4) as qty, ts_submitted "
        "FROM orders WHERE ts_submitted > NOW() - INTERVAL '10 minutes' ORDER BY ts_submitted DESC"
    )
    ord_lines = []
    for o in orders:
        ord_lines.append(
            f"• {o['ts_submitted'].strftime('%M:%S')} {o['symbol']} {o['side']} {o['type']} -> {o['status']} ({o['qty']})"
        )
    ord_text = "\n".join(ord_lines) if ord_lines else "• (None)"

    await conn.close()

    # 3. Check logs for errors
    # Count error log occurrences in the last 100 lines
    err_res = subprocess.run(
        "tail -n 100 /root/nt-base/logs/nt_base.log | grep -i 'ERROR'",
        shell=True, capture_output=True, text=True
    )
    err_text = err_res.stdout.strip()
    err_status = "⚠️ Errors found in log!" if err_text else "✅ Log clean (no errors)"

    # 4. Construct Telegram message
    emoji = "🟢" if status_service == "active" and not err_text else "🔴"
    msg = (
        f"{emoji} <b>nt-base System Audit Report</b>\n\n"
        f"<b>Service</b>: {status_service.upper()}\n"
        f"<b>Health</b>: {err_status}\n\n"
        f"<b>Active Strategy</b>:\n{strat_text}\n\n"
        f"<b>Open Positions</b>:\n{pos_text}\n\n"
        f"<b>Recent Orders (Last 10m)</b>:\n{ord_text}\n\n"
        f"<i>Next audit in 10 minutes...</i>"
    )

    token = '8730820649:AAGc1uH70e76480dWWcXaCrjhixmCLKDRNY'
    chat_id = '8491479697'
    print(f"[{time.strftime('%H:%M:%S')}] Dispatched audit report to Telegram. Status={status_service}")
    await send_message(token, chat_id, msg)

async def main():
    print("Pipeline Monitor daemon started...")
    while True:
        try:
            await check_pipeline()
        except Exception as e:
            print(f"Error in monitor: {e}", file=sys.stderr)
        await asyncio.sleep(600)  # Check every 10 minutes

if __name__ == '__main__':
    asyncio.run(main())
