# nt-base Design Spec

## Purpose

nt-base is the sole Binance-connected process on the server. It owns market data ingestion, timescale persistence, factor computation, strategy hosting, risk management, and order execution. Everything else (backtest, RD-Agent, strategies) connects to it or reads from the database it populates.

## Architecture

```
Binance WS ──► DataManageActor ──► TimescaleDB (1s/5s/1m bars, ticks, funding)
                    │
               on_bar() callback:
                    │
               ├── 1s bar → risk_loop.check(slot)  [每槽位止损/止盈/超时/熔断]
               ├── 5s bar → 写库 (future: inference tick)
               └── 1m bar → factor_engine.compute() → 附因子
                         → for slot in slots: slot.strategy.on_bar(bar)
                         → signal = slot.strategy.output()
                         → executor.execute(slot, signal, price)
                              ├── 风控 gate (tripped/cooldown/direction)
                              ├── NT order (entry/flat/reverse)
                              └── 更新 slot 状态
```

No DB polling. No extra WebSocket connections. One code path for all bar processing.

## Components

### 1. DataManageActor (`base/data_manage.py`)
- Copied from trading-infra, owns Binance WS connection
- Writes bars (1s/5s/1m), ticks, funding to TimescaleDB
- Calls registry hooks on bar arrival
- Graceful disconnect on shutdown

### 2. Registry (`base/registry.py`)
- `register(slot)` — add strategy slot, update factor_index
- `unregister(strategy_id)` — flat positions, remove slot
- `get_slots(symbol, timeframe)` — which strategies need this bar
- `all_slots()` — for risk loop iteration
- factor_index: `{factor_name: {strategy_id, ...}}` — only compute subscribed factors

### 3. StrategySlot (`base/slot.py`)
- Declarative: subscriptions, risk params, position params
- Runtime state: has_position, entry_price, entry_side, tripped
- Strategies never mutate their own slot directly — only via signal return

### 4. Factor Engine (`factor/compute.py`)
- Copied from trading-v2, same code
- Called from DataManageActor.on_bar(1m) with factor_index
- Computes only factors that have subscribers
- Result attached to bar before pushing to strategies

### 5. Risk Checker (`risk/checker.py`)
- Pure functions: `check_stop(slot, price)`, `check_take(slot, price)`, `check_hold(slot, now)`, `check_daily(slot)`
- Used by both nt-base (1s loop) and trading-v2 backtest (per-bar check)
- Same code, same logic, same parameters

### 6. Risk Loop (`risk/loop.py`)
- 1s interval, iterates all slots with positions
- Calls risk/checker functions
- Triggers executor.flat() on breach

### 7. Executor (`base/executor.py`)
- `execute(slot, signal, price)` → SignalAck
- Gates: tripped, cooldown, same-direction
- Actions: entry, flat, reverse — via NT order

### 8. Signal Protocol (`base/signal_protocol.py`)
- `SignalStrategy` Protocol: `on_bar(bar) → Signal`
- `BarSubscription`: {symbol, timeframe, factors}
- `StrategySignal`: {direction: -1/0/+1, reason: str}

### 9. Trading Node (`base/trading_node.py`)
- Builds NT TradingNodeConfig with SandboxExecutionClient
- No BinanceDataClient (DataManageActor handles data)
- Sandbox mode by default, live with sub-account config

### 10. Entrypoint (`main.py`)
- Load env, build node, create DataManageActor, init registry
- Register default strategies
- Start risk loop
- Run until SIGTERM, then graceful shutdown

## File Structure

```
nt-base/
├── main.py
├── .env
├── .gitignore
│
├── base/
│   ├── data_manage.py
│   ├── trading_node.py
│   ├── registry.py
│   ├── executor.py
│   ├── slot.py
│   └── signal_protocol.py
│
├── risk/
│   ├── checker.py
│   └── loop.py
│
├── shared/          # copied from trading-v2
│   ├── db.py
│   ├── env.py
│   └── log.py
│
├── factor/          # copied from trading-v2
│   ├── compute.py
│   └── registry.py
│
├── factors/         # copied from trading-v2
│   ├── trend_regime.py
│   ├── cvd_divergence.py
│   └── residual_momentum.py
│
├── nt/              # copied from trading-v2
│   └── instruments.py
│
└── tests/
    ├── test_checker.py
    ├── test_registry.py
    └── test_executor.py
```

## Risk Parameters (per slot)

| Param | Default | Description |
|-------|---------|-------------|
| stop_pct | 0.03 | Stop-loss % from entry |
| take_pct | 0.06 | Take-profit % from entry |
| max_hold_sec | 3600 | Max position duration |
| max_daily_loss_pct | 0.05 | Circuit breaker |
| cooldown_sec | 60 | Min time between trades |
| leverage | 2 | Position leverage |
| position_size_pct | 0.20 | % of equity per trade |

## Shutdown Sequence

1. Stop accepting new strategy registrations
2. Notify all strategies: shutting down
3. For each slot with position: executor.flat(slot, "shutdown")
4. Wait max 30s for fills
5. DataManageActor disconnect
6. TradingNode dispose
7. Exit

## Testing

1. **checker unit tests**: stop/take/hold/daily gate logic
2. **registry unit tests**: register/unregister, factor_index correctness
3. **executor unit tests**: signal gating, cooldown enforcement
4. **integration**: AlphaSignal registered → 1h sandbox → verify trades
5. **shutdown test**: signal while in position → SIGTERM → verify flat

## Relationship with trading-v2

- trading-v2 reads TimescaleDB (populated by nt-base) for backtest
- trading-v2 copies risk/checker.py for consistent risk simulation
- trading-v2 rd_agent optimizes strategy params → nt-base loads result
- No runtime dependency between them
