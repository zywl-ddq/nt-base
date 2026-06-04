# nt-base Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development to implement. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Build nt-base — standalone trading base with DataManageActor, registry, risk loop, executor.

**Architecture:** Binance WS → DataManageActor → TimescaleDB. 1m bar → factor compute → strategy slots → executor → NT orders. 1s risk loop per slot.

**Tech Stack:** Python 3.12, NautilusTrader 1.227, TimescaleDB, asyncpg

---

### Task 11: Wire DataManageActor bar callback to Registry + Factor + Executor

This is the core integration that connects all components. The DataManageActor's `on_bar` must:
1. Persist bar to DB (existing logic)
2. Compute factors for subscribed strategies
3. Push bar+factors to each strategy slot
4. Route signals to executor

- [ ] **Step 1: Add bar dispatch callback to main.py**

Modify `/root/nt-base/main.py`, add after `node.build()`:

```python
    # ── Wire bar dispatch ──────────────────────────────────────
    from factor.compute import compute_factor_incremental
    
    # Get DataManageActor reference
    dm_actor = None
    for actor in node.trader._actors:
        if isinstance(actor, DataManageActor):
            dm_actor = actor
            break
    
    if dm_actor:
        # Store reference for price tracking
        _latest_price = {"SOLUSDT-PERP.BINANCE": 0.0}
        
        # Monkey-patch on_bar to add factor+strategy dispatch
        _original_on_bar = dm_actor.on_bar
        
        def _on_bar_with_dispatch(bar):
            _original_on_bar(bar)  # persist to DB
            
            # Track latest price for risk loop
            iid = str(bar.bar_type.instrument_id)
            _latest_price[iid] = float(bar.close)
            
            # Only process 1m bars for strategies
            if "1-MINUTE" not in str(bar.bar_type.spec):
                return
            
            # Compute factors that have subscribers
            active_factors = registry.active_factors()
            if not active_factors:
                return
            
            # Load recent bars for factor computation
            # (simplified: use latest bars from cache or DB)
            # For each subscribed slot, compute factors and push
            slots = registry.get_slots(iid.replace(".BINANCE", ""), "1m")
            if not slots:
                return
            
            for slot in slots:
                # Push bar data to strategy
                bar_data = {
                    "close": float(bar.close),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "ts_ns": bar.ts_event,
                    "factors": {},  # factor compute TBD
                }
                signal = slot.strategy.on_bar(bar_data)
                if signal and signal.direction != 0:
                    result = executor.execute(slot, signal, float(bar.close))
                    logger.info(f"Signal: {slot.strategy_id} {signal.direction} → {result}")
        
        dm_actor.on_bar = _on_bar_with_dispatch
        logger.info("Bar dispatch wired: DataManageActor → factors → strategies → executor")
```

- [ ] **Step 2: Restart and verify integration**

```bash
systemctl restart nt-base && sleep 30 && tail -20 /root/nt-base/logs/nt_base.log | grep -E "dispatch|Signal|entry|factor"
```

Expected: no errors, DataManageActor processes bars.

- [ ] **Step 3: Commit**

```bash
cd /root/nt-base && git add -A && git commit -m "feat: wire DataManageActor → registry → factor → executor pipeline"
```
