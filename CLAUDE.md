# nt-base — Trading Base Service

## 项目概述

基于 NautilusTrader 的加密货币自动交易系统，运行在 Binance USDT Futures（沙盘模式）。核心功能：实时行情采集、因子计算、信号生成、自动下单、风控止损、Telegram 通知。

## 服务器信息

| 项目 | 值 |
|------|-----|
| 日本服务器 | `208.87.207.228:58453` root / `A2026cccvvv` |
| 香港服务器 | `103.142.190.90:26743` root / `cccvvvA2026` |
| 服务管理 | `systemctl start/stop/restart nt-base` |
| 应用目录 | `/root/nt-base` |
| Python环境 | `/root/miniconda3/envs/nautilus/bin/python` |
| 数据库 | TimescaleDB (Docker容器 `timescaledb`)，本地5432端口 |
| DB连接 | `nautilus_admin` / `timescaledb_A2026cccvvv` / `trading_data` |

## 技术栈

- **NautilusTrader** 1.227.0 — 交易引擎（行情+沙盘执行）
- **Binance Futures API** — 实时行情（WebSocket）、沙盘交易
- **TimescaleDB** (asyncpg 0.31) — 时序数据存储（ticks, bars, fills）
- **pandas** 2.3 / **numpy** 2.4 / **scipy** 1.17 — 因子计算
- **systemd** — 进程守护，失败自动重启

## 项目结构

```
/root/nt-base/
├── main.py                  # 入口：启动TradingNode、数据订阅、因子分发、策略调度
├── .env                     # 环境变量（API密钥、DB、Telegram）
├── base/
│   ├── data_manage.py       # DataManageActor：行情订阅+入库
│   ├── trading_node.py      # TradingNode工厂：Binance数据+Sandbox执行
│   ├── registry.py          # StrategyRegistry：策略槽管理+因子索引
│   ├── slot.py              # StrategySlot：策略运行时状态
│   ├── registration.py      # RegistrationManager：DB驱动的策略热注册
│   ├── executor.py          # OrderExecutor：下单+平仓+动态仓位
│   ├── v2_signal.py         # AlphaSignal v3：信号生成+四层退出
│   ├── v2_adapter.py        # V2SignalAdapter：协议适配
│   ├── signal_protocol.py   # SignalStrategy协议定义
│   └── notify.py            # Telegram通知
├── factor/
│   ├── compute.py           # 因子计算引擎：加载+执行+reindex
│   └── registry.py          # 因子目录：名称→定义映射
├── factors/
│   ├── cvd_divergence.py    # CVD背离因子（order-flow）
│   ├── residual_momentum.py # 残差动量因子（SOL vs BTC beta剥离）
│   ├── channel_breakout.py  # 通道突破因子（trend-following）
│   └── trend_regime.py      # 趋势状态因子（gate + confidence）
├── risk/
│   ├── loop.py              # 1秒风控循环：止损/止盈/持仓时间/日亏损
│   └── checker.py           # 风控检查函数
├── strategy/
│   ├── signal.py            # SignalComposer：分层因子门控+权重调制
│   ├── exit_manager.py      # ExitManager：Bar级4层退出
│   └── tick_exit.py         # TickExitManager：Tick级3层退出
├── shared/
│   ├── env.py               # 环境配置加载（AppCfg）
│   ├── db.py                # asyncpg连接池
│   └── log.py               # 日志配置
├── prefill_bar_buffer.py    # 启动时从DB加载历史bar预热
├── logs/
│   └── nt_base.log          # 运行日志
└── tests/                   # 单元测试
```

## 核心数据流

```
Binance WebSocket
  → DataManageActor (SOL + BTC bars/ticks)
    → main.py bar dispatch (1m bar)
      → 因子计算 (cvd_divergence, residual_momentum, channel_breakout, trend_regime)
        → V2SignalAdapter.push_factors()
          → AlphaSignal.on_bar()
            → SignalComposer.composite() → 方向判定
              → OrderExecutor.execute() → NautilusTrader下单
        → TickExitManager.on_tick() (SOL tick only)
          → TickTrail/ToxicFlow/Breakeven → 即时退出
```

## 数据库核心表

| 表 | 用途 |
|----|------|
| `ticks` | 逐笔成交（symbol, price, size, aggressor, ts_event） |
| `bars` | K线聚合 |
| `positions` | 持仓记录（side, avg_price, realized_pnl, opened_at, closed_at） |
| `fills` | 成交记录 |
| `orders` | 委托记录 |
| `strategy_instances` | 策略实例配置（params JSON + status: pending/active/stopped） |
| `factor_values` | 因子历史值 |

## 策略配置

当前活跃策略：**AlphaV2-005**

- 因子组合：cvd_divergence(dir=-1, w=2.0) + residual_momentum(dir=1, w=0.5) + channel_breakout(dir=1, w=1.0)
- 门控因子：trend_regime（趋势状态+置信度）
- 信号阈值：0.28（composite > 阈值触发入场）
- 杠杆：3x，仓位：20% equity
- 退出：Tick级（ToxicFlow/TickTrail/Breakeven）+ Bar级（BTC shock/CVD反转/时间衰减/硬止损）
- 自适应：弱趋势→缩仓+缩时，强趋势→加仓+放时

## 关键设计决策

1. **Sandbox模式**：所有交易在 Binance testnet 沙盘执行，不涉及真实资金
2. **热注册**：通过 `strategy_instances` 表动态增删策略，无需重启服务
3. **因子解耦**：因子计算独立于策略，多个策略可共享同一因子结果
4. **Tick过滤**：只有 SOLUSDT 的 tick 送入策略的 TickExitManager（BTC tick 仅用于因子计算）
5. **residual_momentum 依赖 btc_close**：该因子通过 OLS 剔除 BTC beta，btc_close 必须存在否则返回0
6. **factor compute 的 reindex**：因子内部 dropna 可能减少行数，用 `reindex` 而非强制赋 index 对齐原始 df

## 常用运维命令

```bash
# 服务管理
systemctl status nt-base
systemctl restart nt-base
journalctl -u nt-base --since "5 min ago"

# 查看日志
tail -50 /root/nt-base/logs/nt_base.log
grep "ERROR\|WARNING" /root/nt-base/logs/nt_base.log
grep "Signal:.*result=entry" /root/nt-base/logs/nt_base.log  # 入场记录
grep "FLAT" /root/nt-base/logs/nt_base.log                   # 平仓记录

# 数据库查询
docker exec timescaledb psql -U nautilus_admin -d trading_data -c "SELECT * FROM strategy_instances;"
docker exec timescaledb psql -U nautilus_admin -d trading_data -c "SELECT count(*) FROM positions;"

# 清理交易数据（保留策略配置和行情）
docker exec timescaledb psql -U nautilus_admin -d trading_data -c "DELETE FROM fills; DELETE FROM orders; DELETE FROM positions; DELETE FROM factor_values;"
```

## 已知问题与修复记录

- **TickTrail BTC价格BUG**（已修复）：tick分发未过滤品种，BTC价格覆盖SOL止损线，导致仓位1分钟被秒杀
- **residual_momentum Length mismatch**（已修复）：因子内部 dropna 减少行数，`compute_factor_history` 强制赋 index 崩溃；改用 `reindex`
- **residual_momentum 缺少 resample**（已修复）：声明5min但未执行 resample，添加 `df.resample("5min").last()`
- **residual_momentum 缺少 Z-score**（已修复）：输出原始残差而非标准化信号，补充 rolling Z-score
- **hold信号被当平仓**（已修复）：`main.py` 中 `direction=0` 无条件执行 `flat()`，但 `reason="hold"` 表示继续持有；增加 `elif reason=="hold"` 跳过
