# Trading-V2 架构设计报告：多策略 RPC 通信架构

## 1. 核心诉求

- NT Base 作为常驻进程，统一管理数据接入、交易执行、风控
- 策略作为独立进程（或独立服务），通过 RPC 与 Base 通信
- 支持多策略并行，策略可以独立开发、独立部署、独立启停
- 策略不感知 Binance、NT、风控细节

## 2. RPC 方案对比

### 方案 A：本地进程间通信 (Unix Socket / gRPC Local)

```
┌─────────────────────────────────────────────────┐
│ NT Base (常驻进程)                                 │
│                                                   │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ DataFeed │  │ RiskMgr  │  │ OrderExecutor │  │
│  └──────────┘  └──────────┘  └───────────────┘  │
│        │             │              │             │
│  ┌─────┴─────────────┴──────────────┴──────────┐ │
│  │        SignalBus (gRPC Server)                │ │
│  │  - Subscribe(symbol) → stream<Bar>           │ │
│  │  - Submit(signal) → ack                      │ │
│  │  - GetState() → positions, balance           │ │
│  └─────────────────┬────────────────────────────┘ │
└────────────────────┼──────────────────────────────┘
                     │ Unix Socket / localhost:50051
         ┌───────────┼───────────┐
         ▼           ▼           ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │ Strategy │ │ Strategy │ │ Strategy │
   │    1     │ │    2     │ │    3     │
   │(独立进程) │ │(独立进程) │ │(独立进程) │
   └──────────┘ └──────────┘ └──────────┘
```

| 维度 | 评估 |
|------|------|
| 延迟 | <1ms (Unix Socket)，策略到下单 ~2ms |
| 可靠性 | 进程在同一台机器，网络故障风险低 |
| 复杂度 | 中等，需要 protobuf + gRPC |
| 扩展性 | 策略只能部署在同一机器 |
| 适合场景 | 单机多策略，对延迟敏感 |

### 方案 B：消息队列 (Redis/NATS)

```
NT Base ──publish──► Redis/NATS ──subscribe──► Strategy 1
    ▲                                              │
    │              Redis/NATS ◄──publish───────────┘
    └────────────── (signal channel)
```

| 维度 | 评估 |
|------|------|
| 延迟 | <1ms (Redis local), NATS 类似 |
| 可靠性 | 消息持久化，重连后不丢数据 |
| 复杂度 | 低，Pub/Sub 模型简单 |
| 扩展性 | 策略可以部署在任何机器 |
| 适合场景 | 分布式部署，策略数量多 |

### 方案 C：HTTP REST + WebSocket

| 维度 | 评估 |
|------|------|
| 延迟 | HTTP ~5ms，WebSocket ~1ms |
| 可靠性 | 需要自己处理重连 |
| 复杂度 | 低，通用协议 |
| 扩展性 | 好 |

## 3. 推荐方案：gRPC + 本地优先

**核心选型：gRPC**，理由：

- **双向流**：Base → 策略推送 Bar（server streaming），策略 → Base 提交信号（unary）
- **强类型**：protobuf 定义接口，编译时检查，策略开发者拿到 `.proto` 就知道怎么对接
- **本地零拷贝**：Unix Domain Socket 延迟 <0.5ms
- **未来可远程**：不改代码，换 IP+Port 即可部署到独立服务器

## 4. Proto 接口定义

```protobuf
service TradingBase {
  // 策略订阅行情（服务端流：Base 持续推送 Bar 给策略）
  rpc SubscribeBars(BarRequest) returns (stream Bar);

  // 策略提交交易信号（一元调用）
  rpc SubmitSignal(Signal) returns (SignalAck);

  // 查询当前状态
  rpc GetState(StateRequest) returns (StateResponse);

  // 策略注册/注销
  rpc Register(StrategyInfo) returns (RegisterAck);
  rpc Unregister(StrategyId) returns (UnregisterAck);
}

message Bar {
  string symbol = 1;
  int64 ts_ns = 2;
  double open = 3; double high = 4; double low = 5; double close = 6;
  double volume = 7;
  double delta_buy = 8; double delta_sell = 9;
  double btc_close = 10;
  // 因子值（Base 预计算并附在 Bar 上）
  map<string, double> factors = 11;
}

message Signal {
  string strategy_id = 1;
  string symbol = 2;
  enum Kind { FLAT=0; LONG=1; SHORT=-1; }
  Kind direction = 3;
  string reason = 4;
  int64 bar_ts_ns = 5;  // 基于哪根 Bar 做出的决策
}

message SignalAck {
  bool accepted = 1;
  string reject_reason = 2;  // 风控拒绝原因
}
```

## 5. 多策略隔离设计

```
策略 ID: "trend_v1"    策略 ID: "revert_v1"    策略 ID: "ml_v1"
     │                      │                      │
     │ Signal(LONG, SOL)    │ Signal(SHORT, SOL)    │ Signal(LONG, SOL)
     ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────┐
│                   TradingBase SignalBus                  │
│                                                         │
│  每个策略有独立的仓位跟踪：                                  │
│    positions = {                                         │
│      "trend_v1":   { "SOLUSDT-PERP": {side:SHORT, qty:3.2}},  │
│      "revert_v1":  { "SOLUSDT-PERP": None },             │
│      "ml_v1":      { "SOLUSDT-PERP": {side:LONG, qty:1.5}},   │
│    }                                                     │
│                                                         │
│  每个策略有独立的风控限额：                                    │
│    limits = {                                            │
│      "trend_v1":   {max_daily_loss: 5%, max_leverage: 3},│
│      "revert_v1":  {max_daily_loss: 3%, max_leverage: 2},│
│      "ml_v1":      {max_daily_loss: 5%, max_leverage: 5},│
│    }                                                     │
└─────────────────────────────────────────────────────────┘
     │
     │ 合并后的实际仓位
     ▼
┌─────────────────────┐
│   Binance 子账户     │
│   (或 Sandbox 模拟)  │
│                     │
│  净头寸 = SUM(各策略仓位) │
│  风控   = MIN(各策略限额) │
└─────────────────────┘
```

**关键设计决策：策略仓位合并**

两个策略同时跑 SOL，一个做多一个做空——到交易所层面是净头寸。两种处理方式：

| 方式 | 描述 | 风险 |
|------|------|------|
| **净头寸** | 3.2 SHORT + 1.5 LONG = 1.7 SHORT 送到交易所 | 策略互相抵消，每笔信号都扣钱但实际没成交 |
| **子账户隔离** | 每个策略独立子账户，互不干扰 | 无，推荐 |

**结论：多策略实盘必须用子账户隔离。Sandbox 模式可以用虚拟子账户（NT 内部模拟多个独立账户）。**

## 6. 策略生命周期

```
策略开发 → 回测验证 → 沙箱测试 → 实盘注册
   │                        │
   │  rd_agent 自动迭代     │  RPC 连接 Base
   │  因子 + 参数搜索        │
   ▼                        ▼
 AlphaSignal.py      Register(strategy_id, config)
                           │
                     Base 分配独立仓位槽位
                           │
                     策略开始接收 Bar 流
                           │
                     策略输出 Signal
                           │
                     Base 风控 → 执行
                           │
                     策略可随时 Unregister
                           │
                     Base 平掉该策略仓位
```

## 7. 与当前实现的关系

当前已实现的部分可以直接演进：

| 当前 | RPC 架构下 |
|------|-----------|
| `SignalStrategy` (Protocol) | 保持不变，只改通信方式 |
| `TradingBase` (NT Strategy) | 嵌入 gRPC Server，调用改为 RPC |
| `DataFeed` (DB 轮询) | 移到 Base 内部，附上因子值通过 gRPC 推送 |
| `AlphaSignal` | 改为 gRPC Client，收 Bar → 算信号 → 提交 |
| `ExitManager` | 留在策略侧，exit 信号通过同一个 RPC 通道 |

## 8. 实施路线图

**Phase 1（当前）**：策略和 Base 在同一进程，直接调用
- ✅ SignalStrategy Protocol 已定义
- ✅ TradingBase 已实现
- ⏳ DataFeed + Event Loop 对齐（调试中）

**Phase 2**：Base 内部支持多策略
- Base 维护 `dict[str, SignalStrategy]`
- 每个策略独立仓位跟踪
- 策略注册/注销 API

**Phase 3**：gRPC 分离
- Base 嵌入 gRPC Server
- 策略改为独立进程，通过 gRPC Client 连接
- Proto 接口定义

**Phase 4**：子账户隔离
- 每个策略绑定独立 Binance 子账户
- Sandbox 模式用虚拟子账户

## 9. 总结

| 问题 | 答案 |
|------|------|
| RPC 通信是否可行？ | ✅ gRPC + Unix Socket，本地延迟 <1ms |
| 能否支持多策略？ | ✅ 独立仓位槽 + 子账户隔离 |
| 策略需要关心 NT 吗？ | ❌ 只需实现 `SignalStrategy` 接口，收 Bar 返回 Signal |
| Base 停机策略怎么处理？ | Base 通知所有策略 → 平仓 → 注销 → 停机 |
| 策略热更新？ | 新策略 Register，旧策略 Unregister，Base 不停机 |
