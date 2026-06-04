# nt-base 代码走查报告

**项目**: nt-base — 基于 NautilusTrader 的加密货币交易基座服务  
**日期**: 2026-06-04  
**审查范围**: 全部源代码、配置、测试、文档  

---

## 一、项目概览

nt-base 是一个基于 NautilusTrader 1.227 的实时交易系统，架构为：

```
Binance WS → DataManageActor → TimescaleDB
                   │
              on_bar() callback:
                   ├── 1s/5s bar → 写库
                   └── 1m bar → 因子计算 → 策略槽 → 执行器 → NT 下单
                   └── 1s risk loop → 止损/止盈/超时/熔断检查
```

**文件统计**: 18 个 Python 源文件，~1600 行代码，3 个测试文件，3 个因子实现。

---

## 二、严重问题 (Critical)

### C1. `.env` 文件包含明文密钥，安全风险极高

**文件**: `.env`  
**问题**: Binance API Key/Secret、TimescaleDB 密码、Telegram Token 全部以明文存储。虽然 `.gitignore` 已排除 `.env`，但：
- 文件权限未限制（任何用户可读）
- 如果仓库曾意外提交，密钥已进入 git 历史
- `shared/env.py` 直接读取并暴露这些值到进程内存

**建议**:
- 立即轮换所有已暴露的密钥
- 使用 `chmod 600 .env` 限制文件权限
- 考虑使用 Vault 或 systemd Credentials 存储敏感信息
- 在 `assert_required()` 中增加密钥强度/格式校验

---

### C2. `factor/registry.py` 存在语法错误，模块无法加载

**文件**: `factor/registry.py:26-32`  
**问题**: `cvd_divergence` 的 `FactorDef` 缺少闭合括号 `)`，导致整个模块无法导入：

```python
"cvd_divergence": FactorDef(
    name="cvd_divergence",
    file="cvd_divergence.py",
    windows=[60],
    output_range=(-3.0, 3.0),
"residual_momentum": FactorDef(   # ← 缺少 cvd_divergence 的 )
    ...
),
),                                  # ← 多余的 )
```

**影响**: 任何依赖 `factor.registry` 的代码在运行时都会 `SyntaxError` 崩溃。

**修复**: 在 `output_range=(-3.0, 3.0),` 后添加 `)`。

---

### C3. `factor/compute.py` 使用 `exec()` 执行任意代码，且安全沙箱未生效

**文件**: `factor/compute.py:95`  
**问题**: 
1. `exec(code, namespace)` 直接执行因子代码字符串，无任何沙箱隔离
2. 定义了 `_SAFE_BUILTINS` 限制可用内置函数，但实际命名空间使用 `__builtins__` 完整内置，`_SAFE_BUILTINS` 完全未被引用
3. 因子代码可以执行任意系统操作：`__import__('os').system('rm -rf /')`

**建议**:
- 将 `_FACTOR_NAMESPACE_BASE` 中的 `__builtins__` 替换为 `_SAFE_BUILTINS`
- 或使用 `RestrictedPython` / 子进程隔离执行因子代码
- 至少对因子代码做静态分析（AST 检查），禁止 `import`、`__import__`、`eval`、`exec` 等调用

---

### C4. `main.py` bar buffer 大小不足以支持所有因子

**文件**: `main.py:115`  
**问题**: `_bar_buffer = deque(maxlen=120)`，但 `residual_momentum` 因子需要 `ROLLING_BETA = 288` 根 bar 才能计算。当前 buffer 只保留 120 根，远不够 288 根。

**影响**: `residual_momentum` 因子永远无法产出有效值，始终返回 0。

**修复**: 将 `maxlen` 设为至少 300，或根据注册因子的最大窗口动态调整。

---

## 三、高风险问题 (High)

### H1. `base/executor.py` 使用了不存在的 NT API

**文件**: `base/executor.py:60`  
**问题**: `self._order_factory.submit_order(order)` — NautilusTrader 的 `OrderFactory` 只负责创建订单，**没有** `submit_order` 方法。正确做法是通过 `Strategy.submit_order()` 提交。`OrderExecutor` 不是 `Strategy` 子类，无法直接提交订单。

**修复**: 需要传入 `Strategy` 引用，或将 `OrderExecutor` 作为 Strategy 的内部类来调用 `self._strategy.submit_order(order)`。

---

### H2. `risk/loop.py` 硬编码交易品种

**文件**: `risk/loop.py:35`  
**问题**: `price = self._prices.get("SOLUSDT-PERP.BINANCE", 0)` 硬编码了品种名。如果 `update_price()` 传入的 key 不完全匹配（如缺少 `.BINANCE` 后缀），风控检查将永远拿不到价格，所有仓位不会被止损/止盈。

**修复**: 从 slot 的 subscription 中获取 symbol，或遍历 `_prices` 查找匹配的 key。

---

### H3. `main.py` 访问 NT 内部私有属性，脆弱性极高

**文件**: `main.py:94-109`  
**问题**: 
- 访问 `node.trader._actors` 或 `_components`（私有属性）
- 通过 `"DataManageActor" in type(actor).__name__` 字符串匹配查找 Actor
- NT 版本升级后这些内部实现可能变化，导致运行时崩溃

**修复**: 使用 NT 公开 API 获取 Actor 引用，或在添加 Actor 时保存引用而不是事后查找。

---

### H4. `base/data_manage.py` OI 轮询使用同步 HTTP 阻塞事件循环

**文件**: `base/data_manage.py:721-724`  
**问题**: `_oi_poll_loop` 中使用 `urllib.request.urlopen()` 同步发起 HTTP 请求，会阻塞整个 asyncio 事件循环。在高频交易系统中，1-10 秒的阻塞可能导致错过行情处理和风控检查。

**修复**: 改用 `aiohttp` 或 `httpx` 异步 HTTP 客户端，与 funding poll 使用 `ccxt.async` 保持一致。

---

### H5. `main.py` 因子计算在 bar 回调中同步执行，可能阻塞事件循环

**文件**: `main.py:143-160`  
**问题**: `compute_factor_history()` 在 on_bar 回调中同步调用，其中 `residual_momentum` 因子包含 O(n×window) 的滚动 OLS 计算。对于 288 窗口、300+ bar 的数据，单次计算可能耗时数十毫秒到数百毫秒，阻塞 bar 处理。

**修复**: 将因子计算放入 `asyncio.to_thread()` 或单独的线程池中异步执行。

---

### H6. `base/data_manage.py` buffer 切换存在数据丢失风险

**文件**: `base/data_manage.py:776`  
**问题**: `_flush_bars()` 执行 `batch, self._bar_buf = self._bar_buf, []`，但在 `on_bar` 和 `_flush_bars` 之间存在微小的竞态窗口——如果 `on_bar` 在 `batch = self._bar_buf` 之后、`self._bar_buf = []` 之前追加了新数据，这些数据会被丢弃。

在单线程 asyncio 环境中此问题较小，但 `on_bar` 是 NT 同步回调，若 NT 在不同线程调用，则会出现数据丢失。

**建议**: 确认 NT 回调是否在同一个 event loop 线程执行。如果是，则无问题；如果不是，需要加锁保护 buffer。

---

## 四、中等风险问题 (Medium)

### M1. `shared/db.py` 模块级 `asyncio.Lock()` 在无事件循环时会失败

**文件**: `shared/db.py:17`  
**问题**: `_lock = asyncio.Lock()` 在模块导入时创建。如果在事件循环启动前导入（如被非 async 代码 import），Python 3.10+ 会抛出 `DeprecationWarning`，3.12+ 可能报错。

**修复**: 改为在 `get_pool()` 内部惰性创建锁。

---

### M2. `nt/instruments.py` 继承了 BTC 的交易限制参数用于 SOL

**文件**: `nt/instruments.py:11-28`  
**问题**: `solusdt_perp_binance()` 以 `btcusdt_perp_binance()` 为基础，继承了 BTC 的 `max_quantity`、`min_quantity`、`max_notional`、`min_notional`、`max_price`、`min_price`。BTC 和 SOL 的价格差距约 200 倍，这些限制参数对 SOL 可能完全不合适。

**修复**: 查阅 Binance SOLUSDT-PERP 合约规格，设置正确的 SOL 专用参数。

---

### M3. `main.py` 品种名不一致

**文件**: `main.py:137` vs `base/signal_protocol.py:20`  
**问题**: 
- `main.py:137` 调用 `registry.get_slots("SOLUSDT-PERP", "1m")`
- 但 `risk/loop.py:35` 使用 `"SOLUSDT-PERP.BINANCE"`
- `test_registry.py` 中 `BarSubscription` 使用 `"SOLUSDT-PERP"`
- 实际 NT 的 `instrument_id` 格式为 `"SOLUSDT-PERP.BINANCE"`

品种标识符格式不统一，可能导致策略槽查找失败。

**修复**: 统一品种标识格式，建议使用完整的 `SOLUSDT-PERP.BINANCE`。

---

### M4. `shared/log.py` 日志目录硬编码

**文件**: `shared/log.py:9`  
**问题**: `LOG_DIR = Path("/root/nt-base/logs")` 硬编码绝对路径。项目部署到其他目录时日志会写入错误位置。

**修复**: 使用 `ROOT / "logs"` 相对路径，与 `shared/env.py` 的 `ROOT` 变量保持一致。

---

### M5. `base/executor.py` reverse 操作非原子性

**文件**: `base/executor.py:32-36`  
**问题**: 反向操作先 `flat()` 平仓再 `_open()` 开仓，两步操作之间如果执行失败（如网络异常），会导致仓位丢失：旧仓已平、新仓未开。

**修复**: 考虑使用 NT 的 OCO（One-Cancels-Other）订单，或在 flat 失败时跳过 open，增加重试和回滚逻辑。

---

### M6. `factor/compute.py:30-33` 重复的搜索路径

**文件**: `factor/compute.py:30-33`  
**问题**: `_load_factor_code()` 遍历的两个目录完全相同：
```python
for factors_dir in (
    Path("/root/nt-base/factors"),
    Path("/root/nt-base/factors"),  # 重复
):
```

**修复**: 删除重复项，或改为基于 `ROOT` 的相对路径。

---

### M7. `base/data_manage.py:393-394` 缩进不一致

**文件**: `base/data_manage.py:393-394`  
**问题**: `on_bar` 方法中注释缩进不一致，虽然不影响运行，但违反 PEP 8。

---

### M8. `shared/env.py:45-46` 覆盖已有环境变量的逻辑过于激进

**文件**: `shared/env.py:45-46`  
**问题**: 如果现有 `os.environ` 值包含 `#`（如 API Key 可能包含 `#`），会被 `.env` 文件中的值覆盖。这个设计意图是处理 systemd 的 inline comment 问题，但副作用太大。

**修复**: 只在值确实是以 `#` 结尾的 inline comment 情况下覆盖，而不是检测整个值中是否包含 `#`。

---

## 五、低风险问题 (Low)

### L1. `base/slot.py` 可变 dataclass 与 `@property` 混用

`StrategySlot` 是可变 dataclass，但 `held_sec` 是 `@property`。Python dataclass 对 `@property` 的处理需要 `field(init=False)`，当前代码能运行但语义不够清晰。

### L2. `base/signal_protocol.py:25-26` `BarSubscription.__hash__` 只基于 symbol+timeframe

`__hash__` 忽略了 `factors`，意味着相同 symbol+timeframe 但不同 factors 的订阅会哈希冲突。如果用于 set/dict key，可能导致因子订阅丢失。

### L3. 缺少类型标注

`base/executor.py:11` 的 `__init__` 参数缺少类型标注（`sol_id, venue, portfolio, order_factory, cache`）。

### L4. `base/data_manage.py` 中 `on_order_book_deltas` 使用 `str(order.side)` 做字符串比较

应使用枚举值比较，如 `order.side == OrderSide.BUY`，避免依赖字符串表示。

### L5. 缺少 `requirements.txt` 或 `pyproject.toml`

项目没有依赖声明文件。依赖包括 `nautilus_trader`、`asyncpg`、`ccxt`、`scipy`、`pandas`、`numpy`，但未显式声明版本。

---

## 六、测试覆盖度分析

| 模块 | 测试文件 | 覆盖情况 |
|------|----------|----------|
| `risk/checker.py` | `test_checker.py` | 部分：只测了 stop 和 take，缺 hold/daily/check_all |
| `base/registry.py` | `test_registry.py` | 良好：注册/注销/因子索引/重复注册 |
| `base/slot.py` | `test_slot.py` | 基础：初始状态/开关仓/持仓时间 |
| `base/executor.py` | 无 | **缺失** |
| `base/data_manage.py` | 无 | **缺失** |
| `factor/compute.py` | 无 | **缺失** |
| `risk/loop.py` | 无 | **缺失** |
| `main.py` | 无 | **缺失** |

**测试覆盖率估计**: ~20%。核心交易执行路径（executor、data_manage、main）完全没有单元测试。

---

## 七、架构层面建议

1. **消除 monkey-patch 模式**: `main.py` 中对 `dm_actor.on_bar` 的猴子补丁是反模式。建议 DataManageActor 提供 `add_bar_callback(handler)` 注册机制。

2. **策略注册机制缺失**: 当前 `registry` 为空启动，没有策略注册入口。设计文档提到了 RPC 架构，但当前无任何策略注册代码。

3. **配置集中化**: 品种名 `SOLUSDT-PERP.BINANCE` 在 `main.py`、`trading_node.py`、`risk/loop.py`、`data_manage.py` 多处硬编码，应统一到 `shared/env.py` 的 `cfg` 中。

4. **错误处理策略**: 大量 `except Exception: pass` 或仅 log 的 catch 块。需要明确哪些错误应中断流程、哪些可以降级。

5. **优雅关闭不完整**: `BaseStrategy.on_stop()` 调用 `asyncio.create_task(self._risk_loop.stop())` 但不等待完成；`flat_all()` 也不等待成交确认。

---

## 八、问题汇总

| 级别 | 编号 | 文件 | 问题摘要 |
|------|------|------|----------|
| **Critical** | C1 | `.env` | 明文密钥暴露 |
| **Critical** | C2 | `factor/registry.py:26` | 语法错误，模块无法加载 |
| **Critical** | C3 | `factor/compute.py:95` | exec() 任意代码执行，沙箱未生效 |
| **Critical** | C4 | `main.py:115` | bar buffer 不足以支持 residual_momentum 因子 |
| **High** | H1 | `base/executor.py:60` | 使用不存在的 NT API (submit_order) |
| **High** | H2 | `risk/loop.py:35` | 硬编码品种名，风控可能失效 |
| **High** | H3 | `main.py:94-109` | 访问 NT 内部私有属性，版本升级易崩 |
| **High** | H4 | `base/data_manage.py:721` | 同步 HTTP 阻塞事件循环 |
| **High** | H5 | `main.py:143-160` | 因子同步计算阻塞 bar 处理 |
| **High** | H6 | `base/data_manage.py:776` | buffer 切换数据丢失风险 |
| **Medium** | M1 | `shared/db.py:17` | 模块级 asyncio.Lock 问题 |
| **Medium** | M2 | `nt/instruments.py` | SOL 继承 BTC 交易限制 |
| **Medium** | M3 | 多文件 | 品种标识符格式不统一 |
| **Medium** | M4 | `shared/log.py:9` | 日志目录硬编码 |
| **Medium** | M5 | `base/executor.py:32-36` | reverse 操作非原子性 |
| **Medium** | M6 | `factor/compute.py:30-33` | 重复的因子搜索路径 |
| **Medium** | M7 | `base/data_manage.py:393` | 缩进不一致 |
| **Medium** | M8 | `shared/env.py:45-46` | 环境变量覆盖逻辑过于激进 |
| **Low** | L1-L5 | 多文件 | dataclass/property、hash、类型标注、依赖声明 |

**统计**: Critical 4 / High 6 / Medium 8 / Low 5，合计 23 项。

---

## 九、优先修复建议

1. **立即修复** (阻塞运行):
   - C2: 修复 `factor/registry.py` 语法错误
   - H1: 修复 `executor.py` 的 NT API 调用
   - C4: 调整 bar buffer 大小

2. **尽快修复** (安全/稳定性):
   - C1: 轮换密钥、限制文件权限
   - C3: 启用因子代码沙箱
   - H2: 风控 loop 使用动态品种
   - H4: OI 轮询改用异步 HTTP

3. **计划修复** (技术债):
   - M1-M8、L1-L5: 逐步清理
   - 补充 executor/data_manage/risk loop 单元测试
   - 添加 `pyproject.toml` 声明依赖
