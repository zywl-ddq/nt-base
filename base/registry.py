"""
================================================================================
模块:    base/registry.py
用途:    策略注册中心 —— 管理策略槽(StrategySlot)的完整生命周期，
        维护策略索引和因子订阅索引，支持高效的 bar 分发查找。

架构位置:
    DataManageActor (bar 数据源)
        ↓ bar 到达
    StrategyRegistry.get_slots()  ← 查找订阅此 bar 的策略
        ↓ 策略列表
    main.py / TradingNode         ← 将 bar 分发给每个策略

核心职责:
    1. 策略注册/注销 — 增删 StrategySlot 实例
    2. 因子索引维护    — 记录每个因子被哪些策略订阅
    3. 状态查询        — 按 symbol/timeframe 查找策略、获取带持仓策略等
    4. 诊断快照        — summary() 提供运行状态总览

设计理念:
    - 轻量级注册中心，不包含业务逻辑，只做索引管理
    - 因子索引 (_factor_index) 用于 FactorEngine 判断需要计算哪些因子
      （而非计算所有已注册因子），避免不必要的计算开销
    - 所有写操作（register/unregister）在 async 主线程中发生
    - 读操作（get_slots/all_slots）在 bar 分发循环中频繁调用

性能考虑:
    - get_slots() 在每根 1m bar 到达时被调用，时间复杂度 O(n)，
      其中 n = 已注册策略数。当前系统策略数量少（<10），性能可接受
    - 如未来策略数量增长，可考虑构建 symbol+timeframe → slot 的哈希索引

线程安全:
    - 所有写操作在 async 主线程（协程安全）
    - 读操作安全（字典读取在 CPython 中是原子的）

================================================================================
"""
from __future__ import annotations
from base.slot import StrategySlot


class StrategyRegistry:
    """
    ========================================================================
    策略注册中心
    ========================================================================
    管理系统中所有策略槽的注册、注销、查询，以及因子订阅索引。

    数据结构:
        _slots:         dict[str, StrategySlot]
                        主存储：strategy_id → StrategySlot 映射

        _factor_index:  dict[str, set[str]]
                        因子索引：因子名称 → 订阅该因子的 strategy_id 集合
                        用于快速判断哪些因子被至少一个策略需要，
                        从而 FactorEngine 可以跳过计算无人订阅的因子。

    生命周期:
        ┌──────────────────────────────────────────────────────────────┐
        │  __init__()              创建空注册中心                      │
        │      ↓                                                       │
        │  register(slot)          注册策略，同时重建因子索引          │
        │      ↓                                                       │
        │  get_slots(sym, tf)      bar 分发时查找目标策略              │
        │      ↓                                                       │
        │  unregister(strategy_id) 注销策略，清理因子索引              │
        │      ↓                                                       │
        │  get_active_slots()      风控模块查询带持仓策略              │
        └──────────────────────────────────────────────────────────────┘

    与 FactorEngine 的协作:
        - registry.active_factors() 返回当前所有被订阅的因子名集合
        - FactorEngine 只需执行这个集合中的因子，跳过无人订阅的因子
        - 当最后一个订阅某因子的策略注销时，因子索引自动清除该因子
    ========================================================================
    """

    def __init__(self):
        """
        初始化空的策略注册中心。

        属性:
            _slots (dict[str, StrategySlot]):
                主数据存储。键为 strategy_id (str)，值为 StrategySlot 实例。
                策略 ID 由调用方在创建 StrategySlot 时指定，通常从数据库
                strategy_instances 表的 instance_id 字段读取。

            _factor_index (dict[str, set[str]]):
                因子订阅索引。键为因子名称（str），值为订阅了该因子的
                strategy_id 集合（set[str]）。
                当一个因子不再被任何策略订阅时，key 会被删除。
        """
        self._slots: dict[str, StrategySlot] = {}
        self._factor_index: dict[str, set[str]] = {}

    def register(self, slot: StrategySlot) -> None:
        """
        注册一个策略槽。

        流程：
            1. 检查 strategy_id 是否已存在，存在则抛异常（防重复注册）
            2. 将 slot 存入 _slots 字典
            3. 更新因子索引：遍历 slot 的所有订阅，记录因子→策略映射

        参数：
            slot (StrategySlot): 已初始化的策略槽实例。
                slot.strategy_id 必须唯一；slot.subscriptions 定义了该策略
                需要的 bar 数据和因子。

        异常：
            ValueError: 如果 strategy_id 已注册，抛出异常防止覆盖。
                注意：这里使用严格模式——不允许静默覆盖。如果需要更新策略，
                应先 unregister 再 register。

        触发时机：
            - 策略通过 gRPC RegisterSignal RPC 注册时（由 trading-v2 发起）
            - 系统启动时从 strategy_instances 表批量加载策略配置
        """
        if slot.strategy_id in self._slots:
            raise ValueError(f"Strategy {slot.strategy_id} already registered")
        self._slots[slot.strategy_id] = slot
        self._update_factor_index(slot, add=True)

    def unregister(self, strategy_id: str) -> None:
        """
        注销一个策略槽。

        流程：
            1. 从 _slots 中弹出 strategy_id 对应的 slot
            2. 如果存在，更新因子索引：移除该策略对所有因子的订阅记录

        参数：
            strategy_id (str): 要注销的策略 ID。

        行为：
            - 如果 strategy_id 不存在，静默忽略（不报错）
            - 因子索引中，如果某因子因此策略注销后无人订阅，自动删除该因子条目

        触发时机：
            - 策略通过 gRPC UnregisterSignal RPC 注销时
            - 策略实例状态变为 stopped 或 deleted 时（热注销）
        """
        slot = self._slots.pop(strategy_id, None)
        if slot:
            self._update_factor_index(slot, add=False)

    def _update_factor_index(self, slot: StrategySlot, add: bool):
        """
        更新因子索引。

        根据 add 参数决定是添加还是移除策略对因子的订阅记录。

        参数：
            slot (StrategySlot): 要处理的策略槽。
            add (bool):
                - True:  将策略的所有因子订阅添加到索引
                - False: 从索引中移除策略的所有因子订阅

        索引维护逻辑：
            add=True 时：
                - 对 slot 中每个订阅配置的每个因子，建立 因子名→{strategy_id} 映射
                - 使用 setdefault 确保首次建立空 set

            add=False 时：
                - 对每个因子，从集合中 discard 当前 strategy_id
                - 如果集合变为空集（该因子无人订阅），删除该索引条目
                - 使用 discard 而非 remove 避免 KeyError（安全删除）

        注意：
            slot.subscriptions 是一个列表，每个元素（subscription）包含：
            - symbol:    交易品种（如 "SOLUSDT"）
            - timeframe: K线周期（如 "1min"）
            - factors:   该订阅需要计算的因子名称列表（如 ["cvd_divergence", "residual_momentum"]）
        """
        for sub in slot.subscriptions:
            for fname in sub.factors:
                if add:
                    # setdefault: 如果 fname 不存在，先创建空 set，再添加 strategy_id
                    self._factor_index.setdefault(fname, set()).add(slot.strategy_id)
                else:
                    s = self._factor_index.get(fname)
                    if s:
                        s.discard(slot.strategy_id)
                        # 如果某因子不再被任何策略订阅，清理索引条目
                        if not s:
                            del self._factor_index[fname]

    def get_slots(self, symbol: str, timeframe: str) -> list[StrategySlot]:
        """
        查找订阅了指定品种和周期的所有策略槽。

        这是 bar 分发路径上的关键方法。当 DataManageActor 收到一根新 bar，
        会调用此方法找到需要接收此 bar 的所有策略。

        参数：
            symbol (str):    交易品种，如 "SOLUSDT"、"BTCUSDT"
            timeframe (str): K线周期，如 "1min"、"5min"

        返回值：
            list[StrategySlot]: 匹配的策略槽列表。
                如果没有策略订阅该 bar 类型，返回空列表 []。

        算法：
            - 遍历所有已注册策略的 slot
            - 对每个 slot，遍历其所有订阅配置
            - 如果任一订阅的 symbol 和 timeframe 同时匹配，该 slot 入选
            - 使用 break 以避免同一个 slot 被重复添加（一个 slot 可能有多个
              订阅配置，但只需匹配一个即加入结果）

        性能:
            O(n*m) 其中 n = 策略数，m = 每个策略的平均订阅数
            当前系统策略通常只有 1-2 个订阅（SOLUSDT+1min），性能可接受。
        """
        result = []
        for slot in self._slots.values():
            for sub in slot.subscriptions:
                if sub.symbol == symbol and sub.timeframe == timeframe:
                    result.append(slot)
                    break  # 每个 slot 最多加入一次结果
        return result

    def all_slots(self) -> list[StrategySlot]:
        """
        返回所有已注册的策略槽。

        返回值：
            list[StrategySlot]: 所有策略槽的列表。
                注意：返回的是列表副本（list(self._slots.values())），
                调用方修改列表不会影响内部状态。

        使用场景：
            - main.py 遍历所有策略执行信号处理
            - 系统关闭时遍历所有策略执行清理
            - 调试/监控时获取全量策略快照
        """
        return list(self._slots.values())

    def get_active_slots(self) -> list[StrategySlot]:
        """
        返回所有当前持有仓位且未被风控触发的策略槽。

        返回值：
            list[StrategySlot]: 活跃持仓策略列表。

        过滤条件：
            - slot.has_position == True:  策略当前持有仓位
            - slot.tripped == False:      策略未被风控触发

        设计意图：
            风控循环（risk/loop.py）使用此方法获取需要监控的策略：
            - 只有持仓中的策略才需要止损/止盈/持仓时间/日亏损检查
            - 已经 tripped（风控触发平仓）的策略不再重复检查
            - tripped 状态通常在下一个 bar 被 reset（条件解除后）

        注意：
            返回的是新构建的列表，不是内部引用。调用方可以安全修改此列表。
        """
        return [s for s in self._slots.values() if s.has_position and not s.tripped]

    def active_factors(self) -> set[str]:
        """
        返回当前被至少一个策略订阅的因子名称集合。

        返回值：
            set[str]: 活跃因子名称集合。
                如果没有任何策略注册，返回空集 set()。

        使用场景：
            - FactorEngine 根据此集合决定需要计算哪些因子
            - 避免执行无人订阅的因子，节省 CPU

        与 _factor_index 的关系：
            active_factors() 直接返回 _factor_index 的键集合。
            _factor_index 在 register/unregister 时自动维护，
            任何时候都只包含被至少一个策略订阅的因子。
        """
        return set(self._factor_index.keys())

    def get_slot(self, strategy_id: str) -> StrategySlot | None:
        """
        根据 strategy_id 查找单个策略槽。

        参数：
            strategy_id (str): 策略 ID。

        返回值：
            StrategySlot | None:
                - StrategySlot: 如果找到对应策略
                - None: 如果未找到（策略未注册或已被注销）

        使用场景：
            - gRPC handler 处理特定策略的请求时
            - 信号验证时检查特定策略的状态
            - slot.get() 是 O(1) 操作，适合频繁调用
        """
        return self._slots.get(strategy_id)

    @property
    def count(self) -> int:
        """
        返回当前注册的策略总数。

        返回值：
            int: 策略数量。

        使用场景：
            - 监控面板展示策略数量
            - 检查系统是否有策略在运行
            - 调试时快速了解注册规模
        """
        return len(self._slots)

    def summary(self) -> dict:
        """
        返回注册中心当前状态的诊断快照。

        返回值：
            dict: 包含以下键的字典：
                - "total":     int    — 注册策略总数
                - "active":    int    — 持有未平仓头寸的策略数
                - "factors":   int    — 活跃因子数量
                - "strategies" list[str] — 所有已注册策略 ID 列表

        使用场景：
            - gRPC Health/Status RPC 返回给 trading-v2 的诊断信息
            - 日志中定期输出运行状态
            - 调试时快速查看系统全貌

        示例返回:
            {
                "total": 3,
                "active": 1,
                "factors": 3,
                "strategies": ["AlphaV2-005", "AlphaV2-006", "AlphaV2-007"]
            }
        """
        return {
            "total": len(self._slots),
            "active": len(self.get_active_slots()),
            "factors": len(self._factor_index),
            "strategies": list(self._slots.keys()),
        }
