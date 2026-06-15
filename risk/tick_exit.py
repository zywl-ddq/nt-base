# -*- coding: utf-8 -*-
"""
Tick 级退出管理器 (risk/tick_exit)
====================================

模块定位
--------
本模块实现**毫秒级**的 Tick 风控检查，直接运行在 nt-base 进程中，
无需经过 gRPC 通信。它与 RiskLoop（1 秒精度）互补，覆盖两个 tick 之间
的反转风险。

在风控体系中的角色
------------------
trading-v2（策略层）通过 gRPC 接收 SOLUSDT 的 tick 数据，然后调用
TickExitManager.on_tick() 进行即时检查。这是一个**本地调用**，延迟在
微秒级别。

Bar 级退出（trading-v2）和 Tick 级退出（nt-base）并行运行，互不干扰。

三层退出机制
------------
L1: Toxic Flow (有毒订单流) -- [已禁用]
    检测 1 秒窗口内的激进买卖比例，当卖方主导比例 > 5:1 时触发退出。
    当前版本已禁用（_check_toxic_flow 始终返回 None），保留框架以备启用。

L2: Tick 级跟踪止损 (Tick Trailing Stop) -- [启用]
    基于入场以来的 Tick 级最高/最低价，结合 ATR 计算浮动止损线。
    与 RiskLoop 的 check_trail 区别：本模块在每次 tick 时检查，
    而 RiskLoop 每秒检查一次。

L3: 保本阶梯 (Breakeven Ladder) -- [启用]
    当浮动盈利达到 1.5 倍 ATR 时，将止损线上移至保本价（覆盖手续费）。
    确保盈利仓位不会因反向波动而变成亏损。

为什么从 trading-v2 迁移到 nt-base？
tick 数据直接由 nt-base 采集（WebSocket），如果发到 trading-v2 再检查
会引入 gRPC 延迟。直接在 nt-base 执行 Tick 级风控，延迟降至最低。

设计目标
--------
- 低延迟：所有计算在内存中完成，无 IO 操作
- 无状态管理：持仓生命周期由 open_position / close_position / add_position 管理
- 非侵入：与 RiskLoop 解耦，各自独立运行

Author: nt-base system
Version: 1.2.0
"""
from collections import deque
from dataclasses import dataclass


@dataclass
class TickTrade:
    """一笔 Tick 交易数据。

    用于 L1 (Toxic Flow) 的 1 秒窗口分析。
    注意：Binance 的 aggressor 标识 is_buyer 表示 taker 是否为买方。

    Fields:
        price:   成交价格
        size:    成交数量（币数量，非 USDT 金额）
        is_buyer: 主动吃单方是否为买方
                  True  = 买方主动吃单（买盘强势）
                  False = 卖方主动吃单（卖盘强势）
        ts_ns:   成交时间戳（纳秒精度）
    """
    price: float
    size: float
    is_buyer: bool
    ts_ns: int


@dataclass
class TickExitAction:
    """Tick 级退出动作 —— 表示某个 Tick 风控条件被触发。

    与 RiskAction（risk/checker.py）不同，本数据结构增加了 urgency 字段
    用于指示退出紧急程度（当前所有触发都设为 'immediate'）。

    Fields:
        reason (str): 触发原因描述，包含关键数值便于日志分析
                      例如: "TickTrail LONG stop=23.50 high=24.10 atr=0.35"
        urgency (str): 紧急程度，当前仅支持 'immediate'（立即退出）
                       保留字段以备将来支持分级退出
    """
    reason: str
    urgency: str = 'immediate'


class TickExitManager:
    """Tick 级退出管理器 —— 三层风控的本地执行引擎。

    本类管理单个持仓的 Tick 级风控状态。对于每个新持仓，需要调用
    open_position() 初始化。持仓金字塔加仓时调用 add_position() 更新
    入场价但不重置极值。

    与 StrategySlot 的关系：
    - StrategySlot 记录日频和 Bar 级风控数据（日盈亏、持仓时间等）
    - TickExitManager 只管理 Tick 级数据（毫秒级极值、1 秒交易窗口）
    - 两者状态独立，但共同作用于同一个持仓

    关键属性说明：
    - _trade_window:  保存最近 1 秒的 TickTrade 数据，用于 Toxic Flow 分析
    - _highest_tick:  入场以来的最高成交价（Tick 级别，非 K 线级别）
    - _lowest_tick:   入场以来的最低成交价
    - _breakeven_activated: 保本阶梯是否已激活（激活后止损线不低于保本价）

    Args:
        toxic_vol_threshold: Toxic Flow 的成交量阈值（当前未使用）
        toxic_ratio:         Toxic Flow 的买卖比阈值（5:1，当前未使用）
        trail_atr_mult:      ATR 倍数，用于计算跟踪止损线的回撤距离，默认 4.0
        breakeven_atr_mult:  ATR 倍数，达到该倍数盈利时激活保本阶梯，默认 1.5
        breakeven_fee_pct:   保本价与入场价的额外偏移比例，用于覆盖手续费，默认 0.001
    """

    def __init__(self,
                 toxic_vol_threshold: float = 500.0,
                 toxic_ratio: float = 5.0,
                 trail_atr_mult: float = 4.0,
                 breakeven_atr_mult: float = 3.0,
                 breakeven_fee_pct: float = 0.001,
                 ):
        # ---- Toxic Flow 参数 ----
        self.toxic_vol_threshold = toxic_vol_threshold
        self.toxic_ratio = toxic_ratio

        # ---- 跟踪止损参数 ----
        self.trail_atr_mult = trail_atr_mult

        # ---- 保本阶梯参数 ----
        self.breakeven_atr_mult = breakeven_atr_mult
        self.breakeven_fee_pct = breakeven_fee_pct

        # ---- 运行时状态 ----
        self._trade_window: deque[TickTrade] = deque()          # 1 秒交易窗口
        self._window_ns: int = 1_000_000_000                   # 窗口时长：1 秒（纳秒）

        # 持仓状态
        self._in_position = False                               # 是否有持仓
        self._is_long = True                                    # 是否做多
        self._entry_price = 0.0                                 # 入场价
        self._highest_tick: float = 0.0                         # 入场以来最高 tick 价
        self._lowest_tick: float = float('inf')                  # 入场以来最低 tick 价
        self._breakeven_activated = False                       # 保本是否已激活

        # 外部数据缓存
        self._current_atr: float = 0.0                          # 最新 ATR
        self._symbol: str = ''                                  # 交易对符号

    # ---- 持仓生命周期管理 ----

    def open_position(self, entry_price: float, is_long: bool, symbol: str = ''):
        """开仓时初始化 Tick 级风控状态。

        重置所有运行时变量为初始值：
          - _highest_tick: 做多=入场价，做空=0（等待第一个 tick 更新）
          - _lowest_tick:  做空=入场价，做多=inf（等待第一个 tick 更新）
          - _breakeven_activated: False
          - _trade_window.clear()

        Args:
            entry_price: 开仓价格
            is_long:     True=做多, False=做空
            symbol:      交易对符号，用于过滤非本交易对的 tick
        """
        self._symbol = symbol
        self._in_position = True
        self._is_long = is_long
        self._entry_price = entry_price
        self._highest_tick = entry_price if is_long else 0.0
        self._lowest_tick = entry_price if not is_long else float('inf')
        self._breakeven_activated = False
        self._trade_window.clear()

    def close_position(self):
        """平仓时清理 Tick 级风控状态。

        将 _in_position 置为 False，清空交易窗口。
        注意：不清除极值（_highest_tick, _lowest_tick）——虽然已无需要，
        但也不会被误读，因为 in_position 为 False 时 on_tick 直接返回 None。
        """
        self._in_position = False
        self._symbol = ''
        self._trade_window.clear()

    def add_position(self, entry_price: float):
        """金字塔加仓：更新 VWAP 入场价，但不重置跟踪止损状态。

        在已有的同方向持仓上继续加仓时调用。关键设计决策：
        - 更新 _entry_price 为新加仓后的加权平均入场价
        - 故意不重置 _highest_tick / _lowest_tick（跟踪止损锚点）
        - 故意不重置 _breakeven_activated（保本阶梯状态）
        - 故意不清空 _trade_window（Toxic Flow 分析连续性）

        为什么这样设计？
        如果加仓后重置极值，跟踪止损线会跳到加仓价附近，导致原持仓的
        浮动保护失效。不重置极值意味着整个仓位（原仓+新仓）继续使用
        原仓位的最优价格作为跟踪锚点，保证风控策略的一致性和保守性。

        Args:
            entry_price: 新加仓后的加权平均入场价
        """
        self._entry_price = entry_price
        # 以下属性故意不重置：
        #   _highest_tick / _lowest_tick -- 跟踪止损锚点
        #   _breakeven_activated         -- 保本阶梯状态
        #   _trade_window                -- 交易窗口

    def update_atr(self, atr: float):
        """更新最新 ATR 值。

        由外部在每次 bar 计算完成后调用。
        ATR 用于跟踪止损距离计算和保本激活判断。

        Args:
            atr: 平均真实波幅值
        """
        self._current_atr = atr

    @property
    def in_position(self) -> bool:
        """当前是否有持仓。"""
        return self._in_position

    # ---- Tick 处理入口 ----

    def on_tick(self, price: float, size: float, is_buyer: bool,
                ts_ns: int, symbol: str = '') -> TickExitAction | None:
        """处理每个传入的 tick，执行三层风控检查。

        这是整个 TickExitManager 的入口方法，每次 SOLUSDT 的 tick 到达时调用。
        执行流水线：
          1. 过滤：无持仓或符号不匹配时直接返回
          2. 更新 1 秒交易窗口（追加 + 裁剪过期数据）
          3. 更新最高/最低价
          4. L1: Toxic Flow 检查（已禁用）
          5. L2: Tick 级跟踪止损检查
          6. L3: 保本阶梯激活检查

        Args:
            price:   成交价格
            size:    成交数量
            is_buyer: 吃单方是否为买方
            ts_ns:   成交时间戳（纳秒）
            symbol:  交易对符号，用于过滤非本持仓的 tick

        Returns:
            触发退出时返回 TickExitAction(reason=..., urgency='immediate')，
            否则返回 None
        """
        # 无持仓时不处理任何 tick
        if not self._in_position:
            return None

        # 符号过滤：如果本管理器指定了 symbol，且传入的 symbol 不同则跳过
        # 这在订阅了多个交易对时非常重要，防止 BTC tick 影响 SOL 持仓
        if self._symbol and symbol and symbol != self._symbol:
            return None

        # 将当前 tick 加入 1 秒窗口
        trade = TickTrade(price=price, size=size, is_buyer=is_buyer, ts_ns=ts_ns)
        self._trade_window.append(trade)
        # 裁剪窗口：移除超过 1 秒的旧数据
        self._prune_window(ts_ns)

        # 更新入场以来的价格极值
        if self._is_long:
            # 做多时只关心最高价（用于跟踪止损）
            self._highest_tick = max(self._highest_tick, price)
        else:
            # 做空时只关心最低价（用于跟踪止损）
            self._lowest_tick = min(self._lowest_tick, price)

        # L1: Toxic Flow 检查（已禁用，保留框架）
        # result = self._check_toxic_flow()
        # if result: return result

        # L2: Tick 级跟踪止损检查（启用）
        result = self._check_trailing_stop(price)
        if result:
            return result

        # L3: 检查是否满足保本激活条件（不返回退出动作，只是修改状态）
        self._check_breakeven_activation(price)

        return None

    # ---- L1: Toxic Flow（已禁用） ----

    def _prune_window(self, current_ns: int):
        """裁剪 1 秒交易窗口，移除超过 1 秒的旧数据。

        Args:
            current_ns: 当前 tick 的时间戳（纳秒）
        """
        cutoff = current_ns - self._window_ns  # 1 秒前的时间点
        while self._trade_window and self._trade_window[0].ts_ns < cutoff:
            # 从左侧（最旧）弹出
            self._trade_window.popleft()

    def _check_toxic_flow(self) -> TickExitAction | None:
        """检查 1 秒窗口内的有毒订单流。

        算法思路（已禁用）：
          计算最近 1 秒内卖方主动成交量 / 买方主动成交量，
          如果比例 > 5:1 且总成交量 > 阈值，则认为存在有毒订单流，
          表明大资金正在集中抛售/买入，价格可能快速反转。

        当前状态：已禁用（始终返回 None）。
        禁用原因：止损止盈可能比 Toxic Flow 信号更快响应，
        且 Toxic Flow 在低流动性时段容易产生误报。

        Returns:
            （始终返回 None）
        """
        return None  # Disabled

    # ---- L2: Tick 级跟踪止损 ----

    def _check_trailing_stop(self, current_price: float) -> TickExitAction | None:
        """Tick 级跟踪止损检查。

        这是核心算法，与 risk/checker.py 的 check_trail 功能相似但更精细：
        1. 使用 trail_atr_mult * ATR 作为止损距离（checker 使用 stop_pct * price）
        2. 如果保本阶梯已激活，止损线上限为保本价（确保不亏本）
        3. 做多：止损价 = max(保本价, 最高价 - trail_atr_mult * ATR)
        4. 做空：止损价 = min(保本价, 最低价 + trail_atr_mult * ATR)

        为什么要和 checker.py 的 check_trail 同时存在？
        - checker.py 的 check_trail 在 RiskLoop 中每秒执行一次
        - 本函数在每个 tick 时就检查，比 RiskLoop 快数百倍
        - 两者是冗余防护：一个挂了另一个还能兜底

        Args:
            current_price: 当前 tick 价格

        Returns:
            触发退出时返回 TickExitAction，否则返回 None
        """
        # 使用当前 ATR 或回退到价格的 0.15%
        atr = self._current_atr if self._current_atr > 0 else current_price * 0.0015

        # 计算保本价（只在保本已激活时有效）
        if self._breakeven_activated:
            if self._is_long:
                # 做多保本价 = 入场价 * (1 + 手续费率)
                # 确保卖出后扣除手续费仍保本
                fee_cover = self._entry_price * (1.0 + self.breakeven_fee_pct)
            else:
                # 做空保本价 = 入场价 * (1 - 手续费率)
                # 确保买回后扣除手续费仍保本
                fee_cover = self._entry_price * (1.0 - self.breakeven_fee_pct)
        else:
            # 保本尚未激活，fee_cover 设为 0 不影响止损线
            fee_cover = 0.0

        if self._is_long:
            # 做多跟踪止损：
            # 止损价 = max(保本价, 最高价 - trail_atr_mult * ATR)
            # 取 max 确保止损线不会低于保本价
            stop_price = max(fee_cover, self._highest_tick - self.trail_atr_mult * atr)
            if current_price <= stop_price:
                return TickExitAction(
                    f'TickTrail LONG stop={stop_price:.4f} '
                    f'high={self._highest_tick:.4f} atr={atr:.4f}',
                    'immediate'
                )
        else:
            # 做空跟踪止损：
            # 保本已激活时：止损价 = min(保本价, 最低价 + trail_atr_mult * ATR)
            # 保本未激活时：止损价 = 最低价 + trail_atr_mult * ATR
            if self._breakeven_activated:
                stop_price = min(fee_cover, self._lowest_tick + self.trail_atr_mult * atr)
            else:
                stop_price = self._lowest_tick + self.trail_atr_mult * atr
            if current_price >= stop_price:
                return TickExitAction(
                    f'TickTrail SHORT stop={stop_price:.4f} '
                    f'low={self._lowest_tick:.4f} atr={atr:.4f}',
                    'immediate'
                )
        return None

    # ---- L3: 保本阶梯激活 ----

    def _check_breakeven_activation(self, current_price: float) -> None:
        """检查是否应激活保本阶梯。

        保本阶梯（Breakeven Ladder）的作用：
          当浮动盈利达到一定程度时，将止损线从"可亏损"提升到"至少不亏"，
          确保盈利仓位不会因为后续的反转变成亏损。

        激活条件（默认）：
          做多：current_price - entry_price > 1.5 * ATR（盈利超过 1.5 倍 ATR）
          做空：entry_price - current_price > 1.5 * ATR

        为什么是 1.5 倍 ATR？
        1.0 倍 ATR 太容易被噪音触及（ATR 本身是平均波幅），导致过早激活
        保本，损失了策略的获利空间。
        2.0 倍 ATR 以上太严格，很多盈利仓位在回撤前都等不到激活。
        1.5 倍 ATR 是一个经验平衡值。

        激活效果：
          _breakeven_activated = True
          后续 _check_trailing_stop 会使用保本价（含手续费偏移）作为
          止损线的边界，确保退出净盈亏 >= 0。

        Args:
            current_price: 当前 tick 价格
        """
        # 已经激活则直接返回（保本只激活一次）
        if self._breakeven_activated:
            return

        # ATR 为 0 时使用价格 * 0.15% 作为 fallback
        atr = self._current_atr if self._current_atr > 0 else current_price * 0.0015

        if self._is_long:
            # 做多的浮动盈利 = 当前价 - 入场价
            profit = current_price - self._entry_price
            # 达到 1.5 倍 ATR 时激活保本
            if profit > self.breakeven_atr_mult * atr:
                self._breakeven_activated = True
        else:
            # 做空的浮动盈利 = 入场价 - 当前价
            profit = self._entry_price - current_price
            if profit > self.breakeven_atr_mult * atr:
                self._breakeven_activated = True
