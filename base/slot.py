"""
==========================================================================
模块:    base/slot
中文名:  策略运行时状态容器
用途:    保存已注册交易策略的完整运行状态
==========================================================================

核心功能:
  StrategySlot 是一个数据类，存储单个策略实例的完整运行状态，包括:
    1. 策略配置参数（ID、订阅、风控阈值、仓位管理等）
    2. 运行时状态（是否持仓、入场价格/方向/时间、每日盈亏等）
    3. Telegram 通知配置（每个策略可独立配置机器人令牌和聊天ID）
    4. 计算属性（如 held_sec 计算持仓时长）

设计理念:
  - 分离配置与状态: 策略的配置参数和运行时状态放在同一个对象中，
    便于集中管理
  - 每个策略实例对应一个 StrategySlot，由 StrategyRegistry 统一管理
  - 不包含任何业务逻辑，仅作为数据容器

使用场景:
  - main.py 中通过 registry.get_slot(strategy_id) 获取策略状态
  - executor.py 中通过 slot 信息执行下单、平仓操作
  - risk/loop.py 中通过 slot 信息执行风控检查
  - notify.py 中通过 slot 的 telegram 配置发送通知

不变量:
  - has_position == False  =>  entry_price == 0.0 and entry_side == ""
  - tripped == True        =>  不再执行新交易（熔断保护）

作者:    nt-base system
版本:    1.2.0
"""
from __future__ import annotations

from dataclasses import dataclass, field
import time

from base.signal_protocol import SignalStrategy, BarSubscription


@dataclass
class StrategySlot:
    """
    ==========================================================================
    策略运行时状态槽
    ==========================================================================

    用途:
      存储单个策略实例的完整运行状态，包括配置参数和运行时动态数据。
      由 StrategyRegistry 管理，供执行器、风控循环、通知模块使用。

    属性分组说明:
      【配置参数】由 strategy_instances 数据库表初始化，启动后一般不修改
      【运行时状态】随着策略运行动态变化，每次交易后更新
      【通知配置】每个策略可独立配置 Telegram 通知，实现多策略多渠道通知
    """

    # =========================================================================
    # 【配置参数】-- 策略标识与核心组件
    # =========================================================================
    strategy_id: str
    """策略实例唯一标识符，对应 strategy_instances 表中的 id 字段。
       格式示例: "AlphaV2-005"，用于日志、数据库、gRPC 通信中的策略识别。"""

    strategy: SignalStrategy
    """策略信号生成器实例，实现了 SignalStrategy 协议。
       该对象负责:
         - 定义 bar 数据订阅（on_bar 回调触发）
         - 接收行情数据后计算信号（返回 StrategySignal）
         - shutdown 时执行清理
       base 层不关心策略内部逻辑，只通过协议接口交互。"""

    # =========================================================================
    # 【配置参数】-- 数据订阅
    # =========================================================================
    subscriptions: list[BarSubscription] = field(default_factory=list)
    """策略订阅的 K 线数据列表。
       每个 BarSubscription 包含 symbol、timeframe、factors 三个字段。
       框架根据此列表向策略推送对应的 bar 数据。
       一个策略可以订阅多个品种/多时间周期的 bar。"""

    # =========================================================================
    # 【风控参数】-- 止损/止盈/持仓时长/日亏损/冷却时间
    # =========================================================================
    stop_pct: float = 0.03
    """硬止损比例（浮点数），默认 0.03 表示 3%。
       当价格向不利方向波动超过此比例时触发硬止损平仓。
       计算方式: LONG 仓位价格下跌超过 entry_price * stop_pct 时触发。
       取值范围: (0, 1)，通常 0.01~0.10。"""

    take_pct: float = 0.06
    """止盈比例（浮点数），默认 0.06 表示 6%。
       当价格向有利方向波动超过此比例时触发止盈平仓。
       计算方式: LONG 仓位价格上涨超过 entry_price * take_pct 时触发。
       取值范围: (0, 1)，通常 > stop_pct。"""

    max_hold_sec: int = 3600
    """最大持仓时间（秒），默认 3600 秒 = 1 小时。
       超过此时间后强制平仓，防止仓位长时间持有带来隔夜风险。
       取值范围: 正整数，通常 600~86400。"""

    max_daily_loss_pct: float = 0.05
    """每日最大亏损比例（浮点数），默认 0.05 表示 5%。
       当日累计亏损超过此比例时触发熔断（tripped = True），
       停止当日所有新交易。
       取值范围: (0, 1)，通常 0.02~0.10。
       重置: 每日 0 点由风控循环自动重置 tripped 标志。"""

    cooldown_sec: float = 60.0
    """交易冷却时间（秒），默认 60 秒。
       平仓后必须等待至少此时间才能再次开仓。
       防止频繁交易导致的过度手续费和滑点。
       取值范围: 正浮点数，通常 10~300。"""

    leverage: int = 2
    """杠杆倍数（整数），默认 2 倍。
       实际仓位价值 = 权益 * position_size_pct * leverage。
       取值范围: 正整数，通常 1~10（根据 Binance 规则限制）。"""

    position_size_pct: float = 0.20
    """单次开仓资金占比（浮点数），默认 0.20 表示 20%。
       每次开仓使用的资金占当前权益的比例。
       最终仓位价值 = 权益 * position_size_pct * leverage。
       取值范围: (0, 1)，通常 0.05~0.50。"""

    symbol: str = ""
    """交易对标识（字符串），默认空字符串。
       示例: "SOLUSDT-PERP"（NautilusTrader 格式的永续合约）。
       决定了该策略在哪个品种上开仓。"""

    # =========================================================================
    # 【通知配置】-- 每个策略可独立配置 Telegram 通知
    # =========================================================================
    telegram_bot_token: str = ""
    """Telegram 机器人令牌（字符串），用于发送策略通知。
       每个策略可配置不同的 Bot Token:
         - 不同策略通知发到不同的 Telegram Bot
         - 或者共用同一个 Bot（配置相同 Token）
       格式: "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
       为空时不发送 Telegram 通知。"""

    telegram_chat_id: str = ""
    """Telegram 聊天 ID（字符串），对应机器人的订阅者/群组。
       与 telegram_bot_token 配合使用。
       格式: 数字字符串，如 "123456789"
       为空时不发送 Telegram 通知。"""

    # =========================================================================
    # 【运行时状态】-- 持仓状态
    # =========================================================================
    has_position: bool = False
    """是否当前持有仓位（布尔值）。
       True 表示有敞口，False 表示无仓位。
       开仓时由 open_position() 设为 True。
       平仓时由 reset_position() 设为 False。"""

    entry_price: float = 0.0
    """仓位入场价格（浮点数）。
       开仓时记录的实际成交价格。
       无仓位时值为 0.0。
       用于计算浮动盈亏和止损/止盈触发价格。"""

    entry_side: str = ""
    """仓位方向（字符串），取值为 "LONG" 或 "SHORT"。
       "LONG" = 做多（买涨），"SHORT" = 做空（买跌）。
       无仓位时值为空字符串 ""。"""

    entry_time: float = 0.0
    """仓位开仓时间戳（浮点数），Unix 时间戳（秒）。
       通过 time.time() 获取，用于计算持仓时长。
       无仓位时值为 0.0。
       通过 held_sec 属性获取当前持仓时长。"""

    last_trade_time: float = 0.0
    """最近一次交易时间戳（浮点数），Unix 时间戳（秒）。
       包括开仓和平仓操作，用于冷却时间判断。
       如果 time.time() - last_trade_time < cooldown_sec，
       则禁止开新仓。"""

    # =========================================================================
    # 【运行时状态】-- 日盈亏与熔断
    # =========================================================================
    daily_pnl: float = 0.0
    """当日累计盈亏（浮点数），以 USDT 计价。
       正值表示盈利，负值表示亏损。
       每日 0 点由风控循环重置为 0。
       当亏损比例超过 max_daily_loss_pct 时触发熔断。"""

    daily_start_equity: float = 0.0
    """当日初始权益（浮点数），以 USDT 计价。
       每日 0 点记录，用于计算当日亏损比例。
       亏损比例 = abs(daily_pnl) / daily_start_equity"""

    tripped: bool = False
    """熔断标志（布尔值）。
       True = 熔断已触发，禁止所有新交易。
       触发条件: 当日亏损超过 max_daily_loss_pct。
       重置: 每日 0 点自动重置为 False。
       注意: tripped 只阻止开新仓，不影响已有仓位的平仓操作。"""

    # =========================================================================
    # 【运行时状态】-- 追踪止损价格跟踪
    # =========================================================================
    highest_since_entry: float = 0.0
    """开仓以来出现的最高价格（浮点数）。
       仅 LONG 仓位有意义，用于计算追踪止损的触发价格。
       TickExitManager 在每次收到 tick 时更新此值。
       追踪止损触发: current_price < highest_since_entry * (1 - trail_pct)"""

    lowest_since_entry: float = float("inf")
    """开仓以来出现的最低价格（浮点数），初始值为正无穷。
       仅 SHORT 仓位有意义，用于计算追踪止损的触发价格。
       TickExitManager 在每次收到 tick 时更新此值。
       追踪止损触发: current_price > lowest_since_entry * (1 + trail_pct)"""

    current_atr: float = 0.0
    """最新的 ATR（Average True Range）值（浮点数）。
       用于动态计算追踪止损的距离。
       ATR 越大，止损距离越宽，防止被市场噪音扫出。
       默认 0.0，由外部模块在 bar 到达时更新。"""

    breakeven_activated: bool = False
    """保本止损是否已激活（布尔值）。
       由 TickExitManager 的 L3 Breakeven 层设置。
       当价格朝有利方向波动超过一定幅度后，
       将止损位上移至入场价，确保至少保本出场。"""

    entry_commission: float = 0.0
    """入场累计手续费（浮点数），以 USDT 计价。
       由 OrderExecutor.on_fill() 在入场成交确认时记录。
       平仓计算盈亏时从 slot 读取以得到双边手续费后的净盈亏。
       在 reset_position() 时重置为 0.0。"""

    # =========================================================================
    # 【运行时状态】-- Bar 级退出请求
    # =========================================================================
    pending_bar_exit: str = ""
    """Bar 级退出原因（字符串），为空表示无退出请求。
       由 ExitManager（bar 级退出）在 bar 到达时设置退出原因，
       由 TickExitManager 或 OrderExecutor 在 tick 级或下单前消费。
       取值示例:
         - ""       : 无退出请求
         - "btc_shock": BTC 剧烈波动触发退出
         - "cvd_reversal": CVD 背离反转触发退出
         - "time_decay": 持仓超时触发退出
         - "hard_stop": 硬止损触发退出
       在 reset_position() 时清空。"""

    # =========================================================================
    # 【计算属性】-- 根据运行时状态动态计算
    # =========================================================================
    @property
    def held_sec(self) -> float:
        """计算当前持仓时长（秒）。

        返回值:
            float -- 持仓时长（秒），无持仓时返回 0.0。

        计算逻辑:
            has_position == True:  time.time() - entry_time
            has_position == False: 0.0

        使用场景:
            - 风控循环检查是否超过 max_hold_sec
            - 日志中记录持仓时长

        注意:
            此属性每次调用都动态计算，无需手动更新。
        """
        if not self.has_position:
            return 0.0
        return time.time() - self.entry_time

    # =========================================================================
    # 【状态管理方法】-- 仓位生命周期管理
    # =========================================================================
    def reset_position(self):
        """重置所有仓位相关状态为初始值。

        使用场景:
            - 平仓成功后由 OrderExecutor 调用
            - 风控强制平仓后由 risk/loop.py 调用
            - 系统开盘重置时调用

        重置字段列表:
            - pending_bar_exit: 清空退出原因
            - has_position: 设为 False（无持仓）
            - entry_price: 设为 0.0
            - entry_side: 设为空字符串
            - entry_time: 设为 0.0
            - highest_since_entry: 设为 0.0
            - lowest_since_entry: 设为正无穷 float("inf")

        不重置字段（这些字段状态跨越多个持仓周期）:
            - last_trade_time: 保持最近交易时间（用于冷却判断）
            - daily_pnl / daily_start_equity: 保持当日数据
            - tripped: 保持熔断状态
            - current_atr: 保持 ATR 参考值

        设计说明:
            highest_since_entry 初始为 0.0，因为价格总是 > 0
            lowest_since_entry 初始为 float("inf")，因为任何正价格都小于 inf
        """
        self.pending_bar_exit = ""
        self.has_position = False
        self.entry_price = 0.0
        self.entry_side = ""
        self.entry_time = 0.0
        self.highest_since_entry = 0.0
        self.lowest_since_entry = float("inf")
        self.entry_commission = 0.0

    def open_position(self, side: str, price: float):
        """设置开仓状态，初始化所有价格跟踪字段。

        参数:
            side: str -- 开仓方向，取值为 "LONG" 或 "SHORT"
            price: float -- 入场价格，取实际成交价或标记价格。

        初始化逻辑:
            1. has_position = True，标记有持仓
            2. entry_side = side，记录方向
            3. entry_price = price，记录入场价
            4. entry_time = time.time()，记录入场时间
            5. 初始化价格跟踪字段:
               - LONG: highest_since_entry = price（当前价为最高点）
               - LONG: lowest_since_entry = float("inf")（从最大值开始跟踪）
               - SHORT: highest_since_entry = 0.0（从最小值开始跟踪）
               - SHORT: lowest_since_entry = price（当前价为最低点）

        设计说明:
            价格跟踪字段的初始值确保追踪止损能正确工作:
            - LONG: 开仓时当前价就是最高点，所以 highest = price
              后续价格必须突破此值才能上移止损线
            - SHORT: 开仓时当前价就是最低点，所以 lowest = price
              后续价格必须跌破此值才能下移止损线
        """
        self.has_position = True
        self.entry_side = side
        self.entry_price = price
        self.entry_time = time.time()
        # LONG 仓位: 初始最高价 = 入场价, 初始最低价 = 正无穷（后续任何下跌都会更新）
        self.highest_since_entry = price if side == "LONG" else 0.0
        # SHORT 仓位: 初始最低价 = 入场价, 初始最高价 = 0（后续任何上涨都会更新）
        self.lowest_since_entry = price if side == "SHORT" else float("inf")
