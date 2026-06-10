"""
==========================================================================
模块:    base/signal_protocol
中文名:  策略信号协议定义
用途:    定义策略执行框架的核心接口和数据结构
==========================================================================

核心功能:
  本模块定义了策略系统和交易引擎之间的契约（contract），包括:
    1. SignalStrategy 协议: 策略实现必须遵循的接口标准
    2. StrategySignal 数据类: 策略输出信号的标准格式
    3. SignalKind 类: 信号方向常量定义
    4. BarSubscription 数据类: K线数据订阅描述

设计决策:
  【策略是纯信号发生器】关键架构原则:
    - 策略只负责: 接收行情 -> 计算因子 -> 生成交易信号
    - 策略不负责: 持仓管理、订单执行、风控、账户管理
    - 所有状态管理（持仓、盈亏、风控）属于 base 层（StrategySlot）

  优势:
    1. 独立测试: 策略逻辑可以在不连接交易所的情况下单独测试
    2. 因子复用: 多个策略可以共享同一因子计算结果
    3. 热切换: 策略可以动态注册/注销，不影响底层运行
    4. 关注分离: 策略写作者只需关注信号逻辑，无需关注执行细节

使用场景:
  - base/slot.py 的 StrategySlot 引用 SignalStrategy 和 BarSubscription
  - strategy/signal.py 的 SignalComposer 使用 SignalKind 常量
  - main.py 中策略实例化时使用 BarSubscription 定义订阅需求

作者:    nt-base system
版本:    1.0.0
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class SignalKind:
    """
    ==========================================================================
    信号方向常量定义
    ==========================================================================

    用途:
      定义交易信号的三种可能方向，替代魔术数字（magic number），
      提高代码可读性。

    常量说明:
      LONG  =  1 -- 做多信号，预测价格上涨，开多头仓位
      SHORT = -1 -- 做空信号，预测价格下跌，开空头仓位
      FLAT  =  0 -- 平仓/观望信号，当前无交易倾向

    使用场景:
      - StrategySignal.direction 字段取值
      - SignalComposer 合成信号时使用
      - OrderExecutor 判断执行动作
    """
    LONG = 1
    """做多信号，值 = 1。表示模型预测价格上涨，应建立多头仓位。"""
    SHORT = -1
    """做空信号，值 = -1。表示模型预测价格下跌，应建立空头仓位。"""
    FLAT = 0
    """平仓/观望信号，值 = 0。表示当前无明确方向，应平仓或保持空仓。"""


@dataclass
class StrategySignal:
    """
    ==========================================================================
    策略交易信号数据类
    ==========================================================================

    用途:
      策略通过 on_bar() 方法返回 StrategySignal 实例，
      向交易引擎传达交易指令。是策略层和执行层之间的通信载体。

    设计说明:
      - direction 是必填字段，reason 和 position_size_pct 是可选的
      - reason 用于记录信号来源，便于日志追踪和问题排查
      - position_size_pct 提供动态仓位调整能力，默认 0.0 表示使用
        StrategySlot 中的默认值

    使用示例:
      # 做多，默认仓位
      return StrategySignal(direction=1, reason="cvd_divergence_long")

      # 做空，覆盖仓位为 30%
      return StrategySignal(direction=-1, reason="breakout_short",
                            position_size_pct=0.30)

      # 平仓
      return StrategySignal(direction=0, reason="take_profit")

      # 继续持有（不返回信号，或返回 None）
      return None     # 表示无交易动作
    """
    direction: int
    """信号方向（必填），取值: SignalKind.LONG(1) / SHORT(-1) / FLAT(0)。
       1  = 做多:  开多头仓位（无持仓时）或保持多头（已持有多头）
       -1 = 做空:  开空头仓位（无持仓时）或保持空头（已持有空头）
       0  = 平仓:  平掉当前所有仓位
       注意: direction 与当前持仓方向不同时会触发反向开仓（换方向）。
             但系统设计上应该避免策略连续给出相反方向的信号，
             良好的策略应在前一个信号被平仓后才发出反向信号。"""

    reason: str = ""
    """信号产生原因（可选），默认空字符串。
       用于日志记录和问题排查，记录信号来源。
       常见取值:
         - "cvd_divergence_long": CVD背离做多信号
         - "breakout_short": 通道突破做空信号
         - "residual_momentum_long": 残差动量做多信号
         - "take_profit": 止盈信号
         - "stop_loss": 止损信号
         - "hold": 继续持有（direction=0，表示维持现状）
       注意: reason="hold" 时 direction 必须为 0，
             系统在 main.py 中特殊处理 reason="hold" 跳过平仓。"""

    position_size_pct: float = 0.0
    """仓位大小比例（可选），默认 0.0 表示使用 StrategySlot 默认值。
       如果 > 0，则覆盖 slot.position_size_pct，实现动态调仓。
       取值范围: (0, 1)，例如 0.25 表示使用 25% 权益。
       使用场景:
         - 强信号时加大仓位（如 0.40）
         - 弱信号时减小仓位（如 0.10）
         - 自适应策略根据市场状态调整仓位
       注意: 最终仓位还受 leverage 影响，仓位价值 = equity * 此值 * leverage。"""


@dataclass
class BarSubscription:
    """
    ==========================================================================
    K线数据订阅描述
    ==========================================================================

    用途:
      定义策略对行情数据的需求，包括品种、时间周期和因子列表。
      StrategyRegistry 根据所有策略的 BarSubscription 列表，
      统一向 DataManageActor 订阅数据。

    设计说明:
      - 一个策略可以订阅多个 BarSubscription（多品种、多周期）
      - hash 基于 (symbol, timeframe) 实现去重，多个策略订阅
        相同数据时只订阅一次，数据分发给所有需要它的策略
      - factors 列表决定了该 bar 到达时需要计算哪些因子

    使用示例:
      # 订阅 SOL 1分钟 bar，需要 CVD 和通道突破因子
      BarSubscription(
          symbol="SOLUSDT-PERP",
          timeframe="1m",
          factors=["cvd_divergence", "channel_breakout"]
      )

      # 订阅 BTC 5分钟 bar，需要趋势状态因子
      BarSubscription(
          symbol="BTCUSDT-PERP",
          timeframe="5m",
          factors=["trend_regime"]
      )
    """
    symbol: str
    """交易对/品种标识（字符串）。
       格式: NautilusTrader 标准的永续合约格式。
       示例:
         - "SOLUSDT-PERP": SOL 永续合约
         - "BTCUSDT-PERP": BTC 永续合约
       注意: 品种字符串必须与 DataManageActor 中订阅的品种一致。"""

    timeframe: str
    """K线时间周期（字符串）。
       格式: NautilusTrader 标准的时间周期字符串。
       示例:
         - "1m":  1 分钟线（最常用，策略主周期）
         - "5m":  5 分钟线（用于慢速因子计算）
         - "15m": 15 分钟线
         - "1h":  1 小时线
       注意: 不同周期的 bar 到达频率不同，影响计算资源消耗。"""

    factors: list[str]
    """需要在该 bar 上计算的因子名称列表（字符串列表）。
       每个因子名对应 factor/registry.py 中注册的因子函数。
       常见因子:
         - "cvd_divergence":    CVD 背离因子
         - "residual_momentum": 残差动量因子
         - "channel_breakout":  通道突破因子
         - "trend_regime":      趋势状态因子
       注意:
         - 因子名必须与 factor_registry 中的 key 完全匹配
         - 空列表表示不计算任何因子
         - 因子计算由 factor/compute.py 的 compute_factor_bar 执行"""

    def __hash__(self):
        """哈希函数，基于 (symbol, timeframe) 计算哈希值。

        用途:
          支持 BarSubscription 作为字典键或集合元素，
          实现自动去重: (BTCUSDT-PERP, 1m) 无论 factors 如何，
          都视为同一个订阅。

        设计说明:
          factors 不参与哈希，因为多个策略可能订阅同样的
          品种和周期但需要不同的因子列表。数据只需要订阅一次，
          因子计算按需执行。

        返回值:
            int -- 基于 (symbol, timeframe) 元组的哈希值。
        """
        return hash((self.symbol, self.timeframe))


class SignalStrategy(Protocol):
    """
    ==========================================================================
    策略信号生成器协议（接口定义）
    ==========================================================================

    用途:
      定义策略实现必须遵循的接口规范（Protocol），
      这是 Python 结构化子类型（structural subtyping）的应用。
      任何实现了这些方法和属性的类都自动被视为 SignalStrategy，
      无需显式继承。

    设计说明:
      【协议 vs 抽象基类】
      - 使用 Protocol 而非 ABC，因为不同策略可能有不同的基类需求
      - 只要"长得像" SignalStrategy（拥有相同的方法签名）即可
      - 便于测试时创建 Mock 对象

      【方法职责】
      - strategy_id:    策略的唯一标识
      - subscriptions:  策略的数据需求声明
      - on_bar():       核心方法，接收 bar 返回信号
      - on_shutdown():  资源清理
      - get_diagnostics(): 监控信息暴露

      【线程安全】
      on_bar() 可能被多线程调用（多个 bar 同时到达），
      策略实现需要注意线程安全。
      建议: 不在策略内维护可变状态，所有状态放在 StrategySlot 中。
    """

    @property
    def strategy_id(self) -> str:
        """策略唯一标识符（只读属性）。

        返回值:
            str -- 策略 ID，格式示例: "AlphaV2-005"

        用途:
            - 日志记录中标识策略来源
            - gRPC 通信中作为策略标识
            - 数据库 strategy_instances 表的外键关联

        实现要求:
            必须返回唯一的字符串，不同策略实例不能重名。
        """
        ...

    @property
    def subscriptions(self) -> list[BarSubscription]:
        """策略的数据订阅列表（只读属性）。

        返回值:
            list[BarSubscription] -- BarSubscription 对象列表，
            定义了策略需要哪些品种/周期的 K 线数据。

        用途:
            - 启动时 StrategyRegistry 收集所有策略的订阅需求
            - 向 DataManageActor 注册数据推送
            - 确定 bar 到达后需要计算哪些因子

        实现要求:
            返回列表中的 subscription 应该是固定的，
            不要在运行时动态修改（会导致订阅不一致）。
        """
        ...

    def on_bar(self, bar_data: dict) -> StrategySignal | None:
        """处理收到的 K 线数据，生成交易信号。

        这是策略最核心的方法，每次有新的 bar 到达时被调用。

        参数:
            bar_data: dict -- 包含该 bar 的完整行情数据和因子值。
            字典结构示例:
            {
                "symbol": "SOLUSDT-PERP",           # 品种
                "timeframe": "1m",                   # 时间周期
                "open": 145.23,                      # 开盘价
                "high": 146.50,                      # 最高价
                "low": 144.80,                       # 最低价
                "close": 146.20,                     # 收盘价
                "volume": 1234567.89,                # 成交量
                "ts_event": 1234567890.123,          # 时间戳
                "factors": {                         # 因子值字典
                    "cvd_divergence": 0.75,
                    "residual_momentum": 0.32,
                    "channel_breakout": -0.45,
                    "trend_regime": 1.0
                }
            }
            注意: factors 字段可能为空字典（如果没有因子订阅）。

        返回值:
            StrategySignal | None:
                - StrategySignal: 有交易动作时的信号对象
                - None: 不需要交易（继续持仓或保持空仓）

        实现要点:
            1. 从 bar_data["factors"] 读取预计算的因子值
            2. 使用因子值通过策略逻辑判断方向
            3. 返回 StrategySignal 或 None
            4. 不直接访问账户/持仓信息（由 base 层管理）
            5. 不应该抛出异常，异常由调用方捕获处理

        异常安全:
            建议策略实现内部 try-except，确保单次 bar 处理失败
            不会影响后续 bar 的处理。
        """
        ...

    def on_shutdown(self) -> None:
        """优雅关闭策略，释放资源。

        调用时机:
            - 系统正常关闭时
            - 策略动态注销时
            - 配置热更新重新加载时

        典型操作:
            - 关闭子线程/进程
            - 释放数据库连接
            - 持久化缓存数据
            - 发送关闭通知

        实现要求:
            必须可多次调用（幂等性），
            不能在首次调用后报错。
        """
        ...

    def get_diagnostics(self) -> dict:
        """返回策略的诊断/监控数据。

        返回值:
            dict -- 策略的内部状态快照，用于运维监控。
            字典示例:
            {
                "bars_received": 12345,        # 收到的 bar 数量
                "signals_generated": 89,       # 生成的信号数量
                "avg_compute_ms": 12.5,        # 平均计算耗时(ms)
                "last_bar_time": "2026-06-10 10:30:00",  # 最后 bar 时间
                "errors": 0                    # 错误计数
            }

        用途:
            - gRPC 的 diagnostics 接口返回给运维监控
            - 日志中定期输出策略健康状态
            - 调试时查看策略内部状态

        实现要求:
            1. 返回的 dict 必须是可 JSON 序列化的
            2. 不抛出异常
            3. key 使用英文（兼容 gRPC 的 JSON 序列化）
        """
        ...
