# -*- coding: utf-8 -*-
"""
===========================================================
模块:    base/v2_adapter
模块名:  策略协议适配器
===========================================================
用途:    协议适配器，桥接 nt-base 的 SignalStrategy 协议接口
         和 trading-v2 的 AlphaSignal 策略逻辑。

类: V2SignalAdapter
  职责:
    1. 将 nt-base 的 bar_data (dict 格式) 翻译为 trading-v2 AlphaSignal 的
       on_bar(close, high, low, delta_buy, delta_sell, btc_close, ts_ns) 调用
    2. 在 on_bar 之前将因子值推送到 AlphaSignal (set_factor_value)
    3. 实现 nt-base 的 SignalStrategy 协议接口 (strategy_id, subscriptions,
       on_bar, on_tick, on_shutdown, get_diagnostics)

协议翻译:
  nt-base bar_data dict 键:
    "close", "high", "low"  -> 转为 float
    "ts_ns"                 -> 直接传递
    "factors"               -> {因子名: 值} 字典, 在 on_bar 前推送
    "btc_close" (可选)      -> 如果缺失则使用 close 作为代理值
    "delta_buy"/"delta_sell" -> 如果缺失则默认 0.0

设计决策:

  适配器拥有因子值推送的职责。这确保了 nt-base 的因子计算
  与 AlphaSignal 的因子消费完全解耦。
  AlphaSignal.on_bar() 看到的因子值来自 bar_data['factors']，
  仿佛它们是由回测循环或实盘因子循环推送的一样。

架构说明:
  本模块是 nt-base 和 trading-v2 两个独立服务之间的关键协议转换层：
  nt-base  (信号协议): on_bar(bar_data: dict) -> StrategySignal | None
  trading-v2 (策略代码): on_bar(close, high, low, delta_buy, delta_sell,
                                btc_close, ts_ns) -> StrategySignal

  Strategy 代码 (AlphaSignal) 保持纯净 —— 不涉及数据库、NT 框架或 I/O。

作者:    nt-base 系统
版本:    1.0.0
===========================================================
"""
from __future__ import annotations

from base.signal_protocol import StrategySignal, BarSubscription


class V2SignalAdapter:
    """适配器：将 trading-v2 的 AlphaSignal 包装为 nt-base 的 SignalStrategy 协议。

    两个项目都使用 @dataclass StrategySignal(direction, reason) —— 结构兼容。
    """

    def __init__(self, alpha_signal, strategy_id: str,
                 symbol: str, timeframe: str = "1m"):
        """初始化适配器。

        Args:
            alpha_signal: trading-v2 的 AlphaSignal 实例 (策略信号生成器)
            strategy_id: 策略唯一标识符 (如 "AlphaV2-005")
            symbol: 交易品种代码 (如 "SOLUSDT-PERP")
            timeframe: bar 时间周期 (默认 "1m")
        """
        self._signal = alpha_signal
        self._id = strategy_id
        self._symbol = symbol
        self._tf = timeframe
        # 构建订阅信息：从 AlphaSignal 的 factor_names 派生需要的因子列表
        self._subs = [
            BarSubscription(
                symbol=symbol,
                timeframe=timeframe,
                factors=alpha_signal.factor_names,
            )
        ]

    # ═══════════════════════════════════════════════════════════
    # SignalStrategy 协议实现
    # ═══════════════════════════════════════════════════════════

    @property
    def strategy_id(self) -> str:
        """返回策略 ID。"""
        return self._id

    @property
    def subscriptions(self) -> list[BarSubscription]:
        """返回 Bar 订阅列表，包含需要的因子名称集合。

        nt-base 的因子引擎根据此列表计算对应因子并附加到 bar_data 中。
        """
        return self._subs

    def on_bar(self, bar_data: dict) -> StrategySignal | None:
        """协议转换核心方法：nt-base bar dict -> trading-v2 on_bar 调用。

        转换步骤:
        1. 从 dict 中提取 close, high, low, ts_ns
        2. 推送因子值到 AlphaSignal (set_factor_value)
        3. 提取 btc_close (缺失时用 close 代理)
        4. 提取 delta_buy/delta_sell (缺失时默认 0)
        5. 调用 AlphaSignal.on_bar() 生成信号
        6. 将结果转换为统一格式的 StrategySignal

        Args:
            bar_data: nt-base 传递的 bar 数据字典，包含:
                - close/high/low: 价格数据
                - ts_ns: 纳秒时间戳
                - factors: {因子名: 值} 字典
                - btc_close (可选): BTC 收盘价 (用于残差动量因子)
                - delta_buy/delta_sell (可选): 买卖成交量差

        Returns:
            StrategySignal | None: 策略信号 (direction: -1/0/1, reason: str)
        """
        close = float(bar_data.get("close", 0))
        high = float(bar_data.get("high", close))
        low = float(bar_data.get("low", close))
        ts_ns = bar_data.get("ts_ns", 0)

        # ── 推送因子值到 AlphaSignal ────────────────────────────
        # AlphaSignal 维护一个因子值队列 (_factor_buffers)，
        # on_bar 内部通过 _get_factor() 获取这些值用于信号合成。
        # 这里将 nt-base 计算的因子值逐个推入，保持因子值与 bar 同步。
        factors = bar_data.get("factors", {})
        for fname, value in factors.items():
            self._signal.set_factor_value(fname, ts_ns, float(value))

        # ── btc_close ────────────────────────────────────────────
        # 如果 nt-base 未提供 BTC 收盘价 (例如 BTC bar 尚未到达)，
        # 使用 SOL close 作为代理值 —— 虽不精确但避免因子崩溃。
        btc_close = float(bar_data.get("btc_close", close))

        # ── delta vol ────────────────────────────────────────────
        # nt-base 在 1m 级别 bar 上不跟踪 tick 级买卖量差，
        # 所以默认值为 0.0。若有需要可在 tick 聚合时计算。
        delta_buy = float(bar_data.get("delta_buy", 0.0))
        delta_sell = float(bar_data.get("delta_sell", 0.0))

        # ── position ────────────────────────────────────────────
        # nt-base 构建的 PositionState protobuf（包含持仓方向、开仓价、ATR 等）
        position = bar_data.get("position")

        # ── 调用策略代码 ────────────────────────────────────────
        result = self._signal.on_bar(
            close=close, high=high, low=low,
            delta_buy_vol=delta_buy, delta_sell_vol=delta_sell,
            btc_close=btc_close, ts_ns=ts_ns,
            position=position,
        )

        if result is None:
            return None
        # 两个项目都使用 @dataclass StrategySignal(direction, reason) —— 结构兼容
        return StrategySignal(direction=result.direction, reason=result.reason)

    def on_tick(self, price, size, is_buyer, ts_ns, symbol=""):
        """委托到 AlphaSignal.on_tick (如果存在)。"""
        if hasattr(self._signal, "on_tick"):
            self._signal.on_tick(price, size, is_buyer, ts_ns, symbol)

    def on_shutdown(self) -> None:
        """委托到 AlphaSignal.on_shutdown (如果存在)。"""
        if hasattr(self._signal, 'on_shutdown'):
            self._signal.on_shutdown()

    def get_diagnostics(self) -> dict:
        """委托到 AlphaSignal.get_diagnostics() 获取策略诊断信息。"""
        return self._signal.get_diagnostics()

    # ═══════════════════════════════════════════════════════════
    # 内部访问
    # ═══════════════════════════════════════════════════════════

    @property
    def inner(self):
        """访问原始的 AlphaSignal 实例。"""
        return self._signal
