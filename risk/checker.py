# -*- coding: utf-8 -*-
"""
风险检查模块 (risk/checker)
============================

模块定位
--------
本模块位于 nt-base 风控系统的核心层，提供一系列纯函数用于检查持仓是否
需要退出。它不持有任何状态，所有输入通过参数传递，输出为 RiskAction 结果。

在风控体系中的角色
------------------
RiskLoop（每秒循环）调用本模块的检查函数，对每个活跃持仓逐一执行：
  - 跟踪止损 (trailing stop)
  - 硬止损 (hard stop-loss)
  - 止盈 (take-profit)
  - 持仓时间上限 (max hold time)
  - 日亏损熔断 (daily loss circuit breaker)

所有函数遵循同一接口：接受 StrategySlot + 当前价格，返回 RiskAction。
调用方（RiskLoop）保证 slot.has_position == True。

核心功能列表
------------
1. check_trail  -- 基于入场以来最高/最低价的浮动跟踪止损
2. check_stop   -- 以入场价为基准的固定比例硬止损
3. check_take   -- 以入场价为基准的固定比例止盈
4. check_hold   -- 持仓时长超过上限时强制退出
5. check_daily  -- 当日累计亏损超过上限时熔断（永久禁用该策略槽）
6. check_all    -- 批量执行上述全部检查，返回所有触发的风险动作

设计原则
--------
- 纯函数：无副作用，可测试性强
- 费前计算：所有盈亏百分比都扣除双边手续费，避免假盈利
- 单一职责：每个函数只做一种检查
- 防御性：对无持仓状态返回 "none"

Author: nt-base system
Version: 1.1.0
"""
from __future__ import annotations
"""风险检查模块 -- 纯函数，用于跟踪止损/硬止损/止盈/持仓时间/日亏损检查。"""
from dataclasses import dataclass
from base.slot import StrategySlot

# Binance USDT 合约 Taker 手续费率 (0.04%)
# 用于盈亏计算时扣除双边开平仓手续费，确保止盈止损阈值覆盖交易成本
FEE_RATE: float = 0.0004


def _fee_adj_pnl_pct(slot: StrategySlot, current_price: float) -> float:
    """计算扣除手续费后的实际盈亏百分比。

    为什么需要手续费调整？
    Binance USDT 永续合约的 taker 手续费为 0.04%，开仓和平仓各扣一次。
    如果不扣除手续费，策略会在实际亏损时误判为盈利/保本，导致止盈止损偏移。

    做多 (LONG) 的费前计算：
      开仓成本 = entry_price * (1 + FEE_RATE)    # 买入时多付手续费
      平仓收入 = current_price * (1 - FEE_RATE)   # 卖出时少收手续费
      盈亏率   = (平仓收入 - 开仓成本) / 开仓成本

    做空 (SHORT) 的费前计算：
      开仓收入 = entry_price * (1 - FEE_RATE)    # 卖出时少收手续费
      平仓成本 = current_price * (1 + FEE_RATE)   # 买入时多付手续费
      盈亏率   = (开仓收入 - 开仓成本) / 开仓收入

    Args:
        slot: 策略槽对象，包含 entry_price（入场价）和 entry_side（方向）
        current_price: 当前最新价格

    Returns:
        扣除双边手续费后的实际盈亏百分比（小数形式，如 -0.025 表示 -2.5%）
    """
    if slot.entry_side == "LONG":
        # 做多：先买入后卖出
        entry_cost = slot.entry_price * (1.0 + FEE_RATE)       # 开仓成本（含买入手续费）
        exit_proceeds = current_price * (1.0 - FEE_RATE)       # 平仓收入（扣除卖出手续费）
        return (exit_proceeds - entry_cost) / entry_cost       # 实际盈亏 / 开仓成本
    else:
        # 做空：先卖出后买入
        entry_proceeds = slot.entry_price * (1.0 - FEE_RATE)   # 开仓收入（扣除卖出手续费）
        exit_cost = current_price * (1.0 + FEE_RATE)           # 平仓成本（含买入手续费）
        return (entry_proceeds - exit_cost) / entry_proceeds   # 实际盈亏 / 开仓收入


@dataclass
class RiskAction:
    """风险动作 -- 表示风控检查的结果。

    当某个风控条件被触发时，返回一个非空的 RiskAction 实例，
    should_exit 属性为 True，调用方据此执行平仓。

    Fields:
        kind (str):   风险动作类型标识
                      取值范围: "none" | "trail_stop" | "stop_loss" | "take_profit"
                               | "max_hold" | "daily_limit"
                      空字符串或 "none" 表示无动作
        reason (str): 触发原因的详细描述，包含关键数值以便日志排查
                      例如: "trail LONG 23.45 <= 23.50 (high=24.10)"
    """
    kind: str = ""          # 风险类型，默认空字符串表示未触发
    reason: str = ""        # 触发原因描述

    @property
    def should_exit(self) -> bool:
        """是否应该平仓。

        当 kind 不为 "none" 时，表示某个风控条件被触发，需要立即平仓。
        调用方通过此属性快速判断是否需要执行 flat() 操作。
        """
        return self.kind != "none"


# ============================================================================
# 一、跟踪止损 (Trailing Stop) -- Tick 级别
# ============================================================================
# 原理：记录持仓以来的最高价（做多）或最低价（做空），
#       当价格从极值回撤超过设定距离时触发平仓。
# 优势：在趋势行情中能锁定大部分利润，仅在反转时退出。
# 距离：使用 stop_pct * entry_price 作为回撤容忍度。

def check_trail(slot: StrategySlot, current_price: float) -> RiskAction:
    """跟踪止损检查 -- 基于入场以来的价格极值计算浮动止损线。

    算法逻辑：
      1. 做多时，止损价 = 最高价 - 回撤距离，当最新价 <= 止损价时触发
      2. 做空时，止损价 = 最低价 + 回撤距离，当最新价 >= 止损价时触发
      3. 回撤距离 = stop_pct * entry_price（使用与硬止损相同的比例）

    边界条件：
      - 无持仓时直接返回 none（防御性检查，尽管调用方已保证）
      - highest_since_entry <= 0 表示尚未收到 tick，跳过检查
      - lowest_since_entry 初始值为 inf，用 inf - 1 做容差判断

    Args:
        slot: 策略槽对象，提供持仓方向、入场价、最高最低价等信息
        current_price: 当前最新价格（来自 tick 流）

    Returns:
        若触发止损返回 RiskAction(kind="trail_stop", reason=...)，
        否则返回 RiskAction("none")
    """
    if not slot.has_position:
        return RiskAction("none")

    # 使用当前 ATR 或回退到入场价的 0.15% 作为动态回撤距离
    # slot.current_atr 由 RiskLoop 每秒更新
    atr = slot.current_atr if slot.current_atr > 0 else slot.entry_price * 0.0015
    # 回撤距离 = stop_pct * 入场价（stop_pct 是策略配置的止损比例，如 0.03 表示 3%）
    trail_distance = slot.stop_pct * slot.entry_price

    if slot.entry_side == "LONG":
        # 做多跟踪止损逻辑：
        # highest_since_entry 记录入场以来的最高价（由 RiskLoop 更新）
        # 只有收到过至少一个 tick（highest > 0）才启用跟踪止损
        if slot.highest_since_entry <= 0:
            return RiskAction("none")
        # 止损线 = 最高价 - 回撤距离
        # 随着最高价上移，止损线同步上移，但永远不会下移
        stop_price = slot.highest_since_entry - trail_distance
        if current_price <= stop_price:
            return RiskAction(
                "trail_stop",
                f"trail LONG {current_price:.4f} <= {stop_price:.4f} (high={slot.highest_since_entry:.4f})"
            )
    else:  # SHORT
        if slot.lowest_since_entry >= float("inf") - 1:
            return RiskAction("none")
        stop_price = slot.lowest_since_entry + trail_distance
        if current_price >= stop_price:
            return RiskAction(
                "trail_stop",
                f"trail SHORT {current_price:.4f} >= {stop_price:.4f} (low={slot.lowest_since_entry:.4f})"
            )
    return RiskAction("none")


# ============================================================================
# 二、固定止损 (Fixed Stop Loss)
# ============================================================================
# 最简单直接的止损方式：以入场价为基准，价格反向运动超过设定比例时退出。
# 与跟踪止损的区别：不随价格极值移动，始终以入场价为锚点。

def check_stop(slot: StrategySlot, current_price: float) -> RiskAction:
    """固定比例止损检查 -- 基于入场价的费后盈亏百分比。

    触发条件：费后盈亏百分比 <= -stop_pct（如 -3%）
    使用 _fee_adj_pnl_pct 确保扣除了双边手续费，避免因手续费导致
    实际亏损超出预期。

    Args:
        slot: 策略槽对象（含 entry_price, stop_pct, entry_side）
        current_price: 当前最新价格

    Returns:
        触发止损时返回 RiskAction(kind="stop_loss", reason=...)，
        否则返回 RiskAction("none")
    """
    if not slot.has_position:
        return RiskAction("none")
    pnl_pct = _fee_adj_pnl_pct(slot, current_price)
    if pnl_pct <= -slot.stop_pct:
        return RiskAction("stop_loss", f"stop {pnl_pct:.4f}")
    return RiskAction("none")


# ============================================================================
# 三、止盈 (Take Profit)
# ============================================================================

def check_take(slot: StrategySlot, current_price: float) -> RiskAction:
    """止盈检查 -- 费后盈亏达到目标比例时退出。

    触发条件：费后盈亏百分比 >= take_pct（如 6%）
    同样扣除手续费，确保实际到手利润不低于预期。

    Args:
        slot: 策略槽对象（含 entry_price, take_pct）
        current_price: 当前最新价格

    Returns:
        触发止盈时返回 RiskAction(kind="take_profit", reason=...)，
        否则返回 RiskAction("none")
    """
    if not slot.has_position:
        return RiskAction("none")
    pnl_pct = _fee_adj_pnl_pct(slot, current_price)
    if pnl_pct >= slot.take_pct:
        return RiskAction("take_profit", f"take {pnl_pct:.4f}")
    return RiskAction("none")


# ============================================================================
# 四、持仓时间上限 (Max Hold Time)
# ============================================================================

def check_hold(slot: StrategySlot, current_price: float = 0.0) -> RiskAction:
    """最大持仓时间检查 -- 超过设定时长后强制平仓。

    current_price 参数未使用，但保留以统一签名方便 RiskLoop 批量调用。

    触发条件：slot.held_sec >= slot.max_hold_sec
    held_sec 由 StrategySlot 根据开仓时间戳实时计算。

    Args:
        slot: 策略槽对象（含 held_sec, max_hold_sec）
        current_price: 未使用，保留以统一接口

    Returns:
        超时时返回 RiskAction(kind="max_hold", reason=...)，
        否则返回 RiskAction("none")
    """
    if not slot.has_position:
        return RiskAction("none")
    if slot.held_sec >= slot.max_hold_sec:
        return RiskAction("max_hold", f"held {slot.held_sec:.0f}s")
    return RiskAction("none")


# ============================================================================
# 五、日亏损熔断 (Daily Loss Circuit Breaker)
# ============================================================================

def check_daily(slot: StrategySlot) -> RiskAction:
    """日亏损熔断检查 -- 当日总亏损超过上限时禁用策略。

    触发条件：daily_pnl / daily_start_equity < -max_daily_loss_pct
    例如：起始权益 10000 USDT，日亏损上限 10%（0.1），
          当日已亏损 1200 USDT，则 1200/10000 = 0.12 > 0.1，触发熔断。

    熔断效果：slot.tripped 被设置为 True（由 RiskLoop 执行），
             该槽不再开新仓，已持仓也会被平仓，直到运维手动重置。

    Args:
        slot: 策略槽对象（含 daily_pnl, daily_start_equity, max_daily_loss_pct）

    Returns:
        触发熔断时返回 RiskAction(kind="daily_limit", reason=...)，
        否则返回 RiskAction("none")
    """
    if slot.daily_start_equity <= 0:
        return RiskAction("none")
    daily_ret = slot.daily_pnl / slot.daily_start_equity
    if daily_ret < -slot.max_daily_loss_pct:
        return RiskAction("daily_limit", f"daily loss {daily_ret:.4f}")
    return RiskAction("none")


# ============================================================================
# 六、批量检查 (Bulk Check)
# ============================================================================

def check_all(slot: StrategySlot, current_price: float) -> list[RiskAction]:
    """批量执行全部五项风控检查，返回所有被触发的风险动作。

    检查顺序（按风险优先级）：
      1. check_trail -- 跟踪止损（保护浮动盈利）
      2. check_stop  -- 硬止损（限制最大亏损）
      3. check_take  -- 止盈（锁定利润）
      4. check_hold  -- 持仓时间（防止死扛）
      5. check_daily -- 日亏损（熔断保护）

    Args:
        slot: 策略槽对象
        current_price: 当前最新价格

    Returns:
        所有被触发的 RiskAction 列表（无触发时返回空列表）
    """
    checks = [
        check_trail(slot, current_price),
        check_stop(slot, current_price),
        check_take(slot, current_price),
        check_hold(slot),
        check_daily(slot),
    ]
    return [c for c in checks if c.should_exit]
