# -*- coding: utf-8 -*-
"""
风控循环模块 (risk/loop)
=========================

模块定位
--------
本模块是 nt-base 风控系统的调度核心，实现了一个**1 秒间隔的异步循环**，
持续检查所有活跃持仓的风控条件，一旦触发立即执行紧急平仓。

在风控体系中的角色
------------------
RiskLoop 连接了 checkers（纯函数判断）和 executor（实际下单平仓）：
  1. 每秒遍历所有活跃策略槽（StrategySlot）
  2. 获取每个槽对应的最新价格和 ATR
  3. 按优先级顺序执行五项风控检查
  4. 首个触发的检查触发 flat() 平仓，跳过后续检查
  5. 日亏损熔断会永久禁用该槽（slot.tripped = True）

与 tick_exit 的关系
-------------------
- RiskLoop（本模块）：1 秒精度的 Bar 级检查（硬止损/止盈/持仓时间/日亏损）
- TickExitManager（tick_exit.py）：Tick 级检查（ToxicFlow/跟踪止损/保本阶梯）
- 两者并行运行：RiskLoop 处理慢速风控，TickExitManager 处理快速风控

核心功能列表
------------
1. 每秒更新价格和 ATR 到策略槽
2. 做多时更新 highest_since_entry，做空时更新 lowest_since_entry
3. 执行日亏损熔断检查（最高优先级）
4. 检查是否有待处理的 Bar 级退出任务（来自 trading-v2 的 bar 层退出）
5. 执行跟踪止损 -> 硬止损 -> 止盈 -> 持仓时间检查（短路逻辑）
6. 60 秒一次的心跳日志

Author: nt-base system
Version: 1.2.0
"""
from __future__ import annotations
"""风控循环模块 -- 每秒遍历所有持仓执行平仓检查，含心跳日志和 ATR 更新。"""
import asyncio
import logging
from risk.checker import check_trail, check_stop, check_take, check_hold, check_daily

logger = logging.getLogger(__name__)


class RiskLoop:
    """风险管理循环 -- 1 秒间隔的异步风控调度器。

    职责：
      1. 维护最新价格表（_prices）和 ATR 表（_atrs），由外部（main.py）通过
         update_price / update_atr 接口推送数据
      2. 每秒从 StrategyRegistry 获取所有活跃持仓，逐一执行风控检查
      3. 检查触发时通过 OrderExecutor.flat() 执行平仓
      4. 处理 trading-v2 发来的 Bar 级退出请求（pending_bar_exit 重试机制）

    设计理念：
      - 无锁设计：所有状态（_prices, _atrs）由单一线程（asyncio 事件循环）访问
      - 非阻塞：使用 asyncio.sleep 而非 time.sleep，不阻塞事件循环
      - 可中断：通过 _running 标志和 task.cancel() 实现优雅关闭

    性能特征：
      - O(active_slots) 复杂度，典型场景 1-3 个活跃槽，开销可忽略
      - 每次迭代执行 ~5 次纯函数检查，无网络 IO

    Attributes:
        _registry: StrategyRegistry 实例，提供 get_active_slots() 获取活跃策略槽
        _executor: OrderExecutor 实例，提供 flat(slot, reason) 执行平仓
        _interval: 检查间隔（秒），默认 1.0
        _running:  运行状态标志，用于优雅停止
        _task:     asyncio.Task 对象，持有后台运行的 _run() 协程
        _prices:   dict[str, float]，符号 -> 最新价格缓存
        _atrs:     dict[str, float]，符号 -> 最新 ATR 缓存
    """

    def __init__(self, registry, executor, interval=1.0):
        """初始化 RiskLoop 实例。

        Args:
            registry: StrategyRegistry 对象
                      必须实现 get_active_slots() 方法返回活跃 StrategySlot 列表
            executor: OrderExecutor 对象
                      必须实现 flat(slot, reason) 方法执行平仓
            interval: 检查间隔（秒），默认 1.0 秒
                      增加间隔降低 CPU 开销但延迟风控响应
        """
        self._registry = registry
        self._executor = executor
        self._interval = interval
        self._running = False
        self._task = None
        self._prices: dict[str, float] = {}      # symbol -> latest price
        self._atrs: dict[str, float] = {}        # symbol -> latest ATR

    def update_price(self, symbol: str, price: float):
        """更新某个交易对的最新价格。

        由 main.py 在每次收到 tick 后调用。
        只有价格 > 0 时才有效，确保不会用无效值覆盖有效数据。

        Args:
            symbol: 交易对符号，如 "SOLUSDT"
            price:  最新价格
        """
        self._prices[symbol] = price

    def update_atr(self, symbol: str, atr: float):
        """更新某个交易对的最新 ATR 值。

        由 main.py 在每次 bar 计算后更新 ATR。
        ATR 用于跟踪止损的距离计算。

        Args:
            symbol: 交易对符号
            atr:    平均真实波幅值
        """
        self._atrs[symbol] = atr

    async def start(self):
        """启动风控循环。

        创建一个异步后台任务运行 _run() 协程。
        该方法非阻塞，立即返回。
        """
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        """优雅停止风控循环。

        设置 _running = False 让 _run() 在下一次循环退出，
        同时 cancel 异步任务以防阻塞。
        """
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        """主循环 -- 每秒执行一次风控检查。

        执行流程（每个 tick，每个活跃槽）：
          1. 从缓存中获取该槽对应交易对的最新价格
          2. 更新槽的 ATR 值和最高/最低价
          3. 60 秒一次心跳日志（记录价格、极值、ATR、持仓时间）
          4. 日亏损熔断检查（最高优先级）-> 触发则平仓并禁用槽
          5. Bar 级退出检查（trading-v2 请求）-> 重试直到平仓完成
          6. 跟踪止损 -> 硬止损 -> 止盈 -> 持仓时间检查（短路：触发即停）
          7. 休眠 interval 秒

        设计要点：
          - 优先检查日亏损（全局熔断），其次处理 Bar 级退出，
            最后执行本地风控检查
          - 平仓操作通过 self._executor.flat() 异步执行
          - 心跳日志间隔 60 秒，便于运维监控系统运行状态
        """
        while self._running:
            # 遍历所有活跃策略槽
            for slot in self._registry.get_active_slots():
                # ---- 第一步：获取该槽对应的最新价格 ----
                # 遍历槽的订阅列表，找到第一个有有效价格的交易对
                price = 0.0
                symbol = ""
                for sub in slot.subscriptions:
                    p = self._prices.get(sub.symbol, 0)
                    if p > 0:
                        price = p
                        symbol = sub.symbol
                        break
                # 如果所有订阅品种都没有有效价格，跳过该槽
                if price <= 0:
                    continue

                # ---- 第二步：更新 ATR ----
                # ATR = 0 表示尚未计算出来，使用 checker 内部的 fallback（0.15%）
                atr = self._atrs.get(symbol, 0.0)

                # ---- 第三步：60 秒心跳日志 ----
                # 使用实例属性 _hb_count 记录循环次数
                if not hasattr(self, '_hb_count'):
                    self._hb_count = 0
                self._hb_count += 1
                if self._hb_count % 60 == 1:
                    # 每 60 次迭代输出一次心跳，方便运维确认系统仍在运行
                    logger.info(f"RiskLoop heartbeat: price={price:.4f} "
                                f"high={slot.highest_since_entry:.4f} "
                                f"low={slot.lowest_since_entry:.4f} "
                                f"atr={slot.current_atr:.4f} "
                                f"held={slot.held_sec:.0f}s")
                if atr > 0:
                    # 只在 ATR 有效时更新到 slot，避免用 0 值覆盖已有数据
                    slot.current_atr = atr

                # ---- 第四步：更新跟踪止损的极值 ----
                # 做多时：记录入场以来的最高价，用于计算止损线
                # 做空时：记录入场以来的最低价，用于计算止损线
                # 检查 slot.has_position 是防御性操作
                if slot.has_position:
                    if slot.entry_side == "LONG" and price > slot.highest_since_entry:
                        # 做多时最高价刷新：止损线随之上移，锁定更多利润
                        slot.highest_since_entry = price
                    elif slot.entry_side == "SHORT" and price < slot.lowest_since_entry:
                        # 做空时最低价刷新：止损线下移，锁定更多利润
                        slot.lowest_since_entry = price

                # ---- 第五步：日亏损熔断检查（最高优先级） ----
                # 这个检查不依赖价格，仅看当日累计盈亏
                daily = check_daily(slot)
                if daily.should_exit:
                    # 日亏损熔断触发：设置 slot.tripped 永久禁用
                    slot.tripped = True
                    # 平仓并通知：reason 中包含熔断触发原因和阈值
                    self._executor.flat(slot, f"{daily.reason} | CB {slot.max_daily_loss_pct*100:.1f}% paused")
                    # 跳过该槽的其他检查，继续处理下一个槽
                    continue

                # ---- 第六步：Bar 级退出任务重试 ----
                # trading-v2 的 bar 层退出（如 BTC shock / CVD 反转 / 时间衰减）
                # 通过 gRPC 设置 slot.pending_bar_exit，然后 RiskLoop 负责执行
                # 由于下单是异步的（NautilusTrader 异步引擎），可能一次 flat
                # 没有立即完成，这里每秒重试直到持仓归零
                if slot.pending_bar_exit and slot.has_position:
                    # has_pending_close_for 检查是否已有正在执行的平仓订单
                    if not self._executor.has_pending_close_for(slot):
                        # 没有待处理的平仓订单，发送新的平仓指令
                        self._executor.flat(slot, slot.pending_bar_exit)
                    # 跳过其他风控检查，等待平仓完成
                    continue

                # ---- 第七步：四级风控检查（短路逻辑） ----
                # 按顺序执行：跟踪止损 -> 硬止损 -> 止盈 -> 持仓时间
                # 任何一个检查触发立即平仓，不再执行后续检查
                # 注意：跟踪止损放在最前面，因为它在盈利时也保持保护
                # 而硬止损只在亏损时触发
                for check in [check_trail, check_stop, check_take, check_hold]:
                    action = check(slot, price)
                    if action.should_exit:
                        # 触发平仓：executor.flat() 通过 NautilusTrader 发出市价单
                        self._executor.flat(slot, action.reason)
                        # 跳出内层 for 循环，不再执行后续检查
                        break

            # 休眠 interval 秒（默认 1 秒）
            # 使用 asyncio.sleep 而非 time.sleep，不阻塞事件循环
            await asyncio.sleep(self._interval)
