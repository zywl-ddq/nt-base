# -*- coding: utf-8 -*-
"""tests/test_tick_exit.py -- TickExitManager 保本激活阈值测试。

验证 P1 修复:breakeven_atr_mult 默认值从 1.5 放宽到 3.0。

背景(走查 + 逐笔回放发现):
  TickExitManager 在 nt-base 进程对每个 SOL tick 做 tick 级退出检查,
  创建时无参(main.py),使用写死的默认 breakeven_atr_mult=1.5。
  SOL 浮盈仅约 1.5*ATR(约 0.07%)即激活保本,止损锁到 entry*(1+0.1%),
  价格稍有反向即平 —— 这是"持仓中位 4.7 分钟、3% 胜率"的直接原因之一
  (回放显示大量 TickTrail 的 stop ≈ entry*1.001,即保本价)。

  放宽到 3.0*ATR 给趋势更多呼吸空间,减少保本锁定扫损。
"""
from risk.tick_exit import TickExitManager


class TestBreakevenThreshold:
    """breakeven 激活阈值应从 1.5*ATR 放宽到 3.0*ATR。"""

    def test_default_breakeven_mult_is_3(self):
        """放宽后 TickExitManager 默认 breakeven_atr_mult 应为 3.0(原 1.5)。"""
        tem = TickExitManager()
        assert tem.breakeven_atr_mult == 3.0

    def test_breakeven_not_activated_below_3_atr(self):
        """浮盈 2.5*ATR(< 3.0*ATR)时不应激活保本。"""
        tem = TickExitManager()
        tem.open_position(100.0, True, "SOL")
        tem.update_atr(1.0)
        # 价格 102.5 -> 浮盈 2.5 < 3.0*ATR=3.0 -> 不应激活
        tem.on_tick(102.5, 1.0, True, 1_000_000_000, "SOL")
        assert tem._breakeven_activated is False

    def test_breakeven_activated_at_or_above_3_atr(self):
        """浮盈 3.5*ATR(>= 3.0*ATR)时应激活保本(确认放宽后仍能激活)。"""
        tem = TickExitManager()
        tem.open_position(100.0, True, "SOL")
        tem.update_atr(1.0)
        tem.on_tick(103.5, 1.0, True, 1_000_000_000, "SOL")
        assert tem._breakeven_activated is True


class TestClosePositionContract:
    """P3 依赖契约:close_position 后 on_tick 必须返回 None。

    main.py 的 tick 退出修复(不再 del 已 close 的 TickExitManager)依赖此契约 ——
    保留的 tem 因 in_position=False 不会在下个 tick 重复触发 flat。
    """

    def test_on_tick_returns_none_after_close_position(self):
        tem = TickExitManager()
        tem.open_position(100.0, True, "SOL")
        tem.update_atr(1.0)
        tem.close_position()
        # close 后即使价格远低于 trailing 线,on_tick 也不应返回退出动作
        result = tem.on_tick(50.0, 1.0, True, 1_000_000_000, "SOL")
        assert result is None
