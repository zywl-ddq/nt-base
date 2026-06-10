# -*- coding: utf-8 -*-
"""
risk 包 -- 风险控制模块
=========================

本包提供 1 秒级风控循环，用于监控所有活跃策略槽位的风险状态。

模块清单
--------
loop        RiskLoop：1 秒间隔的风控主循环，遍历所有活跃策略槽位，
            调用 checker 进行检查，触发退出时通知 OrderExecutor 平仓。
checker     风控检查函数集合：
            - check_stop：   止损检查（价格向不利方向移动超过 stop_pct）
            - check_take：   止盈检查（价格向有利方向移动超过 take_pct）
            - check_hold：   持仓时间检查（超时强制退出）
            - check_daily：  日亏损检查（当日亏损超过 max_daily_loss_pct）
            - check_all：    一次性执行全部检查

检查流程
--------
RiskLoop (1s) -> 遍历 slots -> check_all(slot, current_price)
-> 若有退出条件触发 -> OrderExecutor.flat() 平仓
-> 记录退出原因到 slot（用于后续分析和日志）

作者: nt-base system
版本: 2.0.0
"""
