# -*- coding: utf-8 -*-
"""
base 包 -- nt-base 核心业务模块
==================================

本包包含交易系统的核心逻辑组件：

模块清单
--------
data_manage     DataManageActor：行情订阅 + 入库持久化
trading_node    TradingNode 工厂：Binance 数据源 + Sandbox 执行引擎
registry        StrategyRegistry：策略槽管理 + 因子索引
slot            StrategySlot：单个策略的运行状态（持仓、入场价、风控参数）
registration    RegistrationManager：基于 DB 表 strategy_instances 的动态热注册
executor        OrderExecutor：下单执行、平仓、动态仓位计算
v2_signal       AlphaSignal v3：信号生成 + 四层退出逻辑
v2_adapter      V2SignalAdapter：协议适配层，桥接新旧信号格式
signal_protocol SignalStrategy 协议定义（抽象接口）
notify          Telegram 通知封装

数据流概览
----------
DataManageActor 接收行情 -> bar dispatch -> 因子计算 -> gRPC 推送
-> trading-v2 策略计算信号 -> gRPC SubmitSignal -> OrderExecutor 下单
-> risk/ checker 风控检查 -> 退出触发

作者: nt-base system
版本: 2.0.0
"""
