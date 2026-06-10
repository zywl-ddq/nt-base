"""
StrategyBot -- per-strategy Telegram Bot.

策略级别的Telegram Bot模块。每个策略实例拥有独立的Bot实例，
通过私有DM接收结构化命令。业务代码通过 notify(msg_type, **data)
发送事件通知，Bot层负责格式化输出，调用方无需接触任何排版逻辑。

模块导出：
  - StrategyBot: 核心Bot类，管理命令分发、通知发送、生命周期
  - register_handler: 装饰器，将处理函数注册到命令路由表
  - ALL_COMMANDS: 支持的所有命令列表
  - HELP_TEXT: 帮助信息模板（HTML格式）
"""
from __future__ import annotations

import asyncio, logging, html as _html
from typing import Callable
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from base.telegram.commands import parse_command, check_permission

logger = logging.getLogger("telegram.bot")

# 所有支持的命令名称列表，用于统一注册CommandHandler
ALL_COMMANDS = [
    "status", "flatme", "status_all", "flat_all", "flat",
    "pause", "resume", "adj", "pauseme", "resumeme", "ping",
]
# 帮助信息HTML模板，按功能分组：交易操组（操作者级别）、管理员操作
HELP_TEXT = (
    "\U0001f4cb <b>Available Commands</b>\n"
    "\n"
    "<b>Trade</b>\n"
    "  /status -- view strategy state\n"
    "  /flatme -- close position\n"
    "  /pauseme -- pause new entries\n"
    "  /resumeme -- resume entries\n"
    "  /adj key value -- adjust param\n"
    "\n"
    "<b>Admin Only</b>\n"
    "  /status_all -- all strategies\n"
    "  /flat id -- close specific\n"
    "  /flat_all -- close all\n"
    "  /pause id -- pause specific\n"
    "  /resume id -- resume specific\n"
)


class StrategyBot:
    """
    策略级别的Telegram Bot。

    职责：管理Telegram PTB（python-telegram-bot）Application实例的生命周期，
    包括命令注册、轮询启动/停止、消息分、通知格式化。
    每个策略实例拥有独立的 StrategyBot 实例，绑定各自的 operator_chat_id。

    与 Framework（base/notify.py）的关系：
      - StrategyBot 只负责消息分和原始发送
      - 消息格式化委托给 base/notify.py 中的 fmt_* 函数
      - 命令处理逻辑委托给 handlers.py（Path A）和 control.py（Path B）
    """
    def __init__(self, instance_id, bot_token, operator_chat_id, admin_chat_id,
                 executor=None, registry=None, grpc_servicer=None):
        """
        初始化Bot实例。

        参数:
          instance_id: 策略实例ID（如 AlphaV2-005），用于标识归属
          bot_token: Telegram Bot Token，从.env配置加载
          operator_chat_id: 策略操作者的Telegram Chat ID（L1权限）
          admin_chat_id: 全局管理员的Telegram Chat ID（L2权限）
          executor: OrderExecutor实例，用于执行平仓操（Path A）
          registry: StrategyRegistry实例，用于查询策略运行时状态
          grpc_servicer: gRPC服务端实例（TradingBaseServicer），用于推送控制指令（Path B）
        """
        self.instance_id = instance_id
        self.bot_token = bot_token
        self.operator_chat_id = operator_chat_id
        self.admin_chat_id = admin_chat_id
        self._executor = executor
        self._registry = registry
        self._grpc = grpc_servicer
        self._app = None

    # -- 生命周期管理 --

    async def start(self):
        """
        启动Bot轮询。

        流程：
          1. 构建 PTB Application 实例（注入 Bot Token）
          2. 遍历 ALL_COMMANDS，为每个命令注册 CommandHandler，统一路由到 _dispatch
          3. 额外注册 /help 和 /start 命令，路由到 _help
          4. initialize -> start -> start_polling（drop_pending_updates=True 丢弃启动前的残留消息）
        """
        self._app = Application.builder().token(self.bot_token).build()
        for cmd in ALL_COMMANDS:
            self._app.add_handler(CommandHandler(cmd, self._dispatch))
        self._app.add_handler(CommandHandler("help", self._help))
        self._app.add_handler(CommandHandler("start", self._help))
        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        logger.info(f"Bot started: {self.instance_id} operator={self.operator_chat_id}")

    async def stop(self):
        """
        停止Bot轮询并释放资源。

        顺序：updater.stop -> app.stop -> app.shutdown
        与 start 完全对应的逆操作。
        """
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            logger.info(f"Bot stopped: {self.instance_id}")

    # -- 事件通知（调用方传入 msg_type + data，Bot负责渲染） --

    async def notify(self, msg_type: str, **data):
        """
        接收事件通知并发送格式化消息。

        调用方传入事件类型和参数字典，Bot内部通过 renderers 映射表
        选择对应的 fmt_* 函数进行格式化，然后调用 send 发送。
        调用方不需要也不应该接触格式化逻辑。

        msg_type 支持：
          - entry: 开仓通知
          - close: 平仓通知（正常退出）
          - risk_exit: 风控强制平仓通知
          - daily_trip: 日亏损超限报警
          - reverse: 多空反转通知
          - orphan: 挂单通知（持仓异常）
          - strategy_start: 策略启动通知
        """
        from base.notify import (
            fmt_entry, fmt_close, fmt_risk_exit, fmt_daily_trip,
            fmt_reverse, fmt_orphan_alert, fmt_strategy_start,
        )
        # 消息类型到渲染函数的映射表
        renderers = {
            "entry": lambda d: fmt_entry(d["sid"], d["symbol"], d["side"],
                d["price"], d["qty"], d["notional"], d["reason"]),
            "close": lambda d: fmt_close(d["sid"], d["symbol"], d["side"],
                d["entry_px"], d["exit_px"], d["pnl"], d["held_sec"], d["reason"]),
            "risk_exit": lambda d: fmt_risk_exit(d["sid"], d["symbol"], d["side"],
                d["price"], d["reason"], d.get("pnl", 0.0)),
            "daily_trip": lambda d: fmt_daily_trip(d["sid"], d["symbol"],
                d["daily_pnl"], d["loss_limit"]),
            "reverse": lambda d: fmt_reverse(d["sid"], d["symbol"],
                d["from_side"], d["to_side"], d["price"], d["reason"]),
            "orphan": lambda d: fmt_orphan_alert(d["sid"], d["symbol"],
                d["side"], d["entry_px"], d["held_sec"]),
            "strategy_start": lambda d: fmt_strategy_start(d["sid"], d["symbol"],
                d["leverage"], d["pos_pct"]),
        }
        fn = renderers.get(msg_type)
        if fn:
            await self.send(fn(data))

    # -- 命令分发 --

    async def _help(self, update, context):
        """回复帮助信息（HTML格式）。"""
        await update.message.reply_text(HELP_TEXT, parse_mode="HTML")

    async def _dispatch(self, update, context):
        """
        命令分发核心逻辑。

        流程：
          1. 提取 chat_id，解析命令文本 -> (cmd_name, args)
          2. 检查权限级别（check_permission）：0=无权限，1=操者，2=管理员
          3. 根据 cmd_name 从 _HANDLERS 路由表查找对应的处理函数
          4. 权限不足或命令未知时直接回复错误信息
          5. 调用处理函数，捕获异常，回复结果

        _HANDLERS 路由表由 register_handler 装饰器填充，
        命令处理函数分布在 handlers.py（Path A）和 control.py（Path B）中。
        """
        chat_id = str(update.effective_chat.id)
        cmd_text = update.message.text.strip()
        cmd_name, args = parse_command(cmd_text)
        level = check_permission(chat_id, self.operator_chat_id, self.admin_chat_id)

        handler = _HANDLERS.get(cmd_name)
        if handler is None:
            await update.message.reply_text(
                f"❓ Unknown: /{cmd_name}\nSend /help for commands",
                parse_mode="HTML")
            return
        if level == 0:
            await update.message.reply_text(
                "\U0001f6ab No permission.", parse_mode="HTML")
            return
        try:
            reply = await handler(self, args, level, chat_id)
        except Exception as e:
            logger.error(f"Handler {cmd_name} error: {e}", exc_info=True)
            reply = f"❌ Error: {_html.escape(str(e))}"
        await update.message.reply_text(reply, parse_mode="HTML")

    # -- 原始消息发送（供 notify 内部使用） --

    async def send(self, text: str):
        """
        发送原始文本消息到 operator_chat_id。

        HTML解析模式，禁用网页预览。
        仅当 Bot 已启动（self._app 不为 None）时才发送。
        发送失败时记录警告日志，不中断调用方流程。
        """
        if self._app:
            try:
                await self._app.bot.send_message(
                    chat_id=self.operator_chat_id, text=text,
                    parse_mode="HTML", disable_web_page_preview=True)
            except Exception as e:
                logger.warning(f"Bot {self.instance_id} send failed: {e}")


# -- 处理函数注册表 --

_HANDLERS: dict[str, Callable] = {}

def register_handler(cmd_name: str):
    """
    装饰器：将命令处理函数注册到全局路由表。

    用法:
        @register_handler("flatme")
        async def handle_flatme(bot, args, level, chat_id):
            ...

    注册的处理函数签名：(bot: StrategyBot, args: dict, level: int, chat_id: str) -> str
    返回值是HTML格式的回复文本。
    """
    def decorator(fn):
        _HANDLERS[cmd_name] = fn
        return fn
    return decorator
