"""
OrderExecutor v2 -- 动态仓位下单执行模块。

【在整个交易系统中的角色】
OrderExecutor 是 nt-base 交易引擎的下单执行核心，负责将策略信号（StrategySignal）
转化为实际的交易所订单并管理整个订单生命周期。具体职责包括：

1. 入场执行（_open）：根据策略信号的方向、仓位大小、置信度，创建市场订单入场
2. 加仓执行（Pyramid）：允许在已有持仓方向上继续加仓，但有仓位上限控制（2x base）
3. 平仓执行（flat）：根据策略退出信号平掉当前持仓
4. 仓位管理（validate）：执行前检查冷却时间、熔断状态、反向拦截等
5. 订单确认（on_fill）：通过 NautilusTrader 的 fill 回调获取实际成交价，
   计算 VWAP，更新 slot 状态，发送 Telegram 通知
6. 部分成交处理（accept_partial_fill）：IOC 订单未完全成交时的兜底逻辑
7. 超时/关闭兜底（flush_pending / cleanup_pending）：订单未成交时的通知保障

【v2 版本新特性】
- 动态仓位（Dynamic Position Sizing）：根据趋势置信度（confidence）调整仓位大小。
  弱趋势时缩仓（floor=0.25），强趋势时满仓（scale=floor+slope*confidence）
- 置信度调整：_adjusted_size() 方法提供线性缩放公式，对 slot.confidence 敏感性可调
- Pyramid 加仓：允许在已有持仓方向上加仓，但总仓位不超过 2x base 大小
- Pyramid 裁剪：当请求仓位超过上限时，自动裁剪到可用剩余仓位，低于 10% min 则拒绝

【通知机制（deferred）】
v2 引入延迟通知机制：订单提交时不立即发送 Telegram 通知，而是将通知上下文
存储在 self._pending 字典中，等到 on_fill() 回调拿到实际成交价后再发送。
这样可以确保通知中的价格是实际 VWAP 而非预估价，大幅提升通知准确性。
"""
import asyncio
import time
import logging
from base.slot import StrategySlot
from base.signal_protocol import StrategySignal
from base.notify import send_message, fmt_entry, fmt_close

logger = logging.getLogger(__name__)


def _notify(slot: StrategySlot, text: str):
    """通过 Telegram 发送通知消息的辅助函数。

    Telegram 通知是异步的（asyncio.ensure_future），不会阻塞下单流程。
    本函数会先打印调试日志（token 存在性、长度），然后通过 slot 中存储的
    bot_token 和 chat_id 发送消息。

    Args:
        slot: 策略运行时状态，包含 Telegram 配置信息
        text: 要发送的消息文本（由 fmt_entry/fmt_close 格式化）
    """
    _lg = logging.getLogger(__name__)
    _lg.info(f"_notify: tok={bool(slot.telegram_bot_token)} chat={bool(slot.telegram_chat_id)} tok_len={len(slot.telegram_bot_token) if slot.telegram_bot_token else 0}")
    if slot.telegram_bot_token and slot.telegram_chat_id:
        _lg.info(f"_notify: SENDING to chat {slot.telegram_chat_id}")
        asyncio.ensure_future(
            send_message(slot.telegram_bot_token, slot.telegram_chat_id, text)
        )


class OrderExecutor:
    """订单执行器 —— 负责所有下单操作（入场、加仓、平仓）及订单生命周期管理。

    通过依赖注入获得 NautilusTrader 的核心对象（venue, portfolio, cache 等），
    自身不持有交易引擎引用，降低耦合度。

    核心属性：
        _sol_id (InstrumentId): SOLUSDT 的交易对标识，用于查询持仓、创建订单
        _venue (Venue): 交易所标识（Binance Futures Testnet）
        _portfolio (Portfolio): NautilusTrader 投资组合，用于查询账户权益
        _submit_order (Callable): 提交订单的回调函数（由 BaseStrategy 注入）
        _cache (Cache): NautilusTrader 缓存，用于查询持仓、行情、交易对信息
        _order_factory (OrderFactory): 订单工厂（可选），用于创建标准市场订单
        _pending (dict): 待确认订单上下文字典，key=client_order_id, value=通知上下文
    """

    def __init__(self, sol_id, venue, portfolio, submit_order, cache, order_factory=None, cancel_order=None, clock=None):
        """初始化 OrderExecutor。

        所有依赖均由外部注入（依赖倒置原则），便于单元测试和替换实现。

        Args:
            sol_id (InstrumentId): SOLUSDT 交易对标识，用于创建订单和查询状态。
                格式如 Binance:XXX-USDT 的 InstrumentId 对象。
            venue (Venue): 交易所标识，用于查询账户信息和创建 Venue 相关的操作，
                连接的是 Binance USDT Futures testnet 沙盘环境。
            portfolio (Portfolio): NautilusTrader 的投资组合对象。
                通过 portfolio.account(venue) 获取账户信息（权益、余额等），
                用于计算仓位大小（notional = equity * size_pct * leverage）。
            submit_order (Callable): 提交订单到交易引擎的回调函数。
                由 BaseStrategy 的 submit_order 方法注入，实际调用
                NautilusTrader 的 TradingStrategy.submit_order()。
                签名: submit_order(order: Order) -> None
            cache (Cache): NautilusTrader 的缓存对象。
                提供以下关键查询方法：
                - cache.positions_open()：获取当前持仓
                - cache.instrument()：获取交易对规格（精度、最小数量等）
                - cache.trade_tick()：获取最新成交价
            order_factory (OrderFactory, optional): NautilusTrader 的订单工厂。
                如果提供了 factory，使用 factory.market() 创建标准市场订单；
                否则使用 instrument.create_order() 的方法创建。
                优先使用 order_factory 是因为它创建的订单更规范（自带 InstrumentId 类型校验）。
        """
        self._sol_id = sol_id
        self._venue = venue
        self._portfolio = portfolio
        self._submit_order = submit_order
        self._cache = cache
        self._order_factory = order_factory
        self._cancel_order = cancel_order  # [maker] cancel callback (main injects Strategy.cancel_order)
        self._clock = clock                # [maker] clock (reserved, timeout uses asyncio)
        # _pending 字典：存储已提交但尚未收到 fill 确认的订单上下文。
        # key = client_order_id (字符串)，value = 包含通知所需全部信息的字典。
        # 当 on_fill() 回调返回时，使用真实成交价 VWAP 发送 Telegram 通知。
        # 这是 deferred（延迟）通知机制的核心数据结构。
        self._pending: dict[str, dict] = {}  # client_order_id -> notification context
        # cid -> instance_id 长生命周期映射（不随 _pending 删除），
        # 供 data_manage 落库时按 client_order_id 反查策略实例。
        self._cid_instance: dict[str, str] = {}

    def execute(self, slot: StrategySlot, signal: StrategySignal,
                current_price: float) -> str:
        """执行策略信号 —— 入场/加仓的核心决策逻辑。

        这是整个交易系统的关键入口方法。从 trading-v2 通过 gRPC 提交的信号最终
        会调用到此方法。它按照以下严格顺序进行决策：

        1. 熔断检查（trip）：如果 slot 处于熔断状态，直接拒绝所有信号
        2. 冷却检查（cooldown）：从上一次交易算起未超过冷却时间，拒绝
        3. 方向判定 + 持仓检查：
           a. 已有持仓且同向 → Pyramid 加仓逻辑（验证仓位上限、裁剪）
           b. 已有持仓且反向 → 拒绝（反转拦截，必须先平仓再入场）
           c. 无持仓 → 正常入场
        4. Pyramid 加仓具体流程：
           a. 获取账户权益（equity）
           b. 计算当前名义价值（current_notional = 持仓量 * 当前价格）
           c. 计算最大允许名义价值（max_notional = equity * 2x base_pct * leverage）
           d. 计算请求名义价值（req_notional = equity * req_pct * leverage）
           e. 如果 new_total = current + req > max → 裁剪
           f. 裁剪后可用仓位 < 10% min → 拒绝
           g. 调用 _open() 执行加仓
        5. 无持仓时直接调用 _open() 入场

        Args:
            slot: 策略运行时状态，包含冷却时间、熔断状态、置信度等
            signal: 策略信号，包含方向（direction）、仓位比例（position_size_pct）、原因（reason）
            current_price: 当前市场价，用于计算名义价值和创建订单

        Returns:
            str: 执行结果描述，用于日志记录。可能值：
                - "rejected: tripped" — 熔断中
                - "rejected: cooldown" — 冷却中
                - "rejected: position limit reached" — 仓位上限（裁剪后仍不足）
                - "rejected: reversal blocked (close first)" — 反向被拦截
                - "pyramid X.XXX" — 加仓成功，X.XXX 为实际仓位比例
                - "entry LONG/SHORT" — 入场成功
        """
        # 第1步：熔断检查（trip）。
        # trip 由风控系统（risk/checker.py）或 TickExitManager 设置，
        # 一旦触发，该策略在 trip_duration 内不再接受任何新信号。
        if slot.tripped:
            return "rejected: tripped"

        # 第2步：冷却检查（cooldown）。
        # 从上次交易（last_trade_time）起必须经过至少 cooldown_sec 秒才能再次入场。
        # 防止高频连续下单（过度交易），也防止因信号抖动导致短时间内反复入场/平仓。
        if time.time() - slot.last_trade_time < slot.cooldown_sec:
            return "rejected: cooldown"

        # 判定信号方向：正数为做多（LONG），负数为做空（SHORT）
        target_long = signal.direction > 0
        # 查询当前是否有持仓
        pos = self._get_position()

        if pos is not None:
            # ── 有持仓 → 拒绝所有新入场（max_concurrent = 1）──
            # 策略必须先发送 direction=0（close 信号）平仓，
            # 等 flat() 完成后才能在后续 bar 中重新入场。
            # 此限制替代了原来的 Pyramid 加仓和反向拦截逻辑，
            # 确保任何时候最多只有 1 个净持仓。
            return "rejected: position exists (max_concurrent=1)"

        else:
            # ── 无持仓 → 正常入场 ──
            from nautilus_trader.model.enums import OrderSide
            # 如果信号指定了仓位比例，使用它；否则使用 slot 默认值
            size_pct = signal.position_size_pct if signal.position_size_pct > 0 else None
            self._open(OrderSide.BUY if target_long else OrderSide.SELL,
                       current_price, slot, signal.reason, size_pct_override=size_pct)
            return f"entry {slot.entry_side}"

    def _create_market_order(self, instrument_id, order_side, quantity, time_in_force):
        """创建市价单（Market Order）。

        提供两种创建方式，优先使用 OrderFactory（更规范），回退到 instrument.create_order()。
        创建的订单使用 IOC（Immediate-Or-Cancel）时间策略，确保不会作为挂单留在订单簿上。

        Args:
            instrument_id: 交易对标识（InstrumentId），如 Binance:SOLUSDT
            order_side: 订单方向（OrderSide.BUY 或 OrderSide.SELL）
            quantity: 订单数量（Decimal 类型，精度由 instrument 规格决定）
            time_in_force: 订单有效期策略（IOC = Immediate-Or-Cancel）

        Returns:
            Order: NautilusTrader 订单对象，可提交到交易引擎

        Note:
            NautilusTrader 的 OrderFactory.market() 会自动处理 instrument_id 的类型校验，
            而 instrument.create_order() 是新版 API 的创建方式，两者功能等价。
        """
        if self._order_factory is not None:
            return self._order_factory.market(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
            )
        return self._cache.instrument(instrument_id).create_order(
            order_side=order_side,
            quantity=quantity,
            time_in_force=time_in_force,
            post_only=False,
            reduce_only=False,
            quote_quantity=False,
        )

    def _create_limit_order(self, instrument_id, order_side, quantity, price):
        """[maker] 创建 GTC 限价单（被动挂单，享 maker fee）。"""
        from nautilus_trader.model.enums import TimeInForce
        if hasattr(price, 'as_decimal'):
            px = price  # 已是 Price 对象
        else:
            # 量化到 tick size，消除浮点尾差（修复 DENIED: price precision > 4）
            px = self._cache.instrument(instrument_id).make_price(price)
        if self._order_factory is not None:
            return self._order_factory.limit(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
                price=px,
                time_in_force=TimeInForce.GTC,
                post_only=True,
            )
        return self._cache.instrument(instrument_id).create_order(
            order_side=order_side,
            quantity=quantity,
            price=px,
            time_in_force=TimeInForce.GTC,
            post_only=True,
            reduce_only=False,
            quote_quantity=False,
        )

    def _passive_price(self, order_side, ref_price):
        """[maker] 被动挂单价：买挂 best_bid、卖挂 best_ask；取不到则 ref∓1tick。"""
        tick = 0.01  # SOL price-based tick fallback
        try:
            book = self._cache.book(self._sol_id) if hasattr(self._cache, 'book') else None
            if book is not None:
                is_buy = (order_side.name == 'BUY')
                if is_buy and hasattr(book, 'best_bid_price'):
                    bp = book.best_bid_price
                    return float(bp.as_decimal()) if bp else ref_price - tick
                if (not is_buy) and hasattr(book, 'best_ask_price'):
                    ap = book.best_ask_price
                    return float(ap.as_decimal()) if ap else ref_price + tick
        except Exception as e:
            logger.warning(f"[maker] book lookup failed, use ref∓tick: {e}")
        # fallback: 买挂低 1tick、卖挂高 1tick
        return ref_price - tick if order_side.name == 'BUY' else ref_price + tick

    async def _maker_timeout(self, cid, delay=5.0):
        """[maker] 超时检查：未完全成交则撤单转 taker 兜底。"""
        import asyncio
        try:
            await asyncio.sleep(delay)
        except Exception:
            return
        p = self._pending.get(cid)
        if p is None or not p.get('is_maker') or p.get('fallback_done'):
            return
        if p.get('accum_qty', 0.0) >= p['expected_qty'] * 0.99:
            return  # 已完全成交
        p['fallback_done'] = True
        # 撤 maker 单
        try:
            from nautilus_trader.model.identifiers import ClientOrderId
            coid = ClientOrderId(cid)
            order = self._cache.order(coid)
            if order is not None and self._cancel_order is not None:
                self._cancel_order(order)
                logger.info(f"[maker] {cid} timeout, canceled -> market fallback")
        except Exception as e:
            logger.warning(f"[maker] cancel {cid} failed: {e}")
        # market 兜底（全 qty；delta_momentum 0.15 信号稀疏，部分成交重复风险可接受）
        try:
            from decimal import Decimal
            from nautilus_trader.model.enums import TimeInForce
            instr = self._cache.instrument(self._sol_id)
            side_obj = p['side_order_obj']
            qty_obj = instr.make_qty(Decimal(str(p['expected_qty'])))
            fb = self._create_market_order(self._sol_id, side_obj, qty_obj, TimeInForce.IOC)
            self._submit_order(fb)
            fb_cid = str(fb.client_order_id)
            fp = dict(p)
            fp['is_maker'] = False
            fp['fallback_done'] = True
            fp['accum_qty'] = 0.0
            fp['accum_notional'] = 0.0
            fp['total_commission'] = 0.0
            fp['created_at'] = time.time()
            self._pending[fb_cid] = fp
            # fallback 单继承原 maker 单的策略实例归属
            self._cid_instance[fb_cid] = p['slot'].strategy_id
        except Exception as e:
            logger.error(f"[maker] fallback submit {cid} failed: {e}")

    def _get_position(self):
        """查询当前 SOLUSDT 的持仓信息。

        遍历 NautilusTrader cache 中的所有未平仓头寸，返回 SOL 的活跃持仓。

        Returns:
            Position 或 None: 如果存在持仓量 > 0 的头寸，返回该头寸对象；
            否则返回 None（表示当前无持仓）。

        Note:
            position.quantity.as_decimal() 返回 Decimal 类型，
            转换为 float 后判断是否 > 0。理论上不会出现 0 或负数的持仓，
            但 > 0 的检查是防御性编程，防止零持仓误认为有持仓。
        """
        for p in self._cache.positions_open(instrument_id=self._sol_id):
            if float(p.quantity.as_decimal()) > 0:
                return p
        return None

    def _adjusted_size(self, slot: StrategySlot) -> float:
        """根据趋势置信度调整仓位大小 —— v2 动态仓位核心。

        【置信度缩放公式】
        scale = floor + slope * confidence
        adjusted_size = position_size_pct * scale

        其中：
        - floor = 0.25：最低仓位比例（即使置信度为 0 也保留 25% 基础仓位）
        - slope = 0.75：斜率，控制置信度对仓位的影响程度
        - confidence = slot.confidence，由趋势状态因子（trend_regime）提供，
          范围通常在 [0, 1] 之间

        【含义】
        - confidence = 0.0（极弱趋势）：scale = 0.25，仓位缩小到 25%，防御性缩仓
        - confidence = 0.5（中等趋势）：scale = 0.625，仓位适中
        - confidence = 1.0（强趋势）：scale = 1.0，满仓进攻

        【设计目的】
        在弱趋势/震荡行情中主动缩减风险敞口，在强趋势行情中充分利用趋势。
        与风控系统的自适应逻辑（weak_trend → 缩仓+缩时）形成闭环。

        Args:
            slot: 策略运行时状态，使用其 confidence 属性

        Returns:
            float: 调整后的仓位比例（小数），作为 position_size_pct 的缩放结果
        """
        conf = getattr(slot, 'confidence', 0.0)
        floor = 0.25
        slope = 0.75
        scale = floor + slope * conf
        return slot.position_size_pct * scale


    # -- Position sizing validation --

    def _max_position_notional(self, slot) -> float:
        """计算该策略的最大允许名义价值（仓位上限）。

        上限规则：最大名义价值 = equity * (2 * position_size_pct) * leverage
        即总仓位不超过基础仓位大小的 2 倍。

        例如：equity=1000, position_size_pct=0.2, leverage=3
            base_notional = 1000 * 0.2 * 3 = 600
            max_notional = 1000 * 0.4 * 3 = 1200

        这意味着 Pyramid 最多加仓到 2x base 大小，防止过度集中。

        Args:
            slot: 策略运行时状态，使用其 position_size_pct 属性和 leverage

        Returns:
            float: 最大允许名义价值（美元计价）
        """
        from decimal import Decimal
        account = self._portfolio.account(self._venue)
        equity = float(account.balance_total().as_decimal())
        max_pct = self._adjusted_size(slot) * 2.0
        return equity * max_pct * slot.leverage

    def _current_position_notional(self, price: float = None) -> float:
        """计算当前持仓的名义价值（以最新市价计价）。

        名义价值 = 持仓量 * 当前价格，用于计算已占用仓位额度，
        以及判断 Pyramid 加仓是否超过上限。

        Args:
            price: 当前价格（可选）。如果未提供，从 cache.trade_tick() 获取最新成交价。

        Returns:
            float: 当前持仓的名义价值。无持仓时返回 0.0。
        """
        pos = self._get_position()
        if pos is None:
            return 0.0
        if price is None:
            trade = self._cache.trade_tick(self._sol_id)
            price = float(trade.price) if trade else 0.0
        qty = float(pos.quantity.as_decimal())
        return qty * price

    def _open(self, side, price, slot, reason, size_pct_override=None):
        """开仓/加仓执行 —— 创建市价单并管理通知上下文。

        这是实际下单的操作方法，负责：
        1. 根据仓位比例（可覆盖）和置信度调整计算实际开仓数量
        2. 使用 _create_market_order 创建 IOC 市价单
        3. 通过 _submit_order 提交订单到交易引擎
        4. 更新 slot.last_trade_time（用于冷却检查）
        5. 将通知上下文存入 _pending 字典（deferred 通知机制）

        【数量计算逻辑】
        1. 如果有 size_pct_override（策略已按置信度调整的比例），直接使用
        2. 否则使用 _adjusted_size()（按 slot 默认值 + 置信度缩放）
        3. notional = equity * adj_size_pct * leverage
        4. qty = instrument.make_qty(notional / price) —— 自动按交易对精度取整

        【延迟通知机制（deferred notification）】
        订单提交时不立即发送 Telegram 通知，而是将交易上下文（方向、预估价、
        预期数量等）暂存在 self._pending 字典中。当 on_fill() 回调携真实成交价
        到来时，才计算 VWAP 并发送通知。这样可以确保通知中的价格是实际成交价
        而非预估价，大幅提升通知准确性。

        Args:
            side (OrderSide): 订单方向（BUY = 做多 / SELL = 做空）
            price (float): 当前市价，用于计算名义价值和数量
            slot (StrategySlot): 策略运行时状态，用于读取和更新
            reason (str): 入场原因描述（来自策略信号）
            size_pct_override (float, optional): 强制指定的仓位比例。
                当 execute() 中的 Pyramid 裁剪后传入裁剪比例时使用；
                为 None 时使用 slot 默认值。
        """
        from nautilus_trader.model.enums import TimeInForce
        from decimal import Decimal

        # 获取交易对规格（用于精度处理）
        instr = self._cache.instrument(self._sol_id)
        # 获取账户权益
        account = self._portfolio.account(self._venue)
        equity = float(account.balance_total().as_decimal())

        # ── 仓位比例确定 ──
        # 优先使用 size_pct_override（由 execute 的 Pyramid 逻辑提供），
        # 否则使用 slot 默认值并经过置信度缩放
        if size_pct_override is not None:
            # size_pct_override is already confidence-adjusted by the strategy
            # (alpha_signal_v3._adjusted_position_pct), use directly
            adj_size_pct = size_pct_override
        else:
            # Fallback: use slot default with executor-level confidence scaling
            adj_size_pct = self._adjusted_size(slot)

        # 计算名义价值和数量
        notional = equity * adj_size_pct * slot.leverage
        # make_qty 自动按交易对的最小交易量精度取整
        qty = instr.make_qty(Decimal(str(notional / float(price))))

        # [maker] 创建并提交 GTC 限价单（挂被动价），超时撤单转 taker 兜底
        passive_px = self._passive_price(side, price)
        order = self._create_limit_order(
            instrument_id=self._sol_id, order_side=side,
            quantity=qty, price=passive_px,
        )
        self._submit_order(order)
        # 更新上次交易时间戳（用于冷却检查）
        slot.last_trade_time = time.time()

        # ── 存储延迟通知上下文 ──
        # side_str = "LONG" 或 "SHORT"（人类可读的方向描述）
        side_str = "LONG" if side.name == "BUY" else "SHORT"
        cid = str(order.client_order_id)
        # 存入 _pending，等 on_fill 回调后使用真实 VWAP 发送通知
        self._pending[cid] = {
            "type": "entry",              # 通知类型：入场
            "is_maker": True,             # [maker] 限价单标记
            "side_order_obj": side,       # [maker] OrderSide 对象（兜底用）
            "slot": slot,                 # 关联的策略 slot
            "side": side_str,             # 交易方向
            "reason": reason,             # 入场原因
            "estimated_price": price,     # 预估价（仅退场时使用）
            "notional": notional,         # 名义价值
            "expected_qty": float(qty),   # 预期成交数量
            "_open_passive_px": passive_px,  # [maker] 调试用
            "accum_qty": 0.0,             # 累计成交数量（填充用）
            "accum_notional": 0.0,        # 累计成交名义价值（用于计算 VWAP）
            "total_commission": 0.0,      # 累计手续费
            "created_at": time.time(),    # 创建时间（用于超时判断）
        }
        # 记录 cid->instance 映射，供 data_manage 落库区分策略
        self._cid_instance[cid] = slot.strategy_id
        # [maker] 调度超时检查
        import asyncio
        try:
            asyncio.get_event_loop().create_task(self._maker_timeout(cid, 5.0))
        except Exception as _e:
            logger.warning(f"[maker] schedule timeout failed: {_e}")

    def has_pending_close_for(self, slot) -> bool:
        """检查该策略是否已有待确认的平仓订单。

        这是重复平仓保护的辅助方法。由于 flat() 可能因为信号延迟或重复调用
        而被触发多次，此方法确保同一策略不会提交多个平仓订单。

        具体场景：如果 tick 级别的退出（如 ToxicFlow）和 bar 级别的退出
        （如 BTC shock）在同一时刻都被触发，两个路径可能都调用 flat()。
        has_pending_close_for 检查后仅第一个 flat() 会实际下单。

        Args:
            slot: 要检查的策略 slot

        Returns:
            bool: True 表示该 slot 已有待确认的平仓订单，不应再次平仓
        """
        for p in self._pending.values():
            if p.get("slot") is slot and p.get("type") == "close":
                return True
        return False

    def instance_for_cid(self, cid: str) -> str | None:
        """client_order_id -> 策略实例ID（如 AlphaV2-005），供 data_manage 落库区分策略。"""
        return self._cid_instance.get(cid)

    def flat(self, slot, reason="", price=None):
        """平仓执行 —— 平掉策略的当前持仓。

        【执行流程】
        1. 检查是否有持仓（无持仓直接返回 False）
        2. 确定平仓方向：做多则卖（SELL），做空则买（BUY）
        3. 重复平仓保护：检查 has_pending_close_for，已有平仓订单则跳过
        4. 获取退出价格（默认取最新成交价）
        5. 创建 IOC 市价单并提交（数量等于当前全部持仓量）
        6. 存储平仓通知上下文到 _pending 字典（deferred 通知）

        【平仓通知内容】
        与入场类似，通知延迟到 on_fill 回调后才发送。存储的上下文包括：
        - 交易方向（side_was）：平仓前是 LONG 还是 SHORT
        - 入场价格（entry_px）：用于计算盈亏
        - 退出预估价（estimated_exit_px）：仅退场回退使用
        - 持仓时长（在 on_fill 中从 slot.held_sec 读取）

        Args:
            slot: 要平仓的策略 slot
            reason: 平仓原因描述（如 "take_profit", "stop_loss", "BTC_shock" 等）
            price: 退出价格（可选，默认取最新成交价）

        Returns:
            bool: True = 平仓订单已提交，False = 不需要平仓（无持仓/已提交）
        """
        pos = self._get_position()
        if pos is None:
            return False

        from nautilus_trader.model.enums import OrderSide, TimeInForce

        # 平仓方向：做多持仓→卖出（SELL），做空持仓→买入（BUY）
        side = OrderSide.SELL if pos.side.name == "LONG" else OrderSide.BUY

        # ── 重复平仓保护 ──
        # 检查是否已有待确认的平仓订单，如果有则跳过本次操作。
        # 这防止了多个退出信号（如 tick 级和 bar 级同时触发）导致重复平仓。
        if self.has_pending_close_for(slot):
            return False

        # 获取退出价格
        if price is None:
            try:
                trade = self._cache.trade_tick(self._sol_id)
                price = float(trade.price) if trade else 0.0
            except Exception:
                price = 0.0
        exit_px = price

        # [maker] 创建并提交 GTC 限价单（挂被动价），超时撤单转 taker 兜底
        passive_px = self._passive_price(side, exit_px)
        order = self._create_limit_order(
            instrument_id=self._sol_id, order_side=side,
            quantity=pos.quantity, price=passive_px,
        )
        self._submit_order(order)

        # 在订单提交时读取 slot 状态快照（入场价和方向），
        # 这些是计算平仓盈亏所需的关键信息。
        # flat() 是发出平仓操作，slot 状态要在 on_fill 确认后才更新。
        entry_px = slot.entry_price if slot.has_position else float(pos.avg_px_open)
        side_was = slot.entry_side if slot.has_position else ("LONG" if pos.side.name == "LONG" else "SHORT")

        logger.info(f"FLAT {slot.strategy_id} reason={reason}")

        # ── 存储延迟平仓通知上下文 ──
        cid = str(order.client_order_id)
        self._pending[cid] = {
            "type": "close",              # 通知类型：平仓
            "is_maker": True,             # [maker] 限价单标记
            "side_order_obj": side,       # [maker] OrderSide 对象（兜底用）
            "slot": slot,                 # 关联的策略 slot
            "side_was": side_was,         # 平仓前的方向
            "entry_px": entry_px,         # 开仓均价（用于盈亏计算）
            "reason": reason,             # 平仓原因
            "estimated_exit_px": exit_px, # 预估退出价（仅退场回退使用）
            "expected_qty": float(pos.quantity.as_decimal()),  # 预期平仓数量
            "accum_qty": 0.0,             # 累计成交数量（填充用）
            "accum_notional": 0.0,        # 累计成交名义价值（用于计算 VWAP）
            "total_commission": slot.entry_commission,  # 带入入场手续费（双边手续费计算）
            "created_at": time.time(),    # 创建时间（用于超时判断）
        }
        # 记录 cid->instance 映射，供 data_manage 落库区分策略
        self._cid_instance[cid] = slot.strategy_id
        # [maker] 调度超时检查
        import asyncio
        try:
            asyncio.get_event_loop().create_task(self._maker_timeout(cid, 5.0))
        except Exception as _e:
            logger.warning(f"[maker] schedule timeout failed: {_e}")
        return True

    def flat_all(self, slots, reason="shutdown"):
        """批量平仓 —— 平掉传入的所有策略持仓。

        通常用于系统关闭（shutdown）或紧急情况下需要平掉所有活跃策略仓位。
        遍历 slots 列表，对每个有持仓的策略调用 flat()。

        Args:
            slots: StrategySlot 列表
            reason: 平仓原因，默认为 "shutdown"
        """
        for s in slots:
            if s.has_position:
                self.flat(s, reason)

    # ── Fill-based notification (actual exchange prices) ──

    def on_fill(self, client_order_id: str, last_px: float, last_qty: float,
                commission: float = 0.0):
        """订单成交回调 —— 接收 NautilusTrader 的 fill 事件，发送带真实成交价的通知。

        这是 deferred（延迟）通知机制的核心回调方法。由 BaseStrategy.on_order_filled()
        在每笔订单部分或全部成交时调用。方法会根据累计成交量判断订单是否完全成交，
        在完全成交时计算 VWAP（成交量加权均价），更新 slot 状态，并发送 Telegram 通知。

        【Fill 累积逻辑】
        - 每次部分成交时：累加成交数量、成交名义价值、手续费
        - 不完全成交的判断：accum_qty >= expected_qty * 0.99（允许 1% 误差）
        - 完全成交后：计算 VWAP = accum_notional / accum_qty

        【入场 vs 平仓的差异处理】

        入场通知（type == "entry"）：
        - 更新 slot 状态：slot.open_position(side, vwap)
          - slot.entry_price = vwap（实际开仓均价）
          - slot.entry_side = 方向
          - slot.has_position = True
          - slot.opened_at = 当前时间
        - 发送入场 Telegram 通知（fmt_entry）

        平仓通知（type == "close"）：
        - 计算盈亏 —— 注意做多和做空公式不同：
          做多盈亏 = 数量 * (退出价 - 入场价) - 手续费
          做空盈亏 = 数量 * (入场价 - 退出价) - 手续费
        - 注意 held_sec 在 slot.reset_position() 之前读取，
          确保能获取到准确的持仓时长
        - 更新 slot 状态：slot.reset_position()
          - slot.has_position = False
          - slot.entry_price = 0.0
          - slot.entry_side = ""
          - slot.opened_at = None
        - 发送平仓 Telegram 通知（fmt_close）

        Args:
            client_order_id: 订单客户端 ID，用于查找 _pending 中的通知上下文
            last_px: 本次成交的价格
            last_qty: 本次成交的数量
            commission: 本次成交的手续费（累计到 total_commission）
        """
        pending = self._pending.get(client_order_id)
        if pending is None:
            return  # not our order, or already processed

        # 累加本次成交信息
        pending["accum_qty"] += last_qty
        pending["accum_notional"] += last_qty * last_px
        pending["total_commission"] += commission

        # ── 判断订单是否完全成交 ──
        # IOC 订单可能部分成交（剩余部分被取消），这里检查是否收到了足够的 fill。
        # 允许 1% 的精度误差（expected_qty 可能因四舍五入与真实成交有微小差异）。
        if pending["accum_qty"] < pending["expected_qty"] * 0.99:
            return  # 尚未完全成交，等待更多 fill

        # 计算 VWAP（成交量加权均价），作为实际成交参考价
        vwap = pending["accum_notional"] / pending["accum_qty"] if pending["accum_qty"] > 0 else last_px
        slot = pending["slot"]

        if pending["type"] == "entry":
            # ── 入场成交确认 ──
            # 更新 slot 状态：记录真实入场价、方向、持仓时间
            slot.open_position(pending["side"], vwap)
            # 发送入场 Telegram 通知（含真实 VWAP）
            _notify(slot, fmt_entry(
                slot.strategy_id, str(self._sol_id), pending["side"],
                vwap, pending["accum_qty"], pending["notional"], pending["reason"],
            ))
            # 保存入场手续费到 slot，供平仓时计算双边手续费后的净盈亏
            slot.entry_commission = pending["total_commission"]

        elif pending["type"] == "close":
            # ── 平仓成交确认 ──
            entry_px = pending["entry_px"]
            qty = pending["accum_qty"]
            # held_sec 必须在 slot.reset_position() 之前读取！
            # 因为 held_sec 属性会计算 time_since_opened，一旦 reset 就丢失了
            held_sec = slot.held_sec  # compute at fill time, slot hasn't been reset yet

            # ── 平仓盈亏计算 ──
            # 做多（LONG）：盈亏 = 退出价 - 入场价（正数 => 盈利）
            # 做空（SHORT）：盈亏 = 入场价 - 退出价（正数 => 盈利）
            if pending["side_was"] == "LONG":
                pnl = qty * (vwap - entry_px) - pending["total_commission"]
            else:
                pnl = qty * (entry_px - vwap) - pending["total_commission"]

            # 重置 slot 状态（持仓已平，清空相关字段）
            slot.reset_position()
            # 发送平仓 Telegram 通知（含入场价、退出价、盈亏、持仓时长）
            _notify(slot, fmt_close(
                slot.strategy_id, str(self._sol_id), pending["side_was"],
                entry_px, vwap, pnl, held_sec, pending["reason"],
            ))

        # 处理完毕，从 _pending 中移除
        del self._pending[client_order_id]

    def flush_pending(self):
        """清理滞留的待确认通知 —— 系统关闭时的兜底处理。

        当系统正常关闭时（如 main.py 的 shutdown 流程），某些订单可能还未收到
        fill 回调（约 10s 内未成交的 IOC 订单）。flush_pending 会在 shutdown 前
        被调用，对超时订单使用预估价发送通知，确保不会遗漏任何交易通知。

        处理逻辑：
        1. 遍历所有 _pending 中超过 10s 的订单
        2. 入场：使用 estimated_price（预估价）发送通知，标注 "(est)"
        3. 平仓：使用 estimated_exit_px（预估退出价）计算盈亏并发送，
           这里仍然先读取 slot.held_sec 再计算盈亏
        """
        now = time.time()
        stale = []
        for cid, p in list(self._pending.items()):
            if now - p["created_at"] > 10:  # 10s 宽限期：fill 未到达，使用预估价
                stale.append(cid)
                slot = p["slot"]
                if p["type"] == "entry":
                    px = p.get("estimated_price", 0)
                    _notify(slot, fmt_entry(
                        slot.strategy_id, str(self._sol_id), p["side"],
                        px, p["expected_qty"], p["notional"], p["reason"] + " (est)",
                    ))
                elif p["type"] == "close":
                    px = p.get("estimated_exit_px", 0)
                    entry_px = p["entry_px"]
                    qty = p["expected_qty"]
                    held_sec = slot.held_sec  # read from slot at flush time
                    if p["side_was"] == "LONG":
                        pnl = qty * (px - entry_px) - p["total_commission"] if px > 0 else -p["total_commission"]
                    else:
                        pnl = qty * (entry_px - px) - p["total_commission"] if px > 0 else -p["total_commission"]
                    _notify(slot, fmt_close(
                        slot.strategy_id, str(self._sol_id), p["side_was"],
                        entry_px, px, pnl, held_sec, p["reason"] + " (est)",
                    ))
        for cid in stale:
            del self._pending[cid]
        if stale:
            logger.info(f"flush_pending: sent {len(stale)} stale notifications")

    def accept_partial_fill(self, client_order_id: str):
        """接受部分成交 —— IOC 订单超时/取消时的兜底处理。

        【使用场景】
        当 IOC 订单被取消（超时未完全成交）时调用。例如 cleanup_pending 检测到
        超时未成交但已有一部分 fill，调用此方法处理已成交的部分。

        【入场 vs 平仓的差异处理】

        入场部分成交（type == "entry"）：
        - 如果 slot 已有持仓（之前已经处理过 open_position），跳过（避免重复更新）
        - 否则部分开仓：slot.open_position(side, vwap)
        - 发送通知标注 "(partial)"
        - slot 状态正常更新（即使只有部分仓位）

        平仓部分成交（type == "close"）：
        - 如果 slot 已无持仓（已经被其他 fill 处理过），跳过
        - 否则部分平仓：计算已平部分的盈亏，发送通知标注 "(partial)"
        - **注意**：不平仓 slot 状态！因为 IOC 只平掉了部分仓位，剩余持仓还在。
          这与 on_fill 中完全平仓后 reset_position() 的处理方式不同。

        Args:
            client_order_id: 订单客户端 ID，用于查找 _pending 中的上下文
        """
        pending = self._pending.get(client_order_id)
        if pending is None:
            return
        # 没有任何成交，直接丢弃
        if pending["accum_qty"] <= 0:
            del self._pending[client_order_id]
            return

        vwap = pending["accum_notional"] / pending["accum_qty"]
        slot = pending["slot"]

        if pending["type"] == "entry":
            # 入场部分成交：如果 slot 已经记录了持仓（被其他 fill 处理过），不再重复更新
            if slot.has_position:
                del self._pending[client_order_id]
                return
            # 部分开仓：确认部分仓位并更新 slot 状态
            slot.open_position(pending["side"], vwap)
            # 保存入场手续费到 slot（部分成交也需记录，后续平仓用）
            slot.entry_commission = pending["total_commission"]
            _notify(slot, fmt_entry(
                slot.strategy_id, str(self._sol_id), pending["side"],
                vwap, pending["accum_qty"], pending["notional"],
                pending["reason"] + " (partial)",
            ))

        elif pending["type"] == "close":
            # 平仓部分成交：如果 slot 已无持仓（被其他 fill 处理过），跳过
            if not slot.has_position:
                del self._pending[client_order_id]
                return
            entry_px = pending["entry_px"]
            qty = pending["accum_qty"]
            # held_sec 在 slot 重置前读取，但部分平仓不会重置 slot
            held_sec = slot.held_sec  # compute now, slot not reset for partial close
            if pending["side_was"] == "LONG":
                pnl = qty * (vwap - entry_px) - pending["total_commission"]
            else:
                pnl = qty * (entry_px - vwap) - pending["total_commission"]
            # 注意：部分平仓后，slot 状态保持不变！
            # 不调用 slot.reset_position()，因为 IOC 只平掉了部分仓位，
            # 剩余持仓仍然存在（由交易所管理），后续还会收到完整的 fill 回调。
            # 如果这里 reset 了，后面 on_fill 记录完整平仓时会丢失信息。
            _notify(slot, fmt_close(
                slot.strategy_id, str(self._sol_id), pending["side_was"],
                entry_px, vwap, pnl, held_sec,
                pending["reason"] + " (partial)",
            ))

        del self._pending[client_order_id]

    def cleanup_pending(self, max_age_sec: float = 10.0):
        """清理超时未成交的待确认订单 —— 定期/关闭时的兜底处理。

        轮询 _pending 字典，对超过 max_age_sec 仍未完全成交的订单进行处理：
        - 有部分成交（accum_qty > 0）：调用 accept_partial_fill 接受部分成交
        - 零成交（accum_qty == 0）：直接丢弃（IOC 未成交，无需处理）

        Args:
            max_age_sec: 超时阈值（秒），默认 10 秒

        Returns:
            int: 清理的订单数量
        """
        now = time.time()
        stale = [cid for cid, p in self._pending.items() if now - p["created_at"] > max_age_sec]
        for cid in stale:
            p = self._pending.get(cid)
            if p and p.get("accum_qty", 0) > 0:
                logger.warning(
                    f"cleanup_pending: partial accept for {cid} ({p['type']}), "
                    f"filled {p['accum_qty']}/{p['expected_qty']}"
                )
                self.accept_partial_fill(cid)
            else:
                logger.warning(
                    f"cleanup_pending: fill never arrived for {cid} ({p['type']})"
                )
                if cid in self._pending:
                    del self._pending[cid]
        return len(stale)
