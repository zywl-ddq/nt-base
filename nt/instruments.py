"""SOLUSDT-PERP instrument factory for NautilusTrader."""
from decimal import Decimal
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider


def solusdt_perp_binance() -> CryptoPerpetual:
    """SOLUSDT-PERP.BINANCE with SOL contract specs."""
    base = TestInstrumentProvider.btcusdt_perp_binance()
    iid = InstrumentId.from_str("SOLUSDT-PERP.BINANCE")
    return CryptoPerpetual(
        instrument_id=iid,
        raw_symbol=iid.symbol,
        base_currency=base.base_currency,
        quote_currency=base.quote_currency,
        settlement_currency=base.settlement_currency,
        is_inverse=False,
        price_precision=2,
        size_precision=2,
        price_increment=Price(Decimal("0.01"), 2),
        size_increment=Quantity(Decimal("0.01"), 2),
        multiplier=base.multiplier,
        lot_size=Quantity(Decimal("0.01"), 2),
        max_quantity=base.max_quantity,
        min_quantity=base.min_quantity,
        max_notional=base.max_notional,
        min_notional=base.min_notional,
        max_price=base.max_price,
        min_price=base.min_price,
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.025"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0005"),
        ts_event=int(0),
        ts_init=int(0),
        info={},
    )
