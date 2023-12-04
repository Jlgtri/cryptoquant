from typing import TYPE_CHECKING, ClassVar, List, Optional

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column, ForeignKey
from sqlalchemy.sql.sqltypes import String

from ....._mixins import Timestamped
from .....base import Base, TableArgs
from ....banks.bank import Bank
from ....exchanges.exchange import Exchange
from ....market.market import Market
from ....miners.miner import Miner
from ....symbols.symbol import Symbol
from ..asset_metric import AssetMetric

if TYPE_CHECKING:
    from .asset_metric_chart_value import AssetMetricChartValue


class AssetMetricChart(Timestamped, Base):
    id: Mapped[str] = Column(
        String(24),
        CheckConstraint("id <> '' AND lower(id) = id"),
        primary_key=True,
    )
    asset_metric_id: Mapped[str] = Column(
        AssetMetric.id.type,
        ForeignKey(AssetMetric.id, onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False,
    )
    title: Mapped[str] = Column(
        String(511),
        CheckConstraint("title <> ''"),
        nullable=False,
    )
    miner_key: Mapped[Optional[str]] = Column(
        Miner.key.type,
        ForeignKey(Miner.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    from_miner_key: Mapped[Optional[str]] = Column(
        Miner.key.type,
        ForeignKey(Miner.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    to_miner_key: Mapped[Optional[str]] = Column(
        Miner.key.type,
        ForeignKey(Miner.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    market_key: Mapped[Optional[str]] = Column(
        Market.key.type,
        ForeignKey(Market.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    exchange_key: Mapped[Optional[str]] = Column(
        Exchange.key.type,
        ForeignKey(Exchange.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    from_exchange_key: Mapped[Optional[str]] = Column(
        Exchange.key.type,
        ForeignKey(Exchange.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    to_exchange_key: Mapped[Optional[str]] = Column(
        Exchange.key.type,
        ForeignKey(Exchange.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    from_bank_key: Mapped[Optional[str]] = Column(
        Bank.key.type,
        ForeignKey(Bank.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )
    symbol_key: Mapped[Optional[str]] = Column(
        Symbol.key.type,
        ForeignKey(Symbol.key, onupdate='CASCADE', ondelete='RESTRICT'),
    )

    metric: Mapped['AssetMetric'] = relationship(
        back_populates='charts',
        lazy='noload',
        cascade='save-update',
    )
    miner: Mapped[Optional['Miner']] = relationship(
        back_populates='charts',
        lazy='noload',
        cascade='save-update',
        foreign_keys=[miner_key],
    )
    from_miner: Mapped[Optional['Miner']] = relationship(
        back_populates='from_charts',
        lazy='noload',
        cascade='save-update',
        foreign_keys=[from_miner_key],
    )
    to_miner: Mapped[Optional['Miner']] = relationship(
        back_populates='to_charts',
        lazy='noload',
        cascade='save-update',
        foreign_keys=[to_miner_key],
    )
    market: Mapped[Optional['Market']] = relationship(
        back_populates='charts',
        lazy='noload',
        cascade='save-update',
    )
    exchange: Mapped[Optional['Exchange']] = relationship(
        back_populates='charts',
        lazy='noload',
        cascade='save-update',
        foreign_keys=[exchange_key],
    )
    from_exchange: Mapped[Optional['Exchange']] = relationship(
        back_populates='from_charts',
        lazy='noload',
        cascade='save-update',
        foreign_keys=[from_exchange_key],
    )
    to_exchange: Mapped[Optional['Exchange']] = relationship(
        back_populates='to_charts',
        lazy='noload',
        cascade='save-update',
        foreign_keys=[to_exchange_key],
    )
    from_bank: Mapped[Optional['Bank']] = relationship(
        back_populates='from_charts',
        lazy='noload',
        cascade='save-update',
        foreign_keys=[from_bank_key],
    )
    symbol: Mapped[Optional['Symbol']] = relationship(
        back_populates='charts',
        lazy='noload',
        cascade='save-update',
    )
    values: Mapped[List['AssetMetricChartValue']] = relationship(
        back_populates='chart',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
    )

    __table_args__: ClassVar[TableArgs] = (dict(schema='public'),)
