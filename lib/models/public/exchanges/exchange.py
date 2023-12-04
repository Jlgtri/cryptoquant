from typing import TYPE_CHECKING, ClassVar, List

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column
from sqlalchemy.sql.sqltypes import String

from ..._mixins import Timestamped
from ...base import Base, TableArgs

if TYPE_CHECKING:
    from ..assets.metrics.charts.asset_metric_chart import AssetMetricChart


class Exchange(Timestamped, Base):
    key: Mapped[str] = Column(
        String(255),
        CheckConstraint("key <> '' AND lower(key) = key"),
        primary_key=True,
    )
    name: Mapped[str] = Column(
        String(255),
        CheckConstraint("name <> ''"),
        nullable=False,
    )

    charts: Mapped[List['AssetMetricChart']] = relationship(
        back_populates='exchange',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
        foreign_keys='[AssetMetricChart.exchange_key]',
    )
    from_charts: Mapped[List['AssetMetricChart']] = relationship(
        back_populates='from_exchange',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
        foreign_keys='[AssetMetricChart.from_exchange_key]',
    )
    to_charts: Mapped[List['AssetMetricChart']] = relationship(
        back_populates='to_exchange',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
        foreign_keys='[AssetMetricChart.to_exchange_key]',
    )

    __table_args__: ClassVar[TableArgs] = (dict(schema='public'),)
