from enum import StrEnum
from typing import TYPE_CHECKING, ClassVar, List, Optional

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column, ForeignKey
from sqlalchemy.sql.sqltypes import ARRAY, Enum, String

from ...._mixins import Timestamped
from ....base import Base, TableArgs
from ...metrics.metric import Metric
from ..asset import Asset

if TYPE_CHECKING:
    from .asset_metric_field import AssetMetricField
    from .charts.asset_metric_chart import AssetMetricChart


class MetricWindow(StrEnum):
    block: ClassVar[str] = 'BLOCK'
    day: ClassVar[str] = 'DAY'
    hour: ClassVar[str] = 'HOUR'
    minute: ClassVar[str] = 'MIN'


class AssetMetric(Timestamped, Base):
    id: Mapped[str] = Column(
        String(24),
        CheckConstraint("id <> '' AND lower(id) = id"),
        primary_key=True,
    )
    asset_id: Mapped[str] = Column(
        Asset.id.type,
        ForeignKey(Asset.id, onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False,
    )
    metric_path: Mapped[str] = Column(
        Metric.path.type,
        ForeignKey(Metric.path, onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False,
    )
    chart_style: Mapped[Optional[str]] = Column(
        String(255),
        CheckConstraint("chart_style <> ''"),
    )
    chart_type: Mapped[Optional[str]] = Column(
        String(255),
        CheckConstraint("chart_type <> ''"),
    )
    expected_close_windows: Mapped[List[MetricWindow]] = Column(
        ARRAY(Enum(MetricWindow)),
        nullable=False,
        default=[],
    )
    windows: Mapped[List[MetricWindow]] = Column(
        ARRAY(Enum(MetricWindow)),
        nullable=False,
        default=[],
    )

    asset: Mapped['Asset'] = relationship(
        back_populates='metrics',
        lazy='noload',
        cascade='save-update',
    )
    metric: Mapped['Metric'] = relationship(
        back_populates='assets',
        lazy='noload',
        cascade='save-update',
    )
    fields: Mapped[List['AssetMetricField']] = relationship(
        back_populates='metric',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
        order_by='AssetMetricField.created_at',
    )
    charts: Mapped[List['AssetMetricChart']] = relationship(
        back_populates='metric',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
    )

    __table_args__: ClassVar[TableArgs] = (dict(schema='public'),)
