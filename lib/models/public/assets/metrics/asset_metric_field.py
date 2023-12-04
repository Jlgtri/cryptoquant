from typing import ClassVar

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column, ForeignKey
from sqlalchemy.sql.sqltypes import String

from ...._mixins import Timestamped
from ....base import Base, TableArgs
from .asset_metric import AssetMetric


class AssetMetricField(Timestamped, Base):
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
    key: Mapped[str] = Column(
        String(255),
        CheckConstraint("id <> '' AND lower(id) = id"),
        nullable=False,
    )
    name: Mapped[str] = Column(
        String(255),
        CheckConstraint("name <> ''"),
        nullable=False,
    )

    metric: Mapped['AssetMetric'] = relationship(
        back_populates='fields',
        lazy='noload',
        cascade='save-update',
    )

    __table_args__: ClassVar[TableArgs] = (dict(schema='public'),)
