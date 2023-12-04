from typing import TYPE_CHECKING, ClassVar, List, Optional

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column, ForeignKey
from sqlalchemy.sql.sqltypes import String

from ..._mixins import Timestamped
from ...base import Base, TableArgs
from .metric_category import MetricCategory

if TYPE_CHECKING:
    from ..assets.asset import AssetMetric


class Metric(Timestamped, Base):
    category_path: Mapped[str] = Column(
        MetricCategory.path.type,
        ForeignKey(
            MetricCategory.path,
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        nullable=False,
    )
    path: Mapped[str] = Column(
        String(255),
        CheckConstraint("path <> '' AND lower(path) = path"),
        primary_key=True,
    )
    title: Mapped[str] = Column(
        String(255),
        CheckConstraint("title <> ''"),
        nullable=False,
    )
    description: Mapped[Optional[str]] = Column(
        String(1023),
        CheckConstraint("description <> ''"),
    )
    type: Mapped[Optional[str]] = Column(
        String(255),
        CheckConstraint("type <> ''"),
    )

    category: Mapped['MetricCategory'] = relationship(
        back_populates='metrics',
        lazy='noload',
        cascade='save-update',
    )
    assets: Mapped[List['AssetMetric']] = relationship(
        back_populates='metric',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
    )

    __table_args__: ClassVar[TableArgs] = (dict(schema='public'),)
