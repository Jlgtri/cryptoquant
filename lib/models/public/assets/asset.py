from typing import TYPE_CHECKING, List

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column
from sqlalchemy.sql.sqltypes import String

from ..._mixins import Timestamped
from ...base import Base

if TYPE_CHECKING:
    from .metrics.asset_metric import AssetMetric


class Asset(Timestamped, Base):
    id: Mapped[str] = Column(
        String(24),
        CheckConstraint("id <> '' AND lower(id) = id"),
        primary_key=True,
    )
    path: Mapped[str] = Column(
        String(255),
        CheckConstraint("path <> '' AND lower(path) = path"),
        nullable=False,
        index=True,
        unique=True,
    )
    name: Mapped[str] = Column(
        String(255),
        CheckConstraint("name <> ''"),
        nullable=False,
    )
    symbol: Mapped[str] = Column(
        String(255),
        CheckConstraint("symbol <> ''"),
        nullable=False,
    )

    metrics: Mapped[List['AssetMetric']] = relationship(
        back_populates='asset',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
    )
