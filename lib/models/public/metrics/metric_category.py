from typing import TYPE_CHECKING, ClassVar, List, Optional

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column
from sqlalchemy.sql.sqltypes import String

from ..._mixins import Timestamped
from ...base import Base, TableArgs

if TYPE_CHECKING:
    from .metric import Metric


class MetricCategory(Timestamped, Base):
    path: Mapped[str] = Column(
        String(255),
        CheckConstraint("path <> '' AND lower(path) = path"),
        primary_key=True,
    )
    name: Mapped[str] = Column(
        String(255),
        CheckConstraint("name <> ''"),
        nullable=False,
    )
    description: Mapped[Optional[str]] = Column(
        String(1023),
        CheckConstraint("description <> ''"),
    )

    metrics: Mapped[List['Metric']] = relationship(
        back_populates='category',
        lazy='noload',
        cascade='save-update, merge, expunge, delete, delete-orphan',
    )

    __table_args__: ClassVar[TableArgs] = (dict(schema='public'),)
