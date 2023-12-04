from typing import Final, Optional

from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import CheckConstraint, Column
from sqlalchemy.sql.sqltypes import String

from ..._mixins import Timestamped
from ...base import Base, TableArgs


class Authorization(Timestamped, Base):
    username: Mapped[str] = Column(
        String(255),
        CheckConstraint("username <> ''"),
        primary_key=True,
    )
    access_token: Mapped[Optional[str]] = Column(
        String(511),
        CheckConstraint(
            r'access_token IS NULL OR refresh_token IS NOT NULL AND '
            r"access_token <> '' AND access_token !~ '\s'"
        ),
    )
    refresh_token: Mapped[Optional[str]] = Column(
        String(511),
        CheckConstraint(
            r'refresh_token IS NULL OR access_token IS NOT NULL AND '
            r"refresh_token <> '' AND refresh_token !~ '\s'"
        ),
    )

    __table_args__: Final[TableArgs] = (dict(schema='service'),)
