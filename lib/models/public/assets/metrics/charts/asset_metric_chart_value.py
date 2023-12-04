from datetime import datetime
from decimal import Decimal
from typing import ClassVar, List

from sqlalchemy.orm import relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import ARRAY, DateTime, Numeric

from .....base import Base, TableArgs
from .asset_metric_chart import AssetMetricChart


class AssetMetricChartValue(Base):
    asset_metric_chart_id: Mapped[str] = Column(
        AssetMetricChart.id.type,
        ForeignKey(
            AssetMetricChart.id,
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        primary_key=True,
    )
    timestamp: Mapped[datetime] = Column(
        DateTime(timezone=True),
        primary_key=True,
    )
    values: Mapped[List[Decimal]] = Column(ARRAY(Numeric), nullable=False)

    chart: Mapped['AssetMetricChart'] = relationship(
        back_populates='values',
        lazy='noload',
        cascade='save-update',
    )

    __table_args__: ClassVar[TableArgs] = (dict(schema='public'),)
