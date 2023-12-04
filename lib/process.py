from datetime import datetime
from decimal import Decimal
from typing import Iterable

from numpy import float64
from pandas import DataFrame

from .models.public.assets.metrics.asset_metric_field import AssetMetricField
from .models.public.assets.metrics.charts.asset_metric_chart import (
    AssetMetricChart,
)


def get_charts_df(charts: Iterable[AssetMetricChart], /) -> DataFrame:
    chart_keys = ['Timestamp', 'Year', 'Month', 'Day']
    chart_values: dict[datetime, Iterable[Decimal]] = {}
    for chart in charts:
        if len(chart.metric.fields) > 1 and all(
            _.name != 'Open' for _ in chart.metric.fields
        ):
            continue
        has_data, empty_data = False, not chart_values
        for value in chart.values:
            # if any(_ is None for _ in value.values):
            #     continue
            has_data = True
            if empty_data:
                chart_values[value.timestamp] = value.values
            elif value.timestamp in chart_values:
                chart_values[value.timestamp] += value.values
        if not has_data:
            continue

        processed_field_names = set()
        for field in chart.metric.fields:
            if field.name in processed_field_names:
                chart_key = '%s (%s)' % (chart.title, field.key)
            elif len(chart.metric.fields) > 1:
                chart_key = '%s (%s)' % (chart.title, field.name)
                processed_field_names.add(field.name)
            else:
                chart_key = chart.title
            chart_keys.append(chart_key)

    return DataFrame(
        [
            [ts.timestamp(), ts.year, ts.month, ts.day] + values
            for ts, values in chart_values.items()
        ],
        columns=chart_keys if chart_values else None,
        dtype=float64,
    )


def deshuffle(
    df: DataFrame,
    fields: Iterable[AssetMetricField],
    values: DataFrame,
    /,
) -> DataFrame:
    df = df.copy()
    for field, prediction in zip(fields, values):
        df.insert(len(df.columns), field.name, prediction)
    s_pred = df.sort_values(df.columns.values[0])
    return s_pred.iloc[:, -len(fields) :]
