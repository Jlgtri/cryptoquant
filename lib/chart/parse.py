from contextlib import suppress
from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import Dict, Iterable, Optional

from sqlalchemy.dialects.postgresql.dml import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio.scoping import async_scoped_session

from ..models.public.assets.asset import Asset
from ..models.public.assets.metrics.asset_metric import AssetMetric
from ..models.public.assets.metrics.asset_metric_field import AssetMetricField
from ..models.public.assets.metrics.charts.asset_metric_chart import (
    AssetMetricChart,
)
from ..models.public.assets.metrics.charts.asset_metric_chart_value import (
    AssetMetricChartValue,
)
from ..models.public.banks.bank import Bank
from ..models.public.exchanges.exchange import Exchange
from ..models.public.market.market import Market
from ..models.public.metrics.metric import Metric
from ..models.public.metrics.metric_category import MetricCategory
from ..models.public.miners.miner import Miner
from ..models.public.symbols.symbol import Symbol

#
logger = getLogger('Parse')


async def parse_asset(
    Session: async_scoped_session,
    /,
    item: Dict[str, object],
    *,
    index: Optional[int] = None,
) -> Optional[Asset]:
    try:
        asset = Asset(
            id=item['id'],
            path=item['path'],
            name=item['name']['en'],
            symbol=item['symbol'],
        )
    except BaseException as _:
        return logger.info(
            '%sInvalid asset: %s',
            '[%s] ' % index if index is not None else '',
            item,
        )

    logger.info(
        '%sParsing asset `%s`...',
        '[%s] ' % index if index is not None else '',
        asset.name,
    )

    try:
        asset = await Session.merge(asset)
        await Session.commit()
        return asset
    except IntegrityError as exception:
        logger.info(
            '%sException while parsing asset `%s`: \n\n%s',
            '[%s] ' % index if index is not None else '',
            asset.name,
            exception,
        )
        return await Session.rollback()


async def parse_category(
    Session: async_scoped_session,
    /,
    item: Dict[str, object],
    *,
    index: Optional[int] = None,
) -> Optional[MetricCategory]:
    try:
        category = MetricCategory(path=item['path'], name=item['name']['en'])
        with suppress(BaseException):
            category.description = item['description']['en']
    except BaseException as _:
        return logger.info(
            '%sInvalid category metric: \n\n%s',
            '[%s] ' % index if index is not None else '',
            item,
        )

    logger.info(
        '%sParsing category metric `%s`...',
        '[%s] ' % index if index is not None else '',
        category.name,
    )

    try:
        category = await Session.merge(category)
        await Session.commit()
        return category
    except IntegrityError as exception:
        logger.info(
            '%sException while parsing category metric `%s`: \n\n%s',
            '[%s] ' % index if index is not None else '',
            category.name,
            exception,
        )
        return await Session.rollback()


async def parse_metric(
    Session: async_scoped_session,
    /,
    asset_id: str,
    category_path: str,
    item: Dict[str, object],
    *,
    index: Optional[int] = None,
) -> Optional[AssetMetric]:
    try:
        metric = Metric(
            category_path=category_path,
            path=item['path'],
            title=item['title']['en'],
            type=item.get('metricType'),
        )
        with suppress(BaseException):
            metric.description = item['description']['en']
        asset_metric = AssetMetric(
            metric_path=item['path'],
            asset_id=asset_id,
            id=item.get('id') or item['metricId'],
        )
    except BaseException as _:
        return logger.info(
            '%sInvalid metric: \n\n%s',
            '[%s] ' % index if index is not None else '',
            item,
        )

    logger.info(
        '%sParsing metric `%s`...',
        '[%s] ' % index if index is not None else '',
        metric.title,
    )

    try:
        await Session.merge(metric)
        await Session.merge(asset_metric)
        await Session.commit()
    except IntegrityError as exception:
        logger.info(
            '%sException while parsing metric `%s`: \n\n%s',
            '[%s] ' % index if index is not None else '',
            metric.title,
            exception,
        )
        return await Session.rollback()
    return asset_metric


async def parse_metric_fields(
    Session: async_scoped_session,
    /,
    asset_id: str,
    metric_path: str,
    item: Dict[str, object],
    *,
    index: Optional[int] = None,
) -> Optional[Iterable[AssetMetricField]]:
    try:
        asset_metric = AssetMetric(
            metric_path=metric_path,
            asset_id=asset_id,
            id=item['metricId'],
            chart_style=item.get('chartStyle'),
            chart_type=item.get('metricChartType'),
            expected_close_windows=item.get('expectedCloseWindows') or [],
            windows=item.get('windows') or [],
        )
    except BaseException as _:
        return logger.info(
            '%sInvalid metric: \n\n%s',
            '[%s] ' % index if index is not None else '',
            item,
        )

    fields = []
    for field in item['metricFields']:
        try:
            fields.append(
                AssetMetricField(
                    id=field['id'],
                    asset_metric_id=item['metricId'],
                    key=field['key'],
                    name=field['name']['en'],
                )
            )
        except BaseException as _:
            return logger.info(
                '%sInvalid metric `%s` field: \n\n%s',
                '[%s] ' % index if index is not None else '',
                asset_metric.id,
                field,
            )

    logger.info(
        '%sParsing metric `%s`%s...',
        '[%s] ' % index if index is not None else '',
        asset_metric.id,
        (' with field%s {}' % ('' if len(fields) == 1 else 's')).format(
            ', '.join('`%s`' % field.name for field in fields)
        )
        if fields
        else '',
    )

    try:
        await Session.merge(asset_metric)
        for field in fields:
            await Session.merge(field)
        await Session.commit()
    except IntegrityError as exception:
        logger.info(
            '%sException while parsing metric `%s`: \n\n%s',
            '[%s] ' % index if index is not None else '',
            asset_metric.id,
            exception,
        )
        return await Session.rollback()

    return fields


async def parse_chart(
    Session: async_scoped_session,
    /,
    asset_metric_id: str,
    item: Dict[str, object],
    *,
    index: Optional[int] = None,
) -> Optional[AssetMetricChart]:
    try:
        asset_metric_chart = AssetMetricChart(
            id=item['id'],
            asset_metric_id=asset_metric_id,
            miner_key=(item.get('miner') or {}).get('key'),
            to_miner_key=(item.get('toMiner') or {}).get('key'),
            from_miner_key=(item.get('fromMiner') or {}).get('key'),
            market_key=(item.get('market') or {}).get('key'),
            exchange_key=(item.get('exchange') or {}).get('key'),
            from_exchange_key=(item.get('fromExchange') or {}).get('key'),
            to_exchange_key=(item.get('toExchange') or {}).get('key'),
            from_bank_key=(item.get('fromBank') or {}).get('key'),
            symbol_key=(item.get('symbol') or {}).get('key'),
            title=item['title']['en'],
        )
        market = exchange = from_exchange = to_exchange = None
        miner = from_miner = to_miner = from_bank = symbol = None

        if asset_metric_chart.miner_key is not None:
            miner = Miner(
                key=item['miner']['key'],
                name=item['miner']['name']['en'],
            )
        if asset_metric_chart.from_miner_key is not None:
            from_miner = Miner(
                key=item['fromMiner']['key'],
                name=item['fromMiner']['name']['en'],
            )
        if asset_metric_chart.to_miner_key is not None:
            to_miner = Miner(
                key=item['toMiner']['key'],
                name=item['toMiner']['name']['en'],
            )
        if asset_metric_chart.market_key is not None:
            market = Market(
                key=item['market']['key'],
                name=item['market']['name']['en'],
            )
        if asset_metric_chart.exchange_key is not None:
            exchange = Exchange(
                key=item['exchange']['key'],
                name=item['exchange']['name']['en'],
            )
        if asset_metric_chart.exchange_key is not None:
            exchange = Exchange(
                key=item['exchange']['key'],
                name=item['exchange']['name']['en'],
            )
        if asset_metric_chart.from_exchange_key is not None:
            from_exchange = Exchange(
                key=item['fromExchange']['key'],
                name=item['fromExchange']['name']['en'],
            )
        if asset_metric_chart.to_exchange_key is not None:
            to_exchange = Exchange(
                key=item['toExchange']['key'],
                name=item['toExchange']['name']['en'],
            )
        if asset_metric_chart.from_bank_key is not None:
            from_bank = Bank(
                key=item['fromBank']['key'],
                name=item['fromBank']['name']['en'],
            )
        if asset_metric_chart.symbol_key is not None:
            symbol = Symbol(
                key=item['symbol']['key'],
                name=item['symbol']['name']['en'],
            )
    except BaseException as _:
        return logger.info(
            '%sInvalid chart: \n\n%s',
            '[%s] ' % index if index is not None else '',
            item,
        )

    logger.info(
        '%sParsing asset metric chart `%s`...',
        '[%s] ' % index if index is not None else '',
        asset_metric_chart.title,
    )

    try:
        if miner is not None:
            miner = await Session.merge(miner)
        if from_miner is not None:
            from_miner = await Session.merge(from_miner)
        if to_miner is not None:
            to_miner = await Session.merge(to_miner)
        if market is not None:
            market = await Session.merge(market)
        if exchange is not None:
            exchange = await Session.merge(exchange)
        if from_exchange is not None:
            from_exchange = await Session.merge(from_exchange)
        if to_exchange is not None:
            to_exchange = await Session.merge(to_exchange)
        if from_bank is not None:
            from_bank = await Session.merge(from_bank)
        if symbol is not None:
            symbol = await Session.merge(symbol)
        asset_metric_chart = await Session.merge(asset_metric_chart)
        await Session.commit()
        return asset_metric_chart
    except IntegrityError as exception:
        logger.info(
            '%sException while parsing asset metric chart `%s`: \n\n%s',
            '[%s] ' % index if index is not None else '',
            asset_metric_chart.title,
            exception,
        )
        return await Session.rollback()


async def parse_chart_value(
    Session: async_scoped_session,
    /,
    asset_metric_chart_id: str,
    item: Dict[int, Decimal],
    *,
    index: Optional[int] = None,
) -> Optional[Iterable[AssetMetricChartValue]]:
    if not item:
        return None
    asset_metric_chart = await Session.get(
        AssetMetricChart, asset_metric_chart_id
    )
    if asset_metric_chart is None:
        return None
    logger.info(
        '%sParsing asset metric chart `%s` values...',
        '[%s] ' % index if index is not None else '',
        asset_metric_chart.title,
    )
    asset_metric_chart_values = []
    if Session.bind.dialect.name == "postgresql":
        await Session.execute(
            insert(AssetMetricChartValue)
            .values(
                [
                    [
                        asset_metric_chart_id,
                        datetime.fromtimestamp(timestamp / 1000),
                        values,
                    ]
                    for timestamp, values in item.items()
                    if values is not None
                ]
            )
            .on_conflict_do_nothing()
        )
    else:
        for timestamp, values in item.items():
            if values is None:
                continue
            try:
                asset_metric_chart_value = AssetMetricChartValue(
                    asset_metric_chart_id=asset_metric_chart_id,
                    timestamp=datetime.fromtimestamp(timestamp / 1000),
                    values=values,
                )
            except BaseException as _:
                return logger.info(
                    '%sInvalid chart value: %s, %s',
                    '[%s] ' % index if index is not None else '',
                    timestamp,
                    values,
                )

            try:
                asset_metric_chart_values.append(
                    await Session.merge(asset_metric_chart_value)
                )
            except IntegrityError as exception:
                logger.info(
                    '%sException while parsing asset metric chart `%s`: '
                    '\n\n%s',
                    '[%s] ' % index if index is not None else '',
                    asset_metric_chart.title,
                    exception,
                )
                return await Session.rollback()
    await Session.commit()
    return asset_metric_chart_values
