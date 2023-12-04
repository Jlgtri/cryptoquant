from dataclasses import dataclass
from decimal import Decimal
from logging import Logger, getLogger
from typing import Dict, Final, Iterable, Optional, Self, Tuple, Union

from aiohttp import ClientSession
from anyio import create_memory_object_stream, create_task_group
from anyio import sleep as asleep
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.ext.asyncio.scoping import async_scoped_session
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm.strategy_options import contains_eager, selectinload
from sqlalchemy.sql.expression import select

from ..authorization import CryptoQuantAuthorization
from ..models.public.assets.asset import Asset
from ..models.public.assets.metrics.asset_metric import AssetMetric
from ..models.public.assets.metrics.charts.asset_metric_chart import (
    AssetMetricChart,
)
from ..models.public.metrics.metric import Metric
from ..utils.filter_type import filter_type
from ..utils.get_bind import Bind, get_bind
from .fetch import fetch_assets, fetch_chart, fetch_charts, fetch_metrics
from .fetch import logger as fetch_logger
from .parse import (
    parse_asset,
    parse_category,
    parse_chart,
    parse_chart_value,
    parse_metric,
    parse_metric_fields,
)


@dataclass(init=False, frozen=True)
class CryptoQuantChart(object):
    assets: Final[Iterable[str]]
    categories: Final[Iterable[str]]
    metrics: Final[Iterable[str]]
    miners: Final[Iterable[str]]
    to_miners: Final[Iterable[str]]
    from_miners: Final[Iterable[str]]
    markets: Final[Iterable[str]]
    exchanges: Final[Iterable[str]]
    from_exchanges: Final[Iterable[str]]
    to_exchanges: Final[Iterable[str]]
    from_banks: Final[Iterable[str]]
    symbols: Final[Iterable[str]]
    fields: Final[Iterable[str]]
    predict: Final[bool]
    force: Final[bool]

    _logger: Final[Logger]
    _session: Final[ClientSession]
    _engine: Final[AsyncEngine]
    _Session: Final[
        Union[
            sessionmaker[AsyncSession],
            async_scoped_session[AsyncSession],
        ]
    ]
    _auth: Final[CryptoQuantAuthorization]

    def __init__(
        self: Self,
        /,
        bind: Bind,
        assets: Optional[Union[str, Iterable[str]]] = None,
        categories: Optional[Union[str, Iterable[str]]] = None,
        metrics: Optional[Union[str, Iterable[str]]] = None,
        miners: Optional[Union[str, Iterable[str]]] = None,
        to_miners: Optional[Union[str, Iterable[str]]] = None,
        from_miners: Optional[Union[str, Iterable[str]]] = None,
        markets: Optional[Union[str, Iterable[str]]] = None,
        exchanges: Optional[Union[str, Iterable[str]]] = None,
        from_exchanges: Optional[Union[str, Iterable[str]]] = None,
        to_exchanges: Optional[Union[str, Iterable[str]]] = None,
        from_banks: Optional[Union[str, Iterable[str]]] = None,
        symbols: Optional[Union[str, Iterable[str]]] = None,
        fields: Optional[Union[str, Iterable[str]]] = None,
        session: Optional[ClientSession] = None,
        auth: Optional[CryptoQuantAuthorization] = None,
        *,
        predict: Optional[bool] = None,
        force: Optional[bool] = None,
        logger_name: Optional[str] = None,
    ) -> None:
        engine, Session = get_bind(bind)
        object.__setattr__(self, 'assets', filter_type(assets, str))
        object.__setattr__(self, 'metrics', filter_type(metrics, str))
        object.__setattr__(self, 'categories', filter_type(categories, str))
        object.__setattr__(self, 'miners', filter_type(miners, str))
        object.__setattr__(self, 'to_miners', filter_type(to_miners, str))
        object.__setattr__(self, 'from_miners', filter_type(from_miners, str))
        object.__setattr__(self, 'markets', filter_type(markets, str))
        object.__setattr__(self, 'exchanges', filter_type(exchanges, str))
        object.__setattr__(
            self, 'from_exchanges', filter_type(from_exchanges, str)
        )
        object.__setattr__(
            self, 'to_exchanges', filter_type(to_exchanges, str)
        )
        object.__setattr__(self, 'from_banks', filter_type(from_banks, str))
        object.__setattr__(self, 'symbols', filter_type(symbols, str))
        object.__setattr__(self, 'fields', filter_type(fields, str))
        object.__setattr__(
            self,
            'predict',
            predict if isinstance(predict, bool) else False,
        )
        object.__setattr__(
            self,
            'force',
            force if isinstance(force, bool) else False,
        )
        object.__setattr__(
            self,
            '_logger',
            getLogger(logger_name or self.__class__.__name__),
        )
        object.__setattr__(
            self,
            '_session',
            session
            if isinstance(session, ClientSession)
            else ClientSession('https://live-api.cryptoquant.com'),
        )
        object.__setattr__(self, '_engine', engine)
        object.__setattr__(self, '_Session', Session)
        object.__setattr__(
            self,
            '_auth',
            auth
            if isinstance(session, CryptoQuantAuthorization)
            else CryptoQuantAuthorization(
                self._Session, session=self._session
            ),
        )

    async def run(self: Self, /) -> Iterable[AssetMetricChart]:
        if self.force or not (charts := await self.get_charts()):
            await self.fetch()
        else:
            await self._Session.remove()
            return charts

        charts = await self.get_charts()
        await self._Session.remove()
        return charts

    async def get_charts(self: Self, /) -> Iterable[AssetMetricChart]:
        self._logger.info('Loading charts...')
        statement = (
            select(AssetMetricChart)
            .join(AssetMetricChart.metric)
            .options(
                selectinload(AssetMetricChart.values),
                contains_eager(AssetMetricChart.metric).selectinload(
                    AssetMetric.fields
                ),
            )
        )
        if self.assets:
            statement = statement.join(AssetMetric.asset).where(
                Asset.path.in_(self.assets)
            )
        if self.categories:
            statement = statement.join(AssetMetric.metric).where(
                Metric.category_path.in_(self.categories)
            )
        if self.metrics:
            statement = statement.where(
                AssetMetric.metric_path.in_(self.metrics)
            )
        if self.miners:
            statement = statement.where(
                AssetMetricChart.miner_key.in_(self.miners)
            )
        if self.from_miners:
            statement = statement.where(
                AssetMetricChart.from_miner.in_(self.from_miners)
            )
        if self.to_miners:
            statement = statement.where(
                AssetMetricChart.to_miner.in_(self.to_miners)
            )
        if self.markets:
            statement = statement.where(
                AssetMetricChart.market_key.in_(self.markets)
            )
        if self.exchanges:
            statement = statement.where(
                AssetMetricChart.exchange_key.in_(self.exchanges)
            )
        if self.from_exchanges:
            statement = statement.where(
                AssetMetricChart.from_exchange_key.in_(self.from_exchanges)
            )
        if self.to_exchanges:
            statement = statement.where(
                AssetMetricChart.to_exchange_key.in_(self.to_exchanges)
            )
        if self.from_banks:
            statement = statement.where(
                AssetMetricChart.from_bank_key.in_(self.from_banks)
            )
        if self.symbols:
            statement = statement.where(
                AssetMetricChart.symbol_key.in_(self.symbols)
            )
        charts = (await self._Session.scalars(statement)).all()
        if not self.fields:
            return charts
        for chart in charts:
            pop_indexes = [
                index
                for index, field in reversed(
                    list(enumerate(chart.metric.fields))
                )
                if field.key not in self.fields
            ]
            for index in pop_indexes:
                chart.metric.fields.pop(index)
            for value in chart.values:
                for index in pop_indexes:
                    value.values.pop(index)
        return charts

    async def fetch(self: Self, /) -> None:
        self._logger.info('Charts started fetching...')
        asset_sender, asset_receiver = create_memory_object_stream(
            0, item_type=Dict[str, object]
        )
        category_sender, category_receiver = create_memory_object_stream(
            0, item_type=Tuple[str, Dict[str, object]]
        )
        metric_sender, metric_receiver = create_memory_object_stream(
            0, item_type=Tuple[str, str, Dict[str, object]]
        )
        chart_sender, chart_receiver = create_memory_object_stream(
            0, item_type=Tuple[str, str, str, Dict[str, object]]
        )
        sender, receiver = create_memory_object_stream(
            0,
            item_type=Tuple[str, str, str, str, Dict[int, Iterable[Decimal]]],
        )
        async with (
            asset_sender,
            asset_receiver,
            category_sender,
            category_receiver,
            metric_sender,
            metric_receiver,
            chart_sender,
            chart_receiver,
            sender,
            receiver,
            create_task_group() as tg,
        ):
            for index in range(1):
                tg.start_soon(
                    self.fetch_assets,
                    asset_sender,
                    index,
                )
            for index in range(1):
                tg.start_soon(
                    self.fetch_categories,
                    asset_receiver,
                    category_sender,
                    index,
                )
            for index in range(1):
                tg.start_soon(
                    self.fetch_metrics,
                    category_receiver,
                    metric_sender,
                    index,
                )
            for index in range(1):
                tg.start_soon(
                    self.fetch_charts,
                    metric_receiver,
                    chart_sender,
                    index,
                )
            for index in range(1):
                tg.start_soon(
                    self.fetch_chart,
                    chart_receiver,
                    sender,
                    index,
                )
            for index in range(1):
                tg.start_soon(
                    self.fetch_chart_values,
                    receiver,
                    index,
                )
        await asleep(1)
        self._logger.info('Charts finished fetching!')

    async def fetch_assets(
        self: Self,
        asset_sender: MemoryObjectSendStream[Dict[str, object]],
        /,
        index: int,
    ) -> None:
        async with asset_sender:
            while (
                assets := await fetch_assets(self._session, index=index)
            ) is None:
                await self._auth.run()
                await self._Session.remove()
            if self.assets:
                for asset_id in self.assets:
                    if asset_id in assets:
                        await asset_sender.send(assets[asset_id])
                    else:
                        fetch_logger.info(
                            '%sSkipped processing asset `%s`!',
                            '[%s] ' % index if index is not None else '',
                            asset_id,
                        )
            else:
                for asset in assets.values():
                    await asset_sender.send(asset)

    async def fetch_categories(
        self: Self,
        asset_receiver: MemoryObjectReceiveStream[Dict[str, object]],
        category_sender: MemoryObjectSendStream[Tuple[str, Dict[str, object]]],
        /,
        index: int,
    ) -> None:
        async with asset_receiver, category_sender:
            async for asset in asset_receiver:
                asset = await parse_asset(self._Session, asset, index=index)
                if asset is None:
                    await self._Session.remove()
                    continue
                await self._Session.remove()

                while (
                    categories := await fetch_metrics(
                        self._session, asset.id, index=index
                    )
                ) is None:
                    await self._auth.run()
                    await self._Session.remove()

                if self.categories:
                    for category_id in self.categories:
                        if category_id in categories:
                            await category_sender.send(
                                (asset.id, categories[category_id])
                            )
                        else:
                            fetch_logger.info(
                                '%sSkipped processing metric category `%s` for '
                                'asset `%s`!',
                                '[%s] ' % index if index is not None else '',
                                category_id,
                                asset.id,
                            )
                else:
                    for category in categories.values():
                        await category_sender.send((asset.id, category))

    async def fetch_metrics(
        self: Self,
        category_receiver: MemoryObjectReceiveStream[
            Tuple[str, Dict[str, object]]
        ],
        metric_sender: MemoryObjectSendStream[
            Tuple[str, str, Dict[str, object]]
        ],
        /,
        index: int,
    ) -> None:
        async with category_receiver, metric_sender:
            async for asset_id, category in category_receiver:
                metrics = {
                    metric['path']: metric for metric in category['metrics']
                }
                category = await parse_category(
                    self._Session, category, index=index
                )
                await self._Session.remove()
                if category is None:
                    continue
                if self.metrics:
                    for metric_id in self.metrics:
                        if metric_id in metrics:
                            await metric_sender.send(
                                (asset_id, category.path, metrics[metric_id])
                            )
                        else:
                            fetch_logger.info(
                                '%sSkipped processing metric `%s` for '
                                'asset `%s`!',
                                '[%s] ' % index if index is not None else '',
                                metric_id,
                                asset_id,
                            )
                else:
                    for metric in metrics.values():
                        await metric_sender.send(
                            (asset_id, category.path, metric)
                        )

    async def fetch_charts(
        self: Self,
        metric_receiver: MemoryObjectReceiveStream[
            Tuple[str, str, Dict[str, object]]
        ],
        chart_sender: MemoryObjectSendStream[
            Tuple[str, str, str, Dict[str, object]]
        ],
        /,
        index: int,
    ) -> None:
        async with metric_receiver, chart_sender:
            async for asset_id, category_path, asset_metric in metric_receiver:
                asset_metric = await parse_metric(
                    self._Session,
                    asset_id,
                    category_path,
                    asset_metric,
                    index=index,
                )
                await self._Session.remove()
                if asset_metric is None:
                    continue

                while (
                    metric := await fetch_charts(
                        self._session, asset_metric.id, index=index
                    )
                ) is None:
                    await self._auth.run()
                    await self._Session.remove()

                fields = await parse_metric_fields(
                    self._Session,
                    asset_id,
                    asset_metric.metric_path,
                    metric,
                    index=index,
                )
                await self._Session.remove()

                all_miner = any(
                    (chart.get('miner') or {}).get('key') == 'all_miner'
                    for chart in metric['charts']
                )
                all_from_miner = any(
                    (chart.get('fromMiner') or {}).get('key') == 'all_miner'
                    for chart in metric['charts']
                )
                all_to_miner = any(
                    (chart.get('toMiner') or {}).get('key') == 'all_miner'
                    for chart in metric['charts']
                )
                all_exchange = any(
                    (chart.get('exchange') or {}).get('key') == 'all_exchange'
                    for chart in metric['charts']
                )
                all_from_exchange = any(
                    (chart.get('fromExchange') or {}).get('key')
                    == 'all_exchange'
                    for chart in metric['charts']
                )
                all_to_exchange = any(
                    (chart.get('toExchange') or {}).get('key')
                    in {'spot_exchange', 'derivative_exchange'}
                    for chart in metric['charts']
                )
                all_symbol = any(
                    (chart.get('symbol') or {}).get('key') == 'all_symbol'
                    for chart in metric['charts']
                )
                field_keys = {field.key for field in fields}
                for chart in metric['charts']:
                    miner_key = (chart.get('miner') or {}).get('key')
                    from_miner_key = (chart.get('fromMiner') or {}).get('key')
                    to_miner_key = (chart.get('toMiner') or {}).get('key')
                    market_key = (chart.get('market') or {}).get('key')
                    exchange_key = (chart.get('exchange') or {}).get('key')
                    from_exchange_key = (chart.get('fromExchange') or {}).get(
                        'key'
                    )
                    to_exchange_key = (chart.get('toExchange') or {}).get(
                        'key'
                    )
                    from_bank_key = (chart.get('fromBank') or {}).get('key')
                    symbol_key = (chart.get('symbol') or {}).get('key')
                    if (
                        (
                            not self.fields
                            or all(_ == field_keys for _ in self.fields)
                        )
                        and (
                            miner_key is None
                            or not self.miners
                            and (not all_miner or miner_key == 'all_miner')
                            or any(_ == miner_key for _ in self.miners)
                        )
                        and (
                            from_miner_key is None
                            or not self.from_miners
                            and (
                                not all_from_miner
                                or from_miner_key == 'all_miner'
                            )
                            or any(
                                _ == from_miner_key for _ in self.from_miners
                            )
                        )
                        and (
                            to_miner_key is None
                            or not self.to_miners
                            and (
                                not all_to_miner or to_miner_key == 'all_miner'
                            )
                            or any(_ == to_miner_key for _ in self.to_miners)
                        )
                        and (
                            (market_key is None or not self.markets)
                            or any(_ == market_key for _ in self.markets)
                        )
                        and (
                            exchange_key is None
                            or not self.exchanges
                            and (
                                not all_exchange
                                or exchange_key == 'all_exchange'
                            )
                            or any(_ == exchange_key for _ in self.exchanges)
                        )
                        and (
                            from_exchange_key is None
                            or not self.from_exchanges
                            and (
                                not all_from_exchange
                                or from_exchange_key == 'all_exchange'
                            )
                            or any(
                                _ == from_exchange_key
                                for _ in self.from_exchanges
                            )
                        )
                        and (
                            to_exchange_key is None
                            or not self.to_exchanges
                            and (
                                not all_to_exchange
                                or to_exchange_key
                                in {'spot_exchange', 'derivative_exchange'}
                            )
                            or any(
                                _ == from_exchange_key
                                for _ in self.to_exchanges
                            )
                        )
                        and (
                            (from_bank_key is None or not self.from_banks)
                            or any(_ == from_bank_key for _ in self.from_banks)
                        )
                        and (
                            symbol_key is None
                            or not self.symbols
                            and (not all_symbol or symbol_key == 'all_symbol')
                            or any(_ == symbol_key for _ in self.symbols)
                        )
                    ):
                        await chart_sender.send(
                            (
                                asset_id,
                                category_path,
                                asset_metric.id,
                                chart,
                            )
                        )
                    else:
                        fetch_logger.info(
                            '%sSkipped processing chart `%s` for metric `%s`!',
                            '[%s] ' % index if index is not None else '',
                            chart.get('id'),
                            asset_metric.id,
                        )

    async def fetch_chart(
        self: Self,
        chart_receiver: MemoryObjectReceiveStream[
            Tuple[str, str, str, Dict[str, object]]
        ],
        sender: MemoryObjectSendStream[
            Tuple[str, str, str, str, Dict[int, Iterable[Decimal]]]
        ],
        /,
        index: int,
    ) -> None:
        async with chart_receiver, sender:
            async for (
                asset_id,
                category_path,
                asset_metric_id,
                chart,
            ) in chart_receiver:
                chart = await parse_chart(
                    self._Session,
                    asset_metric_id,
                    chart,
                    index=index,
                )
                await self._Session.remove()
                if chart is None:
                    break

                while (
                    values := await fetch_chart(
                        self._session, chart.id, index=index
                    )
                ) is None:
                    await self._auth.run()
                    await self._Session.remove()
                await sender.send(
                    (
                        asset_id,
                        category_path,
                        asset_metric_id,
                        chart.id,
                        values,
                    )
                )

    async def fetch_chart_values(
        self: Self,
        receiver: MemoryObjectReceiveStream[
            Tuple[str, str, str, str, Dict[int, Iterable[Decimal]]]
        ],
        /,
        index: int,
    ) -> None:
        async with receiver:
            async for (
                asset_id,
                category_path,
                asset_metric_id,
                chart_id,
                values,
            ) in receiver:
                while (
                    values := await parse_chart_value(
                        self._Session, chart_id, values, index=index
                    )
                ) is None:
                    await self._auth.run()
                    await self._Session.remove()
