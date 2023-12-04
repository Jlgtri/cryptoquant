from decimal import Decimal
from logging import getLogger
from time import time
from typing import Dict, Iterable, Optional, Tuple

from aiohttp import ClientSession
from anyio.to_thread import run_sync
from orjson import loads
from simplejson import loads as sloads

#
logger = getLogger('Fetch')


async def fetch_assets(
    session: ClientSession,
    /,
    *,
    index: Optional[int] = None,
) -> Optional[Dict[str, Dict[str, str]]]:
    logger.info(
        '%sFetching assets...',
        '[%s] ' % index if index is not None else '',
    )
    async with session.get('/api/v2/assets') as resp:
        data = await resp.read()
    if not resp.ok:
        return logger.exception(
            '%sException while fetching assets: %s',
            '[%s] ' % index if index is not None else '',
            await run_sync(data.decode, resp.get_encoding()),
        )
    return {asset['path']: asset for asset in await run_sync(loads, data)}


async def fetch_metrics(
    session: ClientSession,
    /,
    asset_id: str,
    *,
    index: Optional[int] = None,
) -> Optional[Dict[str, Dict[str, object]]]:
    logger.info(
        '%sFetching metrics for asset `%s`...',
        '[%s] ' % index if index is not None else '',
        asset_id,
    )
    async with session.get(f'/api/v2/assets/{asset_id}/metrics') as resp:
        data = await resp.read()
    if not resp.ok:
        return logger.exception(
            '%sException while fetching metrics for asset `%s`: \n\n%s',
            '[%s] ' % index if index is not None else '',
            asset_id,
            await run_sync(data.decode, resp.get_encoding()),
        )
    return {
        item['category']['path']: item['category']
        for item in await run_sync(loads, data)
    }


async def fetch_charts(
    session: ClientSession,
    /,
    metric_id: str,
    *,
    index: Optional[int] = None,
) -> Optional[Dict[Tuple[str, Optional[str]], Dict[str, object]]]:
    logger.info(
        '%sFetching charts for metric `%s`...',
        '[%s] ' % index if index is not None else '',
        metric_id,
    )
    async with session.get(f'/api/v3/metrics/{metric_id}/charts') as resp:
        data = await resp.read()
    if not resp.ok:
        return logger.exception(
            '%sException while fetching charts for metric `%s`: %s',
            '[%s] ' % index if index is not None else '',
            metric_id,
            await run_sync(data.decode, resp.get_encoding()),
        )
    return await run_sync(loads, data)


async def fetch_chart(
    session: ClientSession,
    /,
    chart_id: str,
    *,
    index: Optional[int] = None,
) -> Optional[Dict[int, Iterable[Decimal]]]:
    logger.info(
        '%sFetching chart `%s`...',
        '[%s] ' % index if index is not None else '',
        chart_id,
    )
    async with session.get(
        f'/api/v3/charts/{chart_id}',
        params={
            'window': 'DAY',
            'from': 1053205200000,
            'to': (time() * 1000).__ceil__(),
            'limit': 70000,
        },
    ) as resp:
        data = await resp.read()
    text = await run_sync(data.decode, resp.get_encoding())
    if not resp.ok:
        return logger.exception(
            '%sException while fetching chart `%s`: %s',
            '[%s] ' % index if index is not None else '',
            chart_id,
            text,
        )
    data = await run_sync(sloads, text, None, None, None, Decimal)
    return await run_sync(
        lambda data: {k: v for k, *v in data},
        data['result']['data'],
    )
