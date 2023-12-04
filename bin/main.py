from asyncio import current_task
from logging import basicConfig, getLogger
from os import name as os_name
from sys import path
from typing import Dict, Iterable, Optional

from aiohttp import ClientSession, TCPConnector
from asyncclick import FloatRange, IntRange, group, option
from sqlalchemy.ext.asyncio.engine import create_async_engine
from sqlalchemy.ext.asyncio.scoping import async_scoped_session
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.pool.impl import AsyncAdaptedQueuePool
from sqlalchemy.sql.ddl import DDL


@group(
    name='group',
    chain=True,
    invoke_without_command=True,
    context_settings=dict(token_normalize_func=lambda x: x.lower()),
)
@option(
    '-l',
    '--logging',
    help='The *logging* level to use.',
    default='INFO',
    required=True,
)
@option(
    '-d',
    '--database-url',
    help='The *database_url* to use with SQLAlchemy.',
    required=True,
    envvar=['CRYPTOQUANT_DATABASE_URL', 'DATABASE_URL'],
    callback=lambda ctx, param, value: 'postgresql+asyncpg://'
    + value.split('://')[-1].split('?')[0].strip()
    if value
    else None,
)
@option('-u', '--username', help='The *username* to use with session.')
@option('-p', '--password', help='The *password* to use with session.')
@option(
    '-h',
    '--headers',
    help='The *headers* to be used with session.',
    type=(str, str),
    multiple=True,
    callback=lambda ctx, param, value: dict(value),
)
@option(
    '--period',
    type=FloatRange(min=0, min_open=True),
    help='The *period* in seconds for loading the chart.',
)
@option(
    '--port',
    help='The *port* used for listening on HTTP requests.',
    type=IntRange(min=0, min_open=True, max=2**16, max_open=True),
)
async def cli(**kwargs: object) -> None:
    pass


@cli.command(name='config')
@option(
    '-a',
    '--asset',
    help='The id of the *asset* to load.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-c',
    '--category',
    help='The id of the *category* to load from asset.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-m',
    '--metric',
    help='The id of the *metric* to load from category.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-r',
    '--miner',
    help='The id of the *miner* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-fr',
    '--from-miner',
    help='The id of the *from miner* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-tr',
    '--to-miner',
    help='The id of the *to miner* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-t',
    '--market',
    help='The id of the *market* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-e',
    '--exchange',
    help='The id of the *exchange* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-fe',
    '--from-exchange',
    help='The id of the *from exchange* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-te',
    '--to-exchange',
    help='The id of the *to exchange* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-fb',
    '--from-bank',
    help='The id of the *from bank* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-s',
    '--symbol',
    help='The id of the *symbol* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '-f',
    '--field',
    help='The id of the *field* to load from chart.',
    multiple=True,
    callback=lambda ctx, param, value: tuple(_.lower() for _ in value),
)
@option(
    '--predict/--no-predict',
    help='Whether this chart is for *prediction*.',
)
@option(
    '--force/--no-force',
    help='Whether to load the data before processing even if it exists.',
)
async def chain(**kwargs: object) -> None:
    return kwargs


@cli.result_callback()
async def main(
    configs: Iterable[Dict[str, object]],
    /,
    logging: str,
    database_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    period: Optional[float] = None,
    port: Optional[int] = None,
):
    basicConfig(level=logging, force=True)
    getLogger('sqlalchemy.engine.Engine').propagate = False
    Session = async_scoped_session(
        sessionmaker(
            engine := create_async_engine(
                echo=False,
                url=database_url,
                poolclass=AsyncAdaptedQueuePool,
                pool_size=20,
                max_overflow=0,
                pool_recycle=3600,
                pool_pre_ping=True,
                pool_use_lifo=True,
                connect_args=dict(ssl=False, server_settings=dict(jit='off')),
                # execution_options=dict(isolation_level='SERIALIZABLE'),
            ),
            class_=AsyncSession,
            expire_on_commit=False,
            future=True,
        ),
        scopefunc=current_task,
    )

    try:
        async with engine.begin() as connection:
            for schema in {
                table.schema
                for table in Base.metadata.tables.values()
                if table.schema
            }:
                await connection.execute(
                    DDL(f'CREATE SCHEMA IF NOT EXISTS {schema}')
                )
            await connection.run_sync(Base.metadata.create_all)

        async with ClientSession(
            'https://live-api.cryptoquant.com',
            headers=headers,
            connector=TCPConnector(limit=0, ssl=False, ttl_dns_cache=600),
        ) as session:
            auth = CryptoQuantAuthorization(
                Session, username, password, session
            )
            charts = [
                CryptoQuantChart(
                    Session,
                    assets=config.get('asset'),
                    categories=config.get('category'),
                    metrics=config.get('metric'),
                    miners=config.get('miner'),
                    from_miners=config.get('from_miner'),
                    to_miners=config.get('to_miner'),
                    markets=config.get('market'),
                    exchanges=config.get('exchange'),
                    from_exchanges=config.get('from_exchange'),
                    to_exchanges=config.get('to_exchange'),
                    from_banks=config.get('from_bank'),
                    symbols=config.get('symbol'),
                    fields=config.get('field'),
                    predict=config.get('predict'),
                    force=config.get('force'),
                    session=session,
                    auth=auth,
                )
                for config in configs
            ]
            if all(not chart.predict for chart in charts):
                raise RuntimeError(
                    'At least one chart should be for prediction.'
                )
            await CryptoQuant(port=port, chart=charts).run()
    finally:
        await engine.dispose()


if __name__ == '__main__':
    path.append('..')
    from lib import CryptoQuant
    from lib.authorization import CryptoQuantAuthorization
    from lib.chart import CryptoQuantChart
    from lib.models.base import Base

    cli(
        auto_envvar_prefix='CRYPTOQUANT',
        _anyio_backend_options=dict(use_uvloop=os_name != 'nt'),
    )
