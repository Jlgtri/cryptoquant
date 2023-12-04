from logging import getLogger
from typing import Optional, Tuple

from aiohttp import ClientSession
from anyio.to_thread import run_sync
from orjson import loads

#
logger = getLogger('Fetch')


async def fetch_me(
    session: ClientSession,
    /,
    *,
    index: Optional[int] = None,
) -> Optional[str]:
    logger.info(
        '%sFetching current user...',
        '[%s] ' % index if index is not None else '',
    )
    async with session.get('/api/v1/users/me') as resp:
        data = await resp.read()
    if not resp.ok:
        return logger.exception(
            '%sException while fetching current user: %s',
            '[%s] ' % index if index is not None else '',
            await run_sync(data.decode, resp.get_encoding()),
        )
    return (await run_sync(loads, data)).get('email')


async def fetch_sign_in(
    session: ClientSession,
    /,
    username: str,
    password: str,
    *,
    index: Optional[int] = None,
) -> Tuple[Optional[str], Optional[str]]:
    logger.info(
        '%sSiginig in with username `%s`...',
        '[%s] ' % index if index is not None else '',
        username,
    )
    async with session.post(
        '/api/v1/sign-in',
        json={'email': username, 'password': password, 'stayLoggedIn': True},
    ) as resp:
        data = await resp.read()
    if not resp.ok:
        logger.exception(
            '%sException while signing in: %s',
            '[%s] ' % index if index is not None else '',
            await run_sync(data.decode, resp.get_encoding()),
        )
        return None, None
    data = await run_sync(loads, data)
    return data['accessToken'], data['refreshToken']


async def fetch_sign_out(
    session: ClientSession,
    /,
    refresh_token: str,
    *,
    index: Optional[int] = None,
) -> bool:
    logger.info(
        '%sReissuing authorization token...',
        '[%s] ' % index if index is not None else '',
    )
    async with session.post(
        '/api/v1/logout',
        json={'refreshToken': refresh_token},
    ) as resp:
        data = await resp.read()
    if not resp.ok:
        logger.exception(
            '%sException while reissuing authorization token: %s',
            '[%s] ' % index if index is not None else '',
            await run_sync(data.decode, resp.get_encoding()),
        )
    return resp.ok


async def fetch_token(
    session: ClientSession,
    /,
    access_token: str,
    refresh_token: str,
    *,
    index: Optional[int] = None,
) -> Tuple[Optional[str], Optional[str]]:
    logger.info(
        '%sReissuing authorization token...',
        '[%s] ' % index if index is not None else '',
    )
    async with session.post(
        '/api/v1/token/reissue',
        json={'accessToken': access_token, 'refreshToken': refresh_token},
    ) as resp:
        data = await resp.read()
    if not resp.ok:
        logger.exception(
            '%sException while reissuing authorization token: %s',
            '[%s] ' % index if index is not None else '',
            await run_sync(data.decode, resp.get_encoding()),
        )
        return None, None
    data = await run_sync(loads, data)
    return data['accessToken'], data['refreshToken']
