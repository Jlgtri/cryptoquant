from dataclasses import dataclass
from logging import Logger, getLogger
from typing import Final, Optional, Self, Union

from aiohttp import ClientSession
from aiohttp.hdrs import AUTHORIZATION
from anyio.to_thread import run_sync
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.ext.asyncio.scoping import async_scoped_session
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm.session import sessionmaker

from ..models.service.authorizations.authorization import Authorization
from ..utils.get_bind import Bind, get_bind
from .fetch import fetch_me, fetch_sign_in, fetch_token


@dataclass(init=False, frozen=True)
class CryptoQuantAuthorization(object):
    username: Final[Optional[str]]
    password: Final[Optional[str]]

    _logger: Final[Logger]
    _session: Final[ClientSession]
    _engine: Final[AsyncEngine]
    _Session: Final[
        Union[
            sessionmaker[AsyncSession],
            async_scoped_session[AsyncSession],
        ]
    ]

    def __init__(
        self: Self,
        /,
        bind: Bind,
        username: Optional[str] = None,
        password: Optional[str] = None,
        session: Optional[ClientSession] = None,
        *,
        logger_name: Optional[str] = None,
    ) -> None:
        engine, Session = get_bind(bind)
        object.__setattr__(self, 'username', username)
        object.__setattr__(self, 'password', password)
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

    async def run(self: Self, /) -> None:
        # 1. Validate tokens provided with headers.
        # 2. Validate tokens provided with parameters.
        # 3. Validate tokens provided with Session.
        # 4. Validate tokens provided by user input.
        fetched_headers = False
        fetched_parameters = False
        fetched_auth = False
        while True:
            access, refresh = None, None
            if not fetched_headers:
                fetched_headers = True
                access = self._session.headers.get(AUTHORIZATION)
            elif not fetched_parameters:
                fetched_parameters = True
                if self.username and self.password:
                    access, refresh = await fetch_sign_in(
                        self._session, self.username, self.password
                    )
            elif not fetched_auth:
                fetched_auth = True
                if self.username:
                    auth = await self._Session.get(
                        Authorization, self.username
                    )
                    if auth is not None:
                        access, refresh = auth.access_token, auth.refresh_token
            else:
                access, refresh = await fetch_sign_in(
                    self._session,
                    await run_sync(input, 'Please provide a username: '),
                    await run_sync(input, 'Please provide a password: '),
                )
                if not access:
                    print('Incorrect credentials. Please try again.')
            access = (access or '').removeprefix('Bearer ')
            refresh = (refresh or '').removeprefix('Bearer ')
            if not access:
                continue
            self._session.headers[AUTHORIZATION] = 'Bearer ' + access
            if (username := await fetch_me(self._session)) is not None:
                break
            if not refresh:
                continue
            access, refresh = await fetch_token(self._session, access, refresh)
            if (username := await fetch_me(self._session)) is not None:
                break

        if refresh is not None:
            token = await self._Session.merge(
                Authorization(
                    username=username,
                    access_token=access,
                    refresh_token=refresh,
                )
            )
            await self._Session.commit()
