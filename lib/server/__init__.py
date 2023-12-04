from logging import getLogger

from aiohttp.web import Request, Response, Server, ServerRunner, TCPSite

#
logger = getLogger('Server')


async def server(port: int, /) -> None:
    async def handler(request: Request, /) -> Response:
        return Response(text='OK')

    await (runner := ServerRunner(Server(handler))).setup()
    await TCPSite(
        runner, port=port, reuse_address=True, reuse_port=True
    ).start()
    logger.info('Serving on http://127.0.0.1:%s/', port)
