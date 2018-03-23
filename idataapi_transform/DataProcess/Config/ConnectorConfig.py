import aiohttp
import asyncio
import inspect
from .MainConfig import main_config

main_config = main_config()
default_concurrency_limit = main_config["main"].getint("concurrency")


class _SessionManger(object):
    def __init__(self, concurrency_limit=default_concurrency_limit, loop=None):
        self.session = self._generate_session(concurrency_limit=concurrency_limit, loop=loop)

    @staticmethod
    def _generate_connector(limit=default_concurrency_limit, loop=None):
        """
        https://github.com/KeepSafe/aiohttp/issues/883
        if connector is passed to session, it is not available anymore
        """
        if not loop:
            loop = asyncio.get_event_loop()
        return aiohttp.TCPConnector(limit=limit, loop=loop)

    @staticmethod
    def _generate_session(concurrency_limit=default_concurrency_limit, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()
        return aiohttp.ClientSession(connector=_SessionManger._generate_connector(limit=concurrency_limit, loop=loop),
                                     loop=loop)

    def get_session(self):
        return self.session

    def __del__(self):
        if inspect.iscoroutinefunction(self.session.close):
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.session.close())
        else:
            self.session.close()


session_manger = _SessionManger()
