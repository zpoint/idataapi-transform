import aiohttp
import asyncio
import inspect
from .MainConfig import main_config


class _SessionManger(object):
    def __init__(self, concurrency_limit=None, loop=None):
        concurrency_limit = main_config()["main"].getint("concurrency") if concurrency_limit is None else concurrency_limit
        self.session = self._generate_session(concurrency_limit=concurrency_limit, loop=loop)

    @staticmethod
    def _generate_connector(limit=None, loop=None):
        """
        https://github.com/KeepSafe/aiohttp/issues/883
        if connector is passed to session, it is not available anymore
        """
        limit = main_config()["main"].getint("concurrency") if limit is None else limit
        if not loop:
            loop = asyncio.get_event_loop()
        return aiohttp.TCPConnector(limit=limit, loop=loop)

    @staticmethod
    def _generate_session(concurrency_limit=None, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()
        concurrency_limit = main_config()["main"].getint("concurrency") if concurrency_limit is None else concurrency_limit
        return aiohttp.ClientSession(connector=_SessionManger._generate_connector(limit=concurrency_limit, loop=loop),
                                     loop=loop)

    def get_session(self):
        return self.session

    def __del__(self):
        try:
            if inspect.iscoroutinefunction(self.session.close):
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.session.close())
            else:
                self.session.close()
        except Exception as e:
            pass


session_manger = _SessionManger()
