import json
import logging
from .BaseGetter import BaseGetter


class MySQLGetter(BaseGetter):
    def __init__(self, config):
        super().__init__(self)
        self.config = config
        self.responses = list()
        self.need_clear = False
        self.done = False
        self.miss_count = 0
        self.total_count = 0
        self.total_size = None

    def init_val(self):
        self.responses = list()
        self.need_clear = False
        self.done = False
        self.miss_count = 0
        self.total_count = 0
        self.total_size = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.total_size is None:
            self.total_size = await self.get_total_size()

        if self.need_clear:
            self.responses.clear()
            self.need_clear = False

        if self.done:
            self.finish()

        if self.total_count < self.total_size:
            self.need_clear = True
            return await self.fetch_per_limit()

        # reach here, means done
        self.finish()

    def __iter__(self):
        raise ValueError("MySQLGetter must be used with async generator, not normal generator")

    def finish(self):
        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.name, self.total_count, self.miss_count))
        self.init_val()
        raise StopAsyncIteration
