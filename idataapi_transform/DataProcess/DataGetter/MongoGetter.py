import asyncio
import traceback
import random
import logging
from .BaseGetter import BaseGetter


class MongoGetter(BaseGetter):
    def __init__(self, config):
        super().__init__(self)
        self.config = config
        self.responses = list()
        self.miss_count = 0
        self.total_count = 0
        self.total_size = None
        self.need_finish = False

    def init_val(self):
        self.responses = list()
        self.miss_count = 0
        self.total_count = 0
        self.total_size = None
        self.need_finish = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.config.get_mongo_cli()  # init mongo pool

        if self.need_finish:
            await self.finish()

        if self.total_size is None:
            self.total_size = await self.get_total_size()

        if self.total_count < self.total_size:
            await self.fetch_per_limit()
            return self.clear_and_return()

        # reach here, means done
        await self.finish()

    def __iter__(self):
        raise ValueError("MongoGetter must be used with async generator, not normal generator")

    async def finish(self):
        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.name, self.total_count, self.miss_count))
        self.init_val()
        raise StopAsyncIteration

    async def get_total_size(self):
        if hasattr(self.config.cursor, "count"):
            size = await self.config.cursor.count()
        else:
            size = await self.config.client[self.config.database][self.config.collection].count_documents({} if not self.config.query_body else self.config.query_body)
        size = min(size, self.config.max_limit if self.config.max_limit is not None else size)
        if size == 0:
            await self.finish()
        return size

    async def fetch_per_limit(self):
        curr_size = 0
        try_time = 0
        get_all = True

        while try_time < self.config.max_retry:
            try:
                async for document in self.config.cursor:
                    curr_size += 1
                    self.responses.append(document)
                    if curr_size >= self.config.per_limit:
                        get_all = False
                        break
                if get_all:
                    # get all item
                    if self.total_count + curr_size < self.total_size:
                        logging.error("get all items: %d, but not reach 'total_size': %d" % (self.total_count + curr_size, self.total_size))
                        self.need_finish = True
                break
            except Exception as e:
                try_time += 1
                if try_time < self.config.max_retry:
                    logging.error("retry: %d, %s" % (try_time, str(e)))
                    await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                else:
                    logging.error("Give up MongoGetter getter: %s, After retry: %d times, still fail, "
                                  "total get %d items, total filtered: %d items, reason: %s" %
                                  (self.config.name, self.config.max_retry, self.total_count, self.miss_count,
                                   str(traceback.format_exc())))
                    self.need_finish = True

        self.total_count += len(self.responses)

        curr_miss_count = 0
        if self.config.filter:
            target_results = list()
            for each in self.responses:
                each = self.config.filter(each)
                if each:
                    target_results.append(each)
                else:
                    curr_miss_count += 1
            self.responses = target_results
            self.miss_count += curr_miss_count

        logging.info("Get %d items from %s, filtered: %d items, percentage: %.2f%%" %
                     (len(self.responses), self.config.name, curr_miss_count,
                      (self.total_count / self.total_size * 100) if self.total_size else 0))

    def clear_and_return(self):
        resp = self.responses
        self.responses = list()
        return resp
