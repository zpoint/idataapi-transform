import json
import asyncio
import traceback
import random
import logging
from .BaseGetter import BaseGetter


class MySQLGetter(BaseGetter):
    def __init__(self, config):
        super().__init__(self)
        self.config = config
        self.responses = list()
        self.miss_count = 0
        self.total_count = 0
        self.total_size = None
        self.key_fields = list()
        self.key_fields_map = dict()
        self.need_finish = False

    def init_val(self):
        self.responses = list()
        self.miss_count = 0
        self.total_count = 0
        self.total_size = None
        self.key_fields = list()
        self.key_fields_map = dict()
        self.need_finish = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self.config.get_mysql_pool_cli()  # init mysql pool

        if self.need_finish:
            await self.finish()

        if self.total_size is None:
            self.total_size, self.key_fields = await self.get_total_size_and_key_field()

        if self.total_count < self.total_size:
            await self.fetch_per_limit()
            return self.clear_and_return()

        # reach here, means done
        await self.finish()

    def __iter__(self):
        raise ValueError("MySQLGetter must be used with async generator, not normal generator")

    async def finish(self):
        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.name, self.total_count, self.miss_count))
        self.init_val()
        self.config.free_resource()
        raise StopAsyncIteration

    async def get_total_size_and_key_field(self):
        await self.config.cursor.execute("DESC %s" % (self.config.table, ))
        result = await self.config.cursor.fetchall()
        field = result[0][0]
        await self.config.cursor.execute("select count(%s) from %s" % (field, self.config.table))
        result = await self.config.cursor.fetchone()
        # key field
        await self.config.cursor.execute("DESC %s" % (self.config.table, ))
        results = await self.config.cursor.fetchall()
        key_fields = list()
        for each in results:
            key_fields.append(each[0])
            if "tinyint" in each[1]:
                self.key_fields_map[each[0]] = bool
            elif "text" in each[1]:
                self.key_fields_map[each[0]] = str  # or json

        key_fields = list(i[0] for i in results)
        return result[0], key_fields

    async def fetch_per_limit(self):
        results = list()
        try_time = 0
        while try_time < self.config.max_retry:
            try:
                await self.config.cursor.execute("SELECT * FROM %s LIMIT %d,%d" %
                                                 (self.config.table, self.total_count,
                                                  self.total_count + self.config.per_limit))
                results = await self.config.cursor.fetchall()
                break
            except Exception as e:
                try_time += 1
                if try_time < self.config.max_retry:
                    logging.error("retry: %d, %s" % (try_time, str(e)))
                    await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                else:
                    logging.error("Give up MySQL getter: %s, After retry: %d times, still fail, "
                                  "total get %d items, total filtered: %d items, reason: %s" %
                                  (self.config.name, self.config.max_retry, self.total_count, self.miss_count,
                                   str(traceback.format_exc())))
                    self.need_finish = True

        self.responses = [self.decode(i) for i in results]
        curr_miss_count = 0
        if self.config.filter:
            target_results = list()
            for each in results:
                each = self.config.filter(each)
                if each:
                    target_results.append(each)
                else:
                    curr_miss_count += 1
            self.responses = target_results
            self.miss_count += curr_miss_count

        self.total_count += len(results)
        logging.info("Get %d items from %s, filtered: %d items, percentage: %.2f%%" %
                     (len(results), self.config.name, curr_miss_count,
                      (self.total_count / self.total_size * 100) if self.total_size else 0))

    def decode(self, item):
        """
        :param item: tuple
        :return: dict
        """
        ret_dict = dict()
        index = 0
        for key in self.key_fields:
            if key in self.key_fields_map:
                if self.key_fields_map[key] is bool:
                    ret_dict[key] = bool(item[index])
                elif item[index] is None:
                    ret_dict[key] = None
                elif item[index][0] in ("{", "["):
                    try:
                        val = json.loads(item[index])
                    except json.decoder.JSONDecodeError:
                        val = item[index]
                    ret_dict[key] = val
                else:
                    ret_dict[key] = item[index]
            else:
                ret_dict[key] = item[index]
            index += 1
        return ret_dict

    def clear_and_return(self):
        resp = self.responses
        self.responses = list()
        return resp
