import logging
import asyncio
import random
import traceback
import json
import zlib
from .BaseWriter import BaseWriter


class RedisWriter(BaseWriter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.total_miss_count = 0
        self.success_count = 0

    def encode(self, dict_object):
        string = json.dumps(dict_object)
        if self.config.compress:
            string = zlib.compress(string.encode(self.config.encoding))
        return string

    async def write(self, responses):
        await self.config.get_redis_pool_cli()  # init redis pool
        miss_count = 0
        target_responses = list()
        for each_response in responses:
            if self.config.filter:
                each_response = self.config.filter(each_response)
                if not each_response:
                    miss_count += 1
                    continue
            target_responses.append(each_response)
            self.success_count += 1
        self.total_miss_count += miss_count
        if target_responses:
            try_time = 0
            while try_time < self.config.max_retry:
                try:
                    if self.config.is_range:
                        await self.config.redis_write_method(self.config.key, *(self.encode(i) for i in target_responses))
                    else:
                        pipe_line = self.config.redis_pool_cli.pipeline()
                        for each in responses:
                            pipe_line.hset(self.config.key, each["id"], self.encode(each))
                        await pipe_line.execute()

                    logging.info("%s write %d item, filtered %d item" % (self.config.name, len(responses), miss_count))
                    break
                except Exception as e:
                    try_time += 1
                    if try_time >= self.config.max_retry:
                        logging.error("Fail to write after try: %d times, Write 0 items to redis, "
                                      "filtered %d item before write, error: %s" %
                                      (self.config.max_retry, miss_count, str(traceback.format_exc())))
                    else:
                        await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
        else:
            logging.info("Write 0 items to %s, filtered: %d, (all filtered, or pass empty result)" % (self.config.name, miss_count))

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info("%s write done, total filtered %d item, total write %d item" %
                     (self.config.name, self.total_miss_count, self.success_count))

    def __enter__(self):
        return self
