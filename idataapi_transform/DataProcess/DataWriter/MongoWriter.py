import json
import asyncio
import random
import logging
import traceback
from .BaseWriter import BaseWriter


class MongoWriter(BaseWriter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.total_miss_count = 0
        self.success_count = 0
        self.table_checked = False
        self.key_fields = list()

    async def write(self, responses):
        self.config.get_mongo_cli()  # init mysql pool

        miss_count = 0
        original_length = len(responses)
        if self.config.filter:
            target_responses = list()
            for i in responses:
                i = self.config.filter(i)
                if i:
                    target_responses.append(i)
                else:
                    miss_count += 1
            responses = target_responses

        if not responses:
            self.finish_once(miss_count, original_length)
            return

        # After filtered, still have responses to write
        if await self.perform_write(responses):
            self.finish_once(miss_count, original_length)

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info("%s write done, total filtered %d item, total write %d item" %
                     (self.config.name, self.total_miss_count, self.success_count))

    def __enter__(self):
        return self

    def finish_once(self, miss_count, original_length):
        self.total_miss_count += miss_count
        self.success_count += original_length
        logging.info("%s write %d item, filtered %d item" % (self.config.name, original_length - miss_count, miss_count))

    async def perform_write(self, responses):
        try_time = 0
        while try_time < self.config.max_retry:
            try:
                bulk = self.config.collection_cli.initialize_ordered_bulk_op()
                for each in responses:
                    each["_id"] = self.config.id_hash_func(each)
                    bulk.find({"_id": each["_id"]}).upsert().replace_one(each)
                await bulk.execute()
                return True
            except Exception as e:
                try_time += 1
                if try_time < self.config.max_retry:
                    logging.error("retry: %d, %s" % (try_time, str(e)))
                    await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                else:
                    logging.error("Give up MongoWriter writer: %s, After retry: %d times, still fail to write, "
                                  "total write %d items, total filtered: %d items, reason: %s" %
                                  (self.config.name, self.config.max_retry, self.success_count, self.total_miss_count,
                                   str(traceback.format_exc())))
        return False
