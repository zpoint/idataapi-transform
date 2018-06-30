import json
import logging
from .BaseWriter import BaseWriter


class MySQLWriter(BaseWriter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.total_miss_count = 0
        self.success_count = 0
        self.table_check = False

    async def write(self, responses):
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
        if not self.table_check:
            await self.table_check(responses)

        await self.perform_write(responses)
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
