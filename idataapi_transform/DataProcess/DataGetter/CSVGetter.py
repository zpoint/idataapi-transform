import csv
import sys
import logging
from .BaseGetter import BaseGetter

if sys.platform == "linux":
    csv.field_size_limit(sys.maxsize)


class CSVGetter(BaseGetter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.f_in = open(self.config.filename, self.config.mode, encoding=self.config.encoding)
        self.reader = csv.DictReader(self.f_in)

        self.done = False
        self.responses = list()
        self.miss_count = 0
        self.total_count = 0

    def init_val(self):
        self.done = False
        self.responses = list()
        self.f_in.seek(0, 0)
        self.miss_count = 0
        self.total_count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.done:
            logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                         (self.config.filename, self.total_count, self.miss_count))
            self.init_val()
            raise StopAsyncIteration

        for row in self.reader:
            if self.config.max_limit and self.total_count > self.config.max_limit:
                self.done = True
                return self.clear_and_return()

            self.total_count += 1
            if self.config.filter:
                row = self.config.filter(row)
            if not row:
                self.miss_count += 1
                continue

            self.responses.append(row)
            if len(self.responses) > self.config.per_limit:
                return self.clear_and_return()

        if self.responses:
            self.done = True
            return self.clear_and_return()

        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.filename, self.total_count, self.miss_count))
        self.init_val()
        raise StopAsyncIteration

    def __iter__(self):
        for row in self.reader:
            if self.config.max_limit and self.total_count > self.config.max_limit:
                self.done = True
                yield self.clear_and_return()
                break

            self.total_count += 1
            if self.config.filter:
                row = self.config.filter(row)
            if not row:
                self.miss_count += 1
                continue

            self.responses.append(row)
            if len(self.responses) > self.config.per_limit:
                yield self.clear_and_return()

        if self.responses:
            yield self.responses

        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.filename, self.total_count, self.miss_count))
        self.init_val()

    def clear_and_return(self):
        resp = self.responses
        self.responses = list()
        return resp
