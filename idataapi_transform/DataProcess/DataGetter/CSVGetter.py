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
        self.need_clear = False
        self.curr_size = 0
        self.responses = list()

    def init_val(self):
        self.done = False
        self.need_clear = False
        self.curr_size = 0
        self.responses = list()
        self.f_in.seek(0, 0)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.need_clear:
            self.responses.clear()
            self.need_clear = False

        if self.done:
            self.init_val()
            logging.info("get source done: %s" % (self.config.filename, ))
            raise StopAsyncIteration

        for row in self.reader:
            self.responses.append(row)
            if len(self.responses) > self.config.per_limit:
                self.curr_size += len(self.responses)
                self.need_clear = True
                return self.responses

            if self.config.max_limit and self.curr_size > self.config.max_limit:
                self.done = self.need_clear = True
                return self.responses

        if self.responses:
            self.done = self.need_clear = True
            return self.responses

        self.init_val()
        logging.info("get source done: %s" % (self.config.filename,))
        raise StopAsyncIteration

    def __iter__(self):
        for row in self.reader:
            self.responses.append(row)
            if len(self.responses) > self.config.per_limit:
                self.curr_size += len(self.responses)
                yield self.responses
                self.responses.clear()

            if self.config.max_limit and self.curr_size > self.config.max_limit:
                yield self.responses
                self.responses.clear()
                break

        if self.responses:
            yield self.responses

        self.init_val()
        logging.info("get source done: %s" % (self.config.filename,))
