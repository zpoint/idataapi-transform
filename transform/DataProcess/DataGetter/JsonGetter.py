import json
import logging
from .BaseGetter import BaseGetter


class JsonGetter(BaseGetter):
    def __init__(self, config):
        super().__init__(self)
        self.config = config
        self.responses = list()
        self.done = False
        self.f_in = open(self.config.filename, self.config.mode, encoding=self.config.encoding)
        self.miss_count = 0
        self.total_count = 0

    def init_val(self):
        self.responses = list()
        self.done = False
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

        for line in self.f_in:
            if self.config.max_limit and self.total_count > self.config.max_limit:
                self.done = True
                return self.clear_and_return()

            self.total_count += 1
            try:
                json_obj = json.loads(line)
            except json.decoder.JSONDecodeError:
                logging.error("JSONDecodeError. give up. line: %d" % (self.total_count, ))
                continue

            if self.config.filter:
                json_obj = self.config.filter(json_obj)
                if not json_obj:
                    self.miss_count += 1
                    continue

            self.responses.append(json_obj)

            if len(self.responses) > self.config.per_limit:
                return self.clear_and_return()

        self.done = True
        if self.responses:
            return self.clear_and_return()

        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.filename, self.total_count, self.miss_count))
        self.init_val()
        raise StopAsyncIteration

    def __iter__(self):
        for line in self.f_in:
            if self.config.max_limit and self.total_count > self.config.max_limit:
                self.done = True
                yield self.clear_and_return()
                break

            self.total_count += 1
            try:
                json_obj = json.loads(line)
            except json.decoder.JSONDecodeError:
                logging.error("JSONDecodeError. give up. line: %d" % (self.total_count, ))
                continue

            if self.config.filter:
                json_obj = self.config.filter(json_obj)
                if not json_obj:
                    self.miss_count += 1
                    continue

            self.responses.append(json_obj)

            if len(self.responses) > self.config.per_limit:
                yield self.clear_and_return()

        if self.responses:
            yield self.clear_and_return()

        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.filename, self.total_count, self.miss_count))
        self.init_val()

    def __del__(self):
        self.f_in.close()

    def clear_and_return(self):
        resp = self.responses
        self.responses = list()
        return resp
