import re
import json
import random
import logging
import aiohttp
import asyncio
from .BaseGetter import BaseGetter


class APIGetter(BaseGetter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.base_url = self.config.source
        self.retry_count = 0
        self.curr_size = 0
        self.responses = list()
        self.done = False
        self.need_clear = False
        self.page_token = ""

    def init_val(self):
        self.base_url = self.config.source
        self.retry_count = 0
        self.curr_size = 0
        self.responses = list()
        self.done = False
        self.need_clear = False
        self.page_token = ""

    def generate_sub_func(self):
        def sub_func(match):

            return match.group(1) + self.page_token + match.group(3)
        return sub_func

    def update_base_url(self, key="pageToken"):
        if self.base_url[-1] == "/":
            self.base_url = self.base_url[:-1]
        elif self.base_url[-1] == "?":
            self.base_url = self.base_url[:-1]

        key += "="
        if key not in self.base_url:
            if "?" not in self.base_url:
                self.base_url = self.base_url + "?" + key + self.page_token
            else:
                self.base_url = self.base_url + "&" + key + self.page_token
        else:
            self.base_url = re.sub("(" + key + ")(.+?)($|&)", self.generate_sub_func(), self.base_url)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.need_clear and self.responses:
            self.responses.clear()
            self.need_clear = False

        if self.done:
            self.init_val()
            logging.info("get source done: %s" % (self.config.source, ))
            raise StopAsyncIteration

        while True:
            try:
                async with self.config.session.get(self.base_url) as resp:
                    text = await resp.text()
                    result = json.loads(text)
            except aiohttp.client_exceptions.ClientPayloadError:
                await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                continue
            except Exception as e:
                logging.error("%s: %s" % (str(e), self.base_url))
                await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                continue

            if "data" in result:
                if not result["data"]:
                    if self.responses:
                        self.done = self.need_clear = True
                        return self.responses

            self.responses.extend(result["data"])
            self.curr_size += len(self.responses)

            if "pageToken" in result:
                if not result["pageToken"]:
                    if self.responses:
                        self.done = self.need_clear = True
                        return self.responses

                self.retry_count = 0
                self.page_token = str(result["pageToken"])
                self.update_base_url()

            elif "retcode" in result and result["retcode"] == "100002":
                if self.retry_count >= self.config.max_retry:
                    if self.responses:
                        self.done = self.need_clear = True
                        return self.responses

                self.retry_count += 1
                continue
            else:
                if self.retry_count >= self.config.max_retry:
                    logging.error("Unable to get url: %s " % (self.base_url, ))
                    if self.responses:
                        self.done = self.need_clear = True
                        return self.responses
                self.retry_count += 1
                await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                continue

            if self.config.max_limit and self.curr_size > self.config.max_limit:
                self.done = self.need_clear = True
                return self.responses
            elif len(self.responses) >= self.config.per_limit:
                self.need_clear = True
                return self.responses

    def __iter__(self):
        raise ValueError("APIGetter must be used with async generator, not normal generator")
