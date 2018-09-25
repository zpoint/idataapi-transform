import re
import json
import random
import logging
import asyncio
import inspect
import traceback
from .BaseGetter import BaseGetter
from ..Config.ConfigUtil.GetterConfig import RAPIConfig
from ..Config.ConfigUtil.AsyncHelper import AsyncGenerator

headers = {
    "Accept-Encoding": "gzip",
    # "Connection": "close"
}


class SourceObject(object):
    def __init__(self, response, tag, source, error_url):
        """
        When error occur
        :param response: error response body
        :param tag: tag user pass in
        :param source: source url user pass in
        :param error_url: current url elicit error
        """
        self.response = response
        self.tag = tag
        self.source = source
        self.error_url = error_url


class APIGetter(BaseGetter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.base_url = self.config.source
        self.retry_count = 0
        self.responses = list()
        self.bad_responses = list()
        self.done = False
        self.page_token = ""
        self.miss_count = 0
        self.total_count = 0
        self.call_back = self.async_call_back = None
        if self.config.call_back is not None:
            if inspect.iscoroutinefunction(self.config.call_back):
                self.async_call_back = self.config.call_back
            else:
                self.call_back = self.config.call_back
        self.request_time = 0

    def init_val(self):
        self.base_url = self.config.source
        self.retry_count = 0
        self.responses = list()
        self.bad_responses = list()
        self.done = False
        self.page_token = ""
        self.miss_count = 0
        self.total_count = 0
        self.call_back = self.async_call_back = None
        self.request_time = 0

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
        if self.done:
            logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                         (self.config.source, self.total_count, self.miss_count))
            self.init_val()
            raise StopAsyncIteration

        while True:
            try:
                async with self.config.session.get(self.base_url, headers=headers) as resp:
                    text = await resp.text()
                    result = json.loads(text)
                    if "data" not in result:
                        if "retcode" not in result or result["retcode"] not in ("100002", "100301", "100103"):
                            raise ValueError("Bad retcode: %s" % (str(result["retcode"]), ))

            except Exception as e:
                self.retry_count += 1
                logging.error("retry: %d, %s: %s" % (self.retry_count, str(e), self.base_url))
                await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                if self.retry_count < self.config.max_retry:
                    continue
                else:
                    # fail
                    logging.error("Give up, After retry: %d times, Unable to get url: %s, total get %d items, "
                                  "total filtered: %d items, error: %s" % (self.config.max_retry, self.base_url,
                                                                           self.total_count, self.miss_count,
                                                                           str(traceback.format_exc()) if "Bad retcode" not in str(e) else str(e)))
                    self.done = True
                    if self.config.return_fail:
                        self.bad_responses.append(SourceObject(None, self.config.tag, self.config.source, self.base_url))
                        return await self.clear_and_return()
                    elif self.responses:
                        return await self.clear_and_return()
                    else:
                        return await self.__anext__()

            self.request_time += 1
            if "data" in result:
                # success
                self.retry_count = 0
                origin_length = len(result["data"])
                self.total_count += origin_length

                if self.config.filter:
                    curr_response = [self.config.filter(i) for i in result["data"]]
                    curr_response = [i for i in curr_response if i]
                    self.miss_count += origin_length - len(curr_response)
                else:
                    curr_response = result["data"]
                self.responses.extend(curr_response)

            # get next page if success, retry if fail
            if "pageToken" in result:
                if not result["pageToken"]:
                    self.done = True
                    if self.need_return():
                        return await self.clear_and_return()

                self.page_token = str(result["pageToken"])
                self.update_base_url()

            elif "retcode" in result and result["retcode"] in ("100002", "100301", "100103"):
                self.done = True
                if self.need_return():
                    return await self.clear_and_return()
                return await self.__anext__()
            else:
                self.retry_count += 1
                if self.retry_count >= self.config.max_retry:
                    logging.error("Give up, After retry: %d times, Unable to get url: %s, total get %d items, "
                                  "total filtered: %d items" % (self.config.max_retry, self.base_url,
                                                                self.total_count, self.miss_count))
                    self.done = True
                    if self.need_return():
                        return await self.clear_and_return()

                await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                return await self.__anext__()

            if self.config.max_limit and self.total_count > self.config.max_limit:
                self.done = True
                return await self.clear_and_return()
            elif len(self.responses) >= self.config.per_limit:
                return await self.clear_and_return()
            elif self.done:
                # buffer has empty data, and done fetching
                return await self.__anext__()

            if self.request_time % self.config.report_interval == 0:
                logging.info("After request %d pages, current item count(%d) < per_limit(%d), latest request page: %s" %
                             (self.request_time, len(self.responses), self.config.per_limit, self.base_url))

    def __iter__(self):
        raise ValueError("APIGetter must be used with async generator, not normal generator")

    async def clear_and_return(self):
        self.request_time = 0
        if self.config.return_fail:
            resp, bad_resp = self.responses, self.bad_responses
            self.responses, self.bad_responses = list(), list()
            if self.call_back is not None:
                r = self.call_back(resp, bad_resp)
                if inspect.iscoroutine(r):
                    # bind function for coroutine
                    self.async_call_back = self.call_back
                    self.call_back = None
                    return await r
                return r
            elif self.async_call_back is not None:
                return await self.async_call_back(resp, bad_resp)
            else:
                return resp, bad_resp
        else:
            resp = self.responses
            self.responses = list()
            if self.call_back is not None:
                r = self.call_back(resp)
                if inspect.iscoroutine(r):
                    # bind function for coroutine
                    self.async_call_back = self.call_back
                    self.call_back = None
                    return await r
                return r
            elif self.async_call_back is not None:
                return await self.async_call_back(resp)
            else:
                return resp

    def need_return(self):
        return self.responses or (self.config.return_fail and (self.responses or self.bad_responses))


class APIBulkGetter(BaseGetter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.async_api_configs = AsyncGenerator(self.config.sources, self.to_config)

        self.pending_tasks = list()
        self.buffers = list()
        self.bad_buffers = list()
        self.success_task = 0
        self.curr_size = 0
        self.curr_bad_size = 0

    def to_config(self, item):
        if isinstance(item, RAPIConfig):
            return item
        else:
            return RAPIConfig(item, session=self.config.session, filter_=self.config.filter, return_fail=self.config.return_fail)

    async def fetch_items(self, api_config):
        if api_config.return_fail:
            async for items, bad_items in APIGetter(api_config):
                if self.config.return_fail:
                    self.bad_buffers.extend(bad_items)
                self.buffers.extend(items)
        else:
            async for items in APIGetter(api_config):
                self.buffers.extend(items)

    async def fill_tasks(self):
        if len(self.pending_tasks) >= self.config.concurrency:
            return

        async for api_config in self.async_api_configs:
            self.pending_tasks.append(self.fetch_items(api_config))
            if len(self.pending_tasks) >= self.config.concurrency:
                return

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self.fill_tasks()
        while self.pending_tasks:
            done, pending = await asyncio.wait(self.pending_tasks, timeout=self.config.interval)
            self.pending_tasks = list(pending)
            self.success_task += len(done)
            if self.buffers or (self.config.return_fail and (self.buffers or self.bad_buffers)):
                return self.clear_and_return()
            else:
                # after interval seconds, no item fetched
                await self.fill_tasks()
                logging.info("After %.2f seconds, no new item fetched, current done task: %d, pending tasks: %d" %
                             (float(self.config.interval), self.success_task, len(self.pending_tasks)))
                continue

        ret_log = "APIBulkGetter Done, total perform: %d tasks, fetch: %d items" % (self.success_task, self.curr_size)
        if self.config.return_fail:
            ret_log += " fail: %d items" % (self.curr_bad_size, )
        logging.info(ret_log)
        raise StopAsyncIteration

    def __iter__(self):
        raise ValueError("APIBulkGetter must be used with async generator, not normal generator")

    def clear_and_return(self):
        if self.config.return_fail:
            buffers, bad_buffers = self.buffers, self.bad_buffers
            self.curr_size += len(self.buffers)
            self.curr_bad_size += len(self.bad_buffers)
            self.buffers, self.bad_buffers = list(), list()
            return buffers, bad_buffers
        else:
            buffers = self.buffers
            self.curr_size += len(self.buffers)
            self.buffers = list()
            return buffers
