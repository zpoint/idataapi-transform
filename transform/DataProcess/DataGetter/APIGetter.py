import re
import json
import hashlib
import random
import logging
import asyncio
import inspect
import traceback
from .BaseGetter import BaseGetter
from ..Config.ConfigUtil.GetterConfig import RAPIConfig
from ..Config.ConfigUtil.AsyncHelper import AsyncGenerator
from ..PersistentUtil.PersistentWriter import PersistentWriter

headers = {
    "Accept-Encoding": "gzip",
    # "Connection": "close"
}


class SourceObject(object):
    def __init__(self, response, tag, source, error_url, post_body):
        """
        When error occur
        :param response: error response body
        :param tag: tag user pass in
        :param source: source url user pass in
        :param error_url: current url elicit error
        :param post_body: HTTP post body
        """
        self.response = response
        self.tag = tag
        self.source = source
        self.error_url = error_url
        self.post_body = post_body


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
        self.method = "POST" if self.config.post_body else "GET"
        self.give_up = False
        self.data_type = ""
        self.app_code = ""

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
        self.config.persistent_writer = None
        self.give_up = False
        self.data_type = ""
        self.app_code = ""

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

    def generate_new_filter(self):
        def next_filter(item):
            item["appCode"] = self.app_code
            item["dataType"] = self.data_type
            return item

        def combine(item):
            result = old_filter(item) if old_filter else item
            if result is not None:
                return next_filter(result)

        old_filter = self.config.filter
        self.config.filter = combine

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.done:
            logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                         (self.config.source, self.total_count, self.miss_count))
            if self.config.persistent_writer and (not self.give_up or self.config.persistent_to_disk_if_give_up):
                self.config.persistent_writer.add(self.config.source)
            self.init_val()
            raise StopAsyncIteration

        while True:
            result = None # for SourceObject
            try:
                if self.config.debug_mode:
                    log_str = "HTTP method: %s, url: %s" % (self.method, self.base_url)
                    logging.info(log_str)
                resp = await self.config.session._request(self.method, self.base_url, headers=headers, data=self.config.post_body)
                text = await resp.text()
                # print(text)
                result = json.loads(text)
                if "data" not in result:
                    if "retcode" not in result or result["retcode"] not in self.config.success_ret_code:
                        raise ValueError("Bad retcode: %s" % (str(result["retcode"]) if "retcode" in result else str(result), ))
                if not self.data_type and self.config.keep_other_fields:
                    self.data_type = result["dataType"]
                    self.app_code = result["appCode"]
                    self.generate_new_filter()

            except Exception as e:
                self.retry_count += 1
                logging.error("retry: %d, %s: %s" % (self.retry_count, str(e), self.base_url))
                await asyncio.sleep(random.uniform(self.config.random_min_sleep, self.config.random_max_sleep))
                if self.retry_count < self.config.max_retry:
                    continue
                else:
                    # fail
                    logging.error("Give up, After retry: %d times, Unable to get url: %s, total get %d items, "
                                  "total filtered: %d items, error: %s" % (self.config.max_retry, self.base_url,
                                                                           self.total_count, self.miss_count,
                                                                           str(traceback.format_exc()) if "Bad retcode" not in str(e) else str(e)))
                    self.done = self.give_up = True
                    if self.config.return_fail:
                        self.bad_responses.append(SourceObject(result, self.config.tag, self.config.source, self.base_url, self.config.post_body))
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

                if self.config.filter:
                    curr_response = [self.config.filter(i) for i in result["data"]]
                    curr_response = [i for i in curr_response if i]
                    self.miss_count += origin_length - len(curr_response)
                else:
                    curr_response = result["data"]
                self.total_count += origin_length if self.config.exclude_filtered_to_max_limit else len(curr_response)
                self.responses.extend(curr_response)
                # trim_to_max_limit
                if self.config.trim_to_max_limit and self.config.max_limit and self.total_count > self.config.max_limit:
                    need_trim_items = self.total_count - self.config.max_limit
                    self.responses = self.responses[:-need_trim_items]
                    logging.info("trim %d items to fit max_limit: %d" % (need_trim_items, self.config.max_limit))
                    self.total_count -= need_trim_items
                # check if done
                if self.config.done_if is not None and self.config.done_if(curr_response):
                    self.done = True
                    return await self.clear_and_return()

            # get next page if success, retry if fail
            if "pageToken" in result:
                if not result["pageToken"]:
                    self.done = True
                    if self.need_return():
                        return await self.clear_and_return()

                self.page_token = str(result["pageToken"])
                self.update_base_url()

            elif "retcode" in result and result["retcode"] in self.config.success_ret_code:
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
                    self.done = self.give_up = True
                    if self.need_return():
                        return await self.clear_and_return()

                await asyncio.sleep(random.uniform(self.config.random_min_sleep, self.config.random_max_sleep))
                return await self.__anext__()

            if self.config.max_limit and self.total_count >= self.config.max_limit:
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
        self.persistent_writer = None
        self.skip_num = 0

    def to_config(self, item):
        if isinstance(item, RAPIConfig):
            r = item
        else:
            r = RAPIConfig(item, session=self.config.session, filter_=self.config.filter,
                              return_fail=self.config.return_fail, done_if=self.config.done_if,
                              trim_to_max_limit=self.config.trim_to_max_limit,
                              exclude_filtered_to_max_limit=self.config.exclude_filtered_to_max_limit,
                           persistent_to_disk_if_give_up=self.config.persistent_to_disk_if_give_up,
                           debug_mode=self.config.debug_mode)
        # persistent
        if self.config.persistent:
            if not self.config.persistent_key:
                self.config.persistent_key = hashlib.md5(r.source.encode("utf8")).hexdigest()
            if self.persistent_writer is None:
                self.persistent_writer = PersistentWriter(self.config.persistent_key)
            r.persistent_writer = self.persistent_writer
        return r

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
            # skip already done task
            if self.config.persistent:
                if api_config.source in self.persistent_writer:
                    self.skip_num += 1
                    continue
            self.pending_tasks.append(self.fetch_items(api_config))
            if len(self.pending_tasks) >= self.config.concurrency:
                self.persistent()
                return

        self.persistent()

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
                log_str = "After %.2f seconds, no new item fetched, current done task: %d, pending tasks: %d" % (float(self.config.interval), self.success_task, len(self.pending_tasks))
                if self.config.persistent:
                    log_str += ", skip %d already finished tasks with persistent mode on" % (self.skip_num, )
                logging.info(log_str)
                continue

        ret_log = "APIBulkGetter Done, total perform: %d tasks, fetch: %d items" % (self.success_task, self.curr_size)
        if self.config.return_fail:
            ret_log += ", fail: %d items" % (self.curr_bad_size, )
        if self.config.persistent:
            ret_log += ", skip %d already finished tasks with persistent mode on" % (self.skip_num,)
        logging.info(ret_log)
        if self.config.persistent:
            self.persistent_writer.clear(self.config.persistent_start_fresh_if_done)
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

    def persistent(self):
        # persistent task to file
        if self.config.persistent:
            self.persistent_writer.write()
            # logging.info("persistent mode on, after sync, totally skip %d already finished tasks" % (self.skip_num,))
