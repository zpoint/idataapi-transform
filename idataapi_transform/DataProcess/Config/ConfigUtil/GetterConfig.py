import asyncio
import inspect
from .BaseConfig import BaseGetterConfig

from ..ESConfig import get_es_client
from ..DefaultValue import DefaultVal
from ..ConnectorConfig import session_manger, main_config


class RAPIConfig(BaseGetterConfig):
    def __init__(self, source, per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit,
                 max_retry=DefaultVal.max_retry, random_min_sleep=DefaultVal.random_min_sleep,
                 random_max_sleep=DefaultVal.random_max_sleep, session=None, filter_=None, return_fail=False,
                 tag=None, *args, **kwargs):
        """
        will request until no more next_page to get, or get "max_limit" items

        :param source: API to get, i.e. "http://..."
        :param per_limit: how many items to get per time
        :param max_limit: get at most max_limit items, if not set, get all
        :param max_retry: if request fail, retry max_retry times
        :param random_min_sleep: if request fail, random sleep at least random_min_sleep seconds before request again
        :param random_max_sleep: if request fail, random sleep at most random_min_sleep seconds before request again
        :param session: aiohttp session to perform request
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param return_fail: if set to True, for each iteration, will return a tuple,
            api_getter = ProcessFactory.create_getter(RAPIConfig("http://..."))
            async for items, bad_objects in getter:
                A = bad_objects[0]
                A.response: -> json object: '{"appCode": "weixinpro", "dataType": "post", "message": "param error", "retcode": "100005"}', if fail in request, response will be None
                A.tag: -> tag you pass to RAPIConfig
                A.source: -> source you pass to RAPIConfig

        :param args:
        :param kwargs:

        Example:
            api_config = RAPIConfig("http://...")
            api_getter = ProcessFactory.create_getter(api_config)
            async for items in api_getter:
                print(items)
        """
        super().__init__()
        self.source = source
        self.per_limit = per_limit
        self.max_limit = max_limit
        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep
        self.session = session_manger.get_session() if not session else session
        self.filter = filter_
        self.return_fail = return_fail
        self.tag = tag


class RCSVConfig(BaseGetterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_r, encoding=DefaultVal.default_encoding,
                 per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit, filter_=None, **kwargs):
        """
        :param filename: filename to read
        :param mode: file open mode, i.e "r"
        :param encoding: file encoding i.e "utf8"
        :param per_limit: how many items to get per time
        :param max_limit: get at most max_limit items, if not set, get all
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param kwargs:

        Example:
            csv_config = RJsonConfig("./result.csv", encoding="gbk")
            csv_getter = ProcessFactory.create_getter(csv_config)
            async for items in csv_getter:
                print(items)

            # both async generator and generator implemented
            for items in csv_getter:
                print(items)
        """
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.per_limit = per_limit
        self.max_limit = max_limit
        self.filter = filter_


class RESConfig(BaseGetterConfig):
    def __init__(self, indices, doc_type, per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit,
                 scroll="1m", query_body=None, return_source=True, max_retry=DefaultVal.max_retry,
                 random_min_sleep=DefaultVal.random_min_sleep, random_max_sleep=DefaultVal.random_max_sleep,
                 filter_=None, **kwargs):
        """
        :param indices: elasticsearch indices
        :param doc_type: elasticsearch doc_type
        :param per_limit: how many items to get per request
        :param max_limit: get at most max_limit items, if not set, get all
        :param scroll: default is "1m"
        :param query_body: default is '{"size": "per_limit", "query": {"match_all": {}}}'
        :param return_source: if set to True, will return [item , ..., itemN], item is the "_source" object
                              if set to False, will return whatever elasticsearch return, i.e {"hits": {"total": ...}}
        :param max_retry: if request fail, retry max_retry times
        :param random_min_sleep: if request fail, random sleep at least random_min_sleep seconds before request again
        :param random_max_sleep: if request fail, random sleep at most random_min_sleep seconds before request again
        :param filter_: run "transform --help" to see command line interface explanation for detail,
            only work if return_source is False
        :param kwargs:

        Example:
            body = {
                "size": 100,
                "_source": {
                    "includes": ["likeCount", "id", "title"]
                    }
            }
            es_config = RESConfig("post20170630", "news", max_limit=1000, query_body=body)
            es_getter = ProcessFactory.create_getter(es_config)
            async for items in es_getter:
                print(item)
        """
        super().__init__()
        if not query_body:
            query_body = {
                "size": per_limit,
                "query": {
                    "match_all": {}
                }
            }
        self.query_body = query_body
        self.indices = indices
        self.doc_type = doc_type
        self.per_limit = per_limit
        self.max_limit = max_limit
        self.scroll = scroll
        self.es_client = get_es_client()
        self.return_source = return_source
        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep
        self.filter = filter_

    def __del__(self):
        self.es_client.transport.close()


class RJsonConfig(BaseGetterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_r, encoding=DefaultVal.default_encoding,
                 per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit, filter_=None, **kwargs):
        """
        :param filename: line by line json file to read
        :param mode: file open mode, i.e "r"
        :param encoding: file encoding i.e "utf8"
        :param per_limit: how many items to get per time
        :param max_limit: get at most max_limit items, if not set, get all
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param kwargs:

        Example:
            json_config = RJsonConfig("./result.json")
            json_getter = ProcessFactory.create_getter(json_config)
            async for items in json_getter:
                print(items)

            # both async generator and generator implemented
            for items in json_getter:
                print(items)
        """
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.per_limit = per_limit
        self.max_limit = max_limit
        self.filter = filter_


class RXLSXConfig(BaseGetterConfig):
    def __init__(self, filename, per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit, sheet_index=0,
                 filter_=None, **kwargs):
        """
        :param filename: filename to read
        :param per_limit: how many items to get per time
        :param max_limit: get at most max_limit items, if not set, get all
        :param sheet_index: which sheet to get, 0 means 0th sheet
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param kwargs:

        Example:
            xlsx_config = RXLSXConfig("./result.xlsx")
            xlsx_getter = ProcessFactory.create_getter(xlsx_config)
            async for items in xlsx_getter:
                print(items)

            # both async generator and generator implemented
            for items in xlsx_getter:
                print(items)

        """
        super().__init__()
        self.filename = filename
        self.per_limit = per_limit
        self.max_limit = max_limit
        self.sheet_index = sheet_index
        self.filter = filter_


class RAPIBulkConfig(BaseGetterConfig):
    def __init__(self, sources, interval=DefaultVal.interval, concurrency=main_config["main"].getint("concurrency"),
                 filter_=None, return_fail=False, **kwargs):
        """
        :param sources: an iterable object, each item must be "url" or instance of RAPIConfig
        :param interval: integer or float, each time you call async generator, you will wait for "interval" seconds
                         and get all items fetch during this "interval"
        :param concurrency: how many concurrency task run, default read from config file, if concurrency set,
                            only string(url) in "sources" will work with this concurrency level, RAPIConfig instance won't
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param return_fail: if set to True, for each iteration, will return a tuple,
            api_getter = ProcessFactory.create_getter(RAPIBulkConfig([...]))
            async for items, bad_objects in getter:
                A = bad_objects[0]
                A.response: -> json object: '{"appCode": "weixinpro", "dataType": "post", "message": "param error", "retcode": "100005"}', if fail in request, response will be None
                A.tag: -> tag you pass to RAPIConfig
                A.source: -> source you pass to RAPIConfig

        :param kwargs:

        Example:
            sources = ["http://....", "http://....", "http://....", RAPIConfig("http://....")]
            bulk_config = RAPUBulkConfig(sources)
            bulk_getter = ProcessFactory.create_getter(bulk_config)
            async for items in bulk_getter:
                print(items)

        """
        super().__init__()
        self.configs = (self.to_config(i) for i in sources)
        self.interval = interval
        self.concurrency = concurrency
        self.session = session_manger._generate_session(concurrency_limit=concurrency)
        self.filter = filter_
        self.return_fail = return_fail

    def to_config(self, item):
        if isinstance(item, RAPIConfig):
            return item
        else:
            return RAPIConfig(item, session=self.session, filter_=self.filter, return_fail=self.return_fail)

    def __del__(self):
        if inspect.iscoroutinefunction(self.session.close):
            asyncio.ensure_future(self.session.close())
        else:
            self.session.close()
