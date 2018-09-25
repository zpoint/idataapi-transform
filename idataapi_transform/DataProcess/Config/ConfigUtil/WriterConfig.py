import asyncio
import aioredis
import inspect

try:
    import aiomysql
except Exception as e:
    pass

try:
    import motor.motor_asyncio
except Exception as e:
    pass

from .BaseConfig import BaseWriterConfig
from ..ESConfig import get_es_client
from ..DefaultValue import DefaultVal


class WCSVConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 headers=None, filter_=None, expand=None, qsn=DefaultVal.qsn, **kwargs):
        """
        :param filename: filename to write
        :param mode: file open mode, i.e "w" or "a+"
        :param encoding: file encoding i.e "utf8"
        :param headers: csv headers in first row, if not set, automatically extract in first bulk of items
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param qsn: run "transform --help" to see command line interface explanation for detail
        :param kwargs:

        Example:
            ...
            csv_config = WCSVConfig("./result.csv", encoding="utf8", headers=["likeCount", "id", "title"])
            with ProcessFactory.create_writer(csv_config) as csv_writer:
                async for items in es_getter:
                    # do whatever you want with items
                    csv_writer.write(items)
        """
        super().__init__()
        self.filename = filename
        self.encoding = encoding
        self.mode = mode
        self.headers = headers
        self.filter = filter_
        self.expand = expand
        self.qsn = qsn


class WESConfig(BaseWriterConfig):
    def __init__(self, indices, doc_type, filter_=None, expand=None, id_hash_func=DefaultVal.default_id_hash_func,
                 appCode=None, actions=None, createDate=None, error_if_fail=True, timeout=None, max_retry=None,
                 random_min_sleep=None, random_max_sleep=None, auto_insert_createDate=True, **kwargs):
        """
        :param indices: elasticsearch indices
        :param doc_type: elasticsearch doc_type
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param id_hash_func: function to generate id_ for each item
        :param appCode: if not None, add appCode to each item before write to es
        :param actions: if not None, will set actions to user define actions, else default actions is 'index'
        :param createDate: if not None, add createDate to each item before write to es
        :param error_if_fail: if True, log to error if fail to insert to es, else log nothing
        :param timeout: http connection timeout when connect to es, seconds
        :param max_retry: if request fail, retry max_retry times
        :param random_min_sleep: if request fail, random sleep at least random_min_sleep seconds before request again
        :param random_max_sleep: if request fail, random sleep at most random_min_sleep seconds before request again
        :param auto_insert_createDate: whether insert createDate for each item automatic -> boolean
        :param kwargs:

        Example:
            ...
            es_config = WCSVConfig("post20170630", "news")
            with ProcessFactory.create_writer(es_config) as es_writer:
                    # asyncio function must call with await
                    await csv_writer.write(items)
        """
        super().__init__()

        if not random_min_sleep:
            random_min_sleep = DefaultVal.random_min_sleep
        if not random_max_sleep:
            random_max_sleep = DefaultVal.random_max_sleep
        if not max_retry:
            max_retry = DefaultVal.max_retry

        if not DefaultVal.main_config.has_es_configured:
            raise ValueError("You must config es_hosts before using Elasticsearch, Please edit configure file: %s" % (DefaultVal.main_config.ini_path, ))

        self.indices = indices
        self.doc_type = doc_type
        self.filter = filter_
        self.expand = expand
        self.id_hash_func = id_hash_func
        self.es_client = get_es_client()
        self.app_code = appCode
        self.actions = actions
        self.create_date = createDate
        self.error_if_fail = error_if_fail
        self.timeout = timeout
        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep
        self.auto_insert_createDate = auto_insert_createDate


class WJsonConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 expand=None, filter_=None, new_line=DefaultVal.new_line, **kwargs):
        """
        :param filename: filename to write
        :param mode: file open mode, i.e "w" or "a+"
        :param encoding: file encoding i.e "utf8"
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param new_line: new_line seperator for each item, default is "\n"
        :param kwargs:

        Example:
            ...
            json_config = WJsonConfig("./result.json")
            with ProcessFactory.create_writer(csv_config) as json_writer:
                async for items in es_getter:
                    json_writer.write(items)
        """
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.expand = expand
        self.filter = filter_
        self.new_line = new_line


class WTXTConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 expand=None, filter_=None, new_line=DefaultVal.new_line, join_val=DefaultVal.join_val, **kwargs):
        """
        :param filename: filename to write
        :param mode: file open mode, i.e "w" or "a+"
        :param encoding: file encoding i.e "utf8"
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param new_line: new_line seperator for each item, default is "\n"
        :param join_val: space seperator for each key in each item, default is " "
        :param kwargs:

        Example:
            ...
            txt_config = WTXTConfig("./result.txt")
            with ProcessFactory.create_writer(txt_config) as txt_writer:
                async for items in es_getter:
                    txt_writer.write(items)
        """
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.expand = expand
        self.filter = filter_
        self.new_line = new_line
        self.join_val = join_val


class WXLSXConfig(BaseWriterConfig):
    def __init__(self, filename, title=DefaultVal.title, expand=None, filter_=None, **kwargs):
        """
        :param filename: filename to write
        :param title: sheet title
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param kwargs:

        Example:
            ...
            xlsx_config = WXLSXConfig("./result.xlsx")
            with ProcessFactory.create_writer(xlsx_config) as xlsx_writer:
                async for items in es_getter:
                    xlsx_writer.write(items)
        """
        super().__init__()
        self.filename = filename
        self.title = title
        self.expand = expand
        self.filter = filter_


class WRedisConfig(BaseWriterConfig):
    def __init__(self, key, key_type="LIST", filter_=None, host=None, port=None, db=None, password=None, timeout=None,
                 encoding=None, direction=None, max_retry=None, random_min_sleep=None, random_max_sleep=None,
                 compress=None, **kwargs):
        """
        :param key: redis key to write data
        :param key_type: redis data type to operate, current only support LIST, HASH
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param host: redis host -> str
        :param port: redis port -> int
        :param db: redis database number -> int
        :param password: redis password -> int
        :param timeout: timeout per redis connection -> float
        :param encoding: redis object encoding -> str
        :param direction: "L" or "R", lpush or rpush
        :param compress: whether compress data use zlib before write to redis -> boolean
        :param kwargs:

        Example:
            redis_config = WRedisConfig("my_key")
            with ProcessFactory.create_writer(redis_config) as redis_writer:
                async for items in es_getter:
                    await redis_writer.write(items)
        """
        super().__init__()
        # load default value
        if not random_min_sleep:
            random_min_sleep = DefaultVal.random_min_sleep
        if not random_max_sleep:
            random_max_sleep = DefaultVal.random_max_sleep
        if not max_retry:
            max_retry = DefaultVal.max_retry
        if host is None:
            host = DefaultVal.redis_host
        if port is None:
            port = DefaultVal.redis_port
        if db is None:
            db = DefaultVal.redis_db
        if password is None:
            password = DefaultVal.redis_password
        if timeout is None:
            timeout = DefaultVal.redis_timeout
        if encoding is None:
            encoding = DefaultVal.redis_encoding
        if direction is None:
            direction = DefaultVal.redis_direction
        if compress is None:
            compress = DefaultVal.redis_compress

        # check value
        if not DefaultVal.main_config.has_redis_configured and port <= 0:
            raise ValueError("You must config redis before using Redis, Please edit configure file: %s" % (DefaultVal.main_config.ini_path, ))
        if key_type not in ("LIST", "HASH"):
            raise ValueError("key_type must be one of (%s)" % (str(("LIST", )), ))
        if not encoding:
            raise ValueError("You must specific encoding, since I am going to load each object in json format, "
                             "and treat it as dictionary in python")
        if not password:
            password = None

        self.redis_pool_cli = None
        self.key = key
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.encoding = encoding
        self.timeout = timeout

        self.key_type = key_type
        self.filter = filter_

        self.name = "%s_%s->%s" % (str(host), str(port), str(key))

        self.redis_write_method = None
        self.direction = direction
        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep
        self.compress = compress

        if key_type == "LIST":
            self.is_range = True
        else:
            self.is_range = False

    async def get_redis_pool_cli(self):
        """
        :return: an async redis client
        """
        if self.redis_pool_cli is None:
            kwargs = {
                "db": self.db,
                "password": self.password,
                "encoding": self.encoding,
                "timeout": self.timeout,
                "minsize": 1,
                "maxsize": 3
            }
            if self.compress:
                del kwargs["encoding"]
            self.redis_pool_cli = await aioredis.create_redis_pool((self.host, self.port), **kwargs)
            if self.key_type == "LIST":
                if self.direction == "L":
                    self.redis_write_method = self.redis_pool_cli.lpush
                else:
                    self.redis_write_method = self.redis_pool_cli.rpush
            else:
                self.redis_write_method = self.redis_pool_cli.hset

        return self.redis_pool_cli


class WMySQLConfig(BaseWriterConfig):
    def __init__(self, table, filter_=None, max_retry=None, random_min_sleep=None, random_max_sleep=None,
                 host=None, port=None, user=None, password=None, database=None, encoding=None, loop=None, **kwargs):
        """
        :param table: mysql table
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param max_retry: if request fail, retry max_retry times
        :param random_min_sleep: if request fail, random sleep at least random_min_sleep seconds before request again
        :param random_max_sleep: if request fail, random sleep at most random_min_sleep seconds before request again
        :param host: mysql host -> str
        :param port: mysql port -> int
        :param user: mysql user -> str
        :param password: mysql password -> str
        :param database: mysql database -> str
        :param charset: default utf8 -> str
        :param loop: async loop instance
        :param kwargs:

        Example:
            mysql_config = WMySQLConfig("my_table")
            mysql_writer = ProcessFactory.create_writer(mysql_config)
            async for items in redis_getter:
                await mysql_writer.write(items)
        """
        super().__init__()
        if not random_min_sleep:
            random_min_sleep = DefaultVal.random_min_sleep
        if not random_max_sleep:
            random_max_sleep = DefaultVal.random_max_sleep
        if not max_retry:
            max_retry = DefaultVal.max_retry
        if not host:
            host = DefaultVal.mysql_host
        if not port:
            port = DefaultVal.mysql_port
        if not user:
            user = DefaultVal.mysql_user
        if not password:
            password = DefaultVal.mysql_password
        if not database:
            database = DefaultVal.mysql_database
        if not encoding:
            encoding = DefaultVal.mysql_encoding

        if not DefaultVal.main_config.has_mysql_configured and port <= 0:
            raise ValueError("You must config mysql before using MySQL, Please edit configure file: %s" % (DefaultVal.main_config.ini_path, ))
        if "aiomysql" not in globals():
            raise ValueError("module mysql disabled, please reinstall "
                             "requirements with python version higher than 3.5.3 to enable it")

        self.table = table
        self.database = database

        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep
        self.filter = filter_

        self.name = "%s->%s" % (self.table, self.database)

        self.host = host
        self.port = port
        self.user = user
        if not password:
            password = ''
        self.password = password
        self.database = database
        self.encoding = encoding

        if not loop:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self.mysql_pool_cli = self.connection = self.cursor = None

    async def get_mysql_pool_cli(self):
        """
        :return: an async mysql client
        """
        if self.mysql_pool_cli is None:
            self.mysql_pool_cli = await aiomysql.create_pool(host=self.host, port=self.port, user=self.user,
                                                             password=self.password, db=self.database, loop=self.loop,
                                                             minsize=1, maxsize=3, charset=self.encoding)
            self.connection = await self.mysql_pool_cli.acquire()
            self.cursor = await self.connection.cursor()
        return self.mysql_pool_cli

    def free_resource(self):
        if self.mysql_pool_cli is not None:
            self.mysql_pool_cli.release(self.connection)
            self.mysql_pool_cli.close()
            self.loop.create_task(self.mysql_pool_cli.wait_closed())
            self.mysql_pool_cli = self.connection = self.cursor = None


class WMongoConfig(BaseWriterConfig):
    def __init__(self, collection, id_hash_func=DefaultVal.default_id_hash_func, max_retry=None, random_min_sleep=None,
                 random_max_sleep=None, filter_=None, host=None, port=None, username=None, password=None,
                 database=None, auto_insert_createDate=False, createDate=None, **kwargs):
        """
        :param collection: collection name
        :param id_hash_func: function to generate id_ for each item, only if "_id" not in item will I use 'id_hash_func' to generate "_id"
        :param return_source: if set to True, will return [item , ..., itemN], item is the "_source" object
                              if set to False, will return whatever elasticsearch return, i.e {"hits": {"total": ...}}
        :param max_retry: if request fail, retry max_retry times
        :param random_min_sleep: if request fail, random sleep at least random_min_sleep seconds before request again
        :param random_max_sleep: if request fail, random sleep at most random_min_sleep seconds before request again
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param host: mongodb host -> str
        :param port: mongodb port -> int
        :param user: mongodb user -> str
        :param password: mongodb password -> str
        :param database: mongodb database -> str
        :param createDate: if not None, add createDate to each item before write to mongodb
        :param auto_insert_createDate: whether insert createDate for each item automatic -> boolean
        :param kwargs:

        Example:
            data = [json_obj, json_obj, json_obj]
            mongo_config = WMongoConfig("my_coll")
            async with ProcessFactory.create_writer(mongo_config) as mongo_writer:
                await mongo_writer.write(data)
        """
        super().__init__()
        if not random_min_sleep:
            random_min_sleep = DefaultVal.random_min_sleep
        if not random_max_sleep:
            random_max_sleep = DefaultVal.random_max_sleep
        if not max_retry:
            max_retry = DefaultVal.max_retry
        if not host:
            host = DefaultVal.mongo_host
        if not port:
            port = DefaultVal.mongo_port
        if not username:
            username = DefaultVal.mongo_username
        if not password:
            password = DefaultVal.mongo_password
        if not database:
            database = DefaultVal.mongo_database

        if not DefaultVal.main_config.has_mongo_configured:
            raise ValueError("You must config MongoDB before using MongoDB, Please edit configure file: %s" % (DefaultVal.main_config.ini_path, ))
        if "motor" not in globals():
            raise ValueError("module motor disabled, please reinstall "
                             "requirements in linux")

        self.collection = collection
        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep
        self.filter = filter_
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.name = "%s->%s" % (self.database, self.collection)
        self.id_hash_func = id_hash_func
        self.auto_insert_createDate = auto_insert_createDate
        self.createDate = createDate

        self.client = self.collection_cli = None

    def get_mongo_cli(self):
        if self.client is None:
            kwargs = {
                "host": self.host,
                "port": self.port
            }
            if self.username:
                self.client = motor.motor_asyncio.AsyncIOMotorClient(
                    "mongodb://%s:%s@%s:%s/%s" % (self.username, self.password, kwargs["host"],
                                                  str(kwargs["port"]), self.database))
            else:
                self.client = motor.motor_asyncio.AsyncIOMotorClient(**kwargs)
            self.collection_cli = self.client[self.database][self.collection]
        return self.client
