# idataapi-transform

Full async support **Toolkit** for [IDataAPI](http://www.idataapi.cn/) for efficiency work

* [中文文档](https://github.com/zpoint/idataapi-transform/blob/master/README_CN.md)

Provide

* [Command line interface](#command-line-interface-example)
* [Python API](#build-complex-routine-easily)

![diagram](https://github.com/zpoint/idataapi-transform/blob/master/idataapi-transform.png)

You can read data from one of

 * **API(http)**
 * **ES(ElasticSearch)**
 * **CSV**
 * **XLSX**
 * **JSON**
 * **Redis**
 * **MySQL**
 * **MongoDB**

and convert to

 * **CSV**
 * **XLSX**
 * **JSON**
 * **TXT**
 * **Redis**
 * **MySQL**
 * **MongoDB**

Features:

* with asyncio support and share same API
* Baesd on Template Method and Factory Method
* Every failure network read operation will log to error log before retry 3(default) times
* Every failure network write operation will log to error log before retry 3(default) times
* Command line support for simple usage, python module provide more features
* Every Getter and Writer support filter, you can alter or drop your data in filter
* Auto header generation(csv/xlsx)/table generation(mysql) based on your data
* APIGetter, will request next page automatically，each page will retry max_retry before fail
-------------------

### catalog

* [Requirment](#requirment)
* [Installation](#installation)
* [Command line interface Example](#command-line-interface-example)
	* [Elasticsearch to CSV](#read-data-from-elasticsearch-convert-to-csv)
	* [API to xlsx](#read-data-from-api-convert-to-xlsx)
	* [json to csv](#read-data-from-json-convert-to-csv)
	* [csv to xlsx](#read-data-from-csv-convert-to-xlsx)
	* [Elasticsearch to csv with parameters](#read-data-from-elasticsearch-convert-to-csv-with-parameters)
	* [API to redis](#read-data-from-api-write-to-redis)
	* [redis to csv](#read-data-from-redis-write-to-csv)
	* [API to MySQL](#read-data-from-api-write-to-mysql)
	* [MySQL to redis](#read-data-from-mysql-write-to-redis)
	* [MongoDB to csv](#read-data-from-mongodb-write-to-csv)
* [Python module support](#python-module-support)
	* [ES to csv](#es-to-csv)
	* [API to xlsx](#api-to-xlsx)
	* [CSV to xlsx](#csv-to-xlsx)
	* [API to redis](#api-to-redis)
    * [redis to MySQL](#redis-to-mysql)
    * [MongoDB to redis](#mongodb-to-redis)
	* [Bulk API to ES/MongoDB/Json](#bulk-api-to-es-or-mongodb-or-json)
	* [Extract error info from API](#extract-error-info-from-api)
	* [REDIS Usage](#redis-usage)
	* [call_back](#call_back)
* [ES Base Operation](#es-base-operation)
	* [Read data from ES](#read-data-from-es)
	* [Write data to ES](#write-data-to-es)
	* [DELETE data from ES](#delete-data-from-es)
	* [API to ES in detail](#api-to-es-in-detail)
	* [Get ES Client](#get-es-client)
* [doc string](#doc-string)
* [Update](#ipdate)
* [License](#license)

-------------------

#### Requirment
* python version >= 3.5.2
* If you need MySQL enable, your python version should be >= 3.5.3
* If you need MongoDB enable, your platform should not be **windows**
-------------------

#### Installation

	python3 -m pip install idataapi-transform
    # shell, run
    transform --help # explanation of each parameter and create configure file
    # edit ~/idataapi-transform.ini to config elasticsearch hosts, redis, mysql etc...

    # Install MySQL module, if your python version >= 3.5.3
    python3 -m pip install 'PyMySQL>=0.7.5,<0.9'
    python3 -m pip install aiomysql

    # Install MongoDB module, if your platform is not Windows
    python3 -m pip install motor

-------------------

#### Command line interface Example

* Read data from **[API, ES, CSV, XLSX, JSON, Redis, MySQL, MongoDB]**
* Write data to **[CSV, XLSX, JSON, TXT, ES, Redis, MySQL, MongoDB]**

##### read data from Elasticsearch convert to CSV

will read at most **500** items from given **index**: **knowledge20170517**, **doc_type**: **question**, and write to ./result.csv

	transform ES csv "knowledge20170517:question" --max_limit=500

##### read data from API convert to XLSX

will read all items from given api url, until no more next page, and save to dest(/Users/zpoint/Desktop/result.xlsx), **dest is optional, default is ./result.xlsx**

	transform API xlsx "http://xxx/post/dengta?kw=中国石化&apikey=xxx" "/Users/zpoint/Desktop/result"

##### read data from JSON convert to csv

will read items from json file, and save to **./result.csv**

	transform JSON csv "/Users/zpoint/Desktop/a.json"

##### read data from CSV convert to xlsx

will read items from csv file, and save to **./result.xlsx**

	transform CSV xlsx "./a.csv"


##### read data from Elasticsearch convert to CSV with parameters
* save csv with file encoding "gbk" **(--w_encoding)**
* specific index: knowledge20170517, doc_type: question **(knowledge20170517:question)**
* when read from Elasticsearch, specific request body **(--query_body)**

    	body = {
        	"size": 100,
        	"_source": {
            	"includes": ["location", "title", "city", "id"]
                }
              }

* before write to csv, add timestamp to each item, and drop items with null city **(--filter)**

        # create a file name my_filter.py (any filename will be accepted)
        import time
        def my_filter(item): # function name must be "my_filter"
            item["createtime"] = int(time.time())
            if item["city"]:
                return item # item will be write to destination
            # reach here, means return None, nothing will be write to destination

* Shell:

    	transform ES csv "knowledge20170517:question" --w_encoding gbk --query_body '{"size": 100, "_source": {"includes": ["location", "title", "city", "id"]}}' --filter ./my_filter.py

##### Read data from API write to Redis

* redis key name: my_key
* redis store/read support LIST and HASH, default value is LIST, you can change it with  --key_type parameter

will read data from ./a.csv, and save to redis LIST data structure, KEY: my_key

	transform API redis "http://xxx/post/dengta?kw=中国石化&apikey=xxx" my_key

##### Read data from Redis write to csv

will read data from redis key **my_key**, read at most 100 data， and save to **./result.csv**

	transform Redis csv my_key --max_limit 100

##### Read data from API write to MySQL

* auto create table if not exist

will read data from **API**, read at most 50 data， and save to MySQL table: **my_table**

	transform API MYSQL 'http://xxx' my_table --max_limit=50

##### Read data from MySQL write to redis

will read data from MySQL table **my_table**, each read operation fetch 60 items， and save to a redis LIST name **result**, **result** is the default key name if you don't provide one

	transform MYSQL redis my_table --per_limit=60

##### Read data from MongoDB write to csv

* you can provide --query_body

will read at most 50 data from "my_coll", and save to **./result.csv**

	transform mongo csv my_coll --max_limit=50


-------------------

#### Python module support

##### ES to csv

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

	async def example():
        body = {
            "size": 100,
            "_source": {
                "includes": ["likeCount", "id", "title"]
                }
        }
        es_config = GetterConfig.RESConfig("post20170630", "news", max_limit=1000, query_body=body)
        es_getter = ProcessFactory.create_getter(es_config)
        csv_config = WriterConfig.WCSVConfig("./result.csv")
        with ProcessFactory.create_writer(csv_config) as csv_writer:
            async for items in es_getter:
                # do whatever you want with items
                csv_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


##### API to xlsx

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

	async def example():
        api_config = GetterConfig.RAPIConfig("http://xxxx")
        getter = ProcessFactory.create_getter(api_config)
        xlsx_config = WriterConfig.WXLSXConfig("./result.xlsx")
        with ProcessFactory.create_writer(xlsx_config) as xlsx_writer:
        	async for items in getter:
                # do whatever you want with items
                xlsx_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### CSV to xlsx

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    async def example():
        csv_config = GetterConfig.RCSVConfig("./result.csv")
        getter = ProcessFactory.create_getter(csv_config)
        xlsx_config = WriterConfig.WXLSXConfig("./result.xlsx")
        with ProcessFactory.create_writer(xlsx_config) as xlsx_writer:
            for items in getter:
                # do whatever you want with items
                xlsx_writer.write(items)

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### API to redis

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    async def example():
        api_config = GetterConfig.RAPIConfig("http://xxx")
        getter = ProcessFactory.create_getter(api_config)
        redis_config = WriterConfig.WRedisConfig("key_a")
        with ProcessFactory.create_writer(redis_config) as redis_writer:
            async for items in getter:
                # do whatever you want with items
                await redis_writer.write(items)

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### redis to MySQL

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    def my_filter(item):
        # I am a filter
        # Every getter or writer created by ProcessFactory.create can set up a filter
        # every data will be pass to filter before return from getter, or before write to writer
        # you can alter data here, or drop data here
    if item["viewCount"] > 10:
        return item
        # if don't return anything(return None) means drop this data

	async def example():
        api_config = GetterConfig.RAPIConfig("http://xxxx", filter_=my_filter)
        getter = ProcessFactory.create_getter(api_config)
        mysql_config = WriterConfig.WMySQLConfig("my_table")
        with ProcessFactory.create_writer(mysql_config) as mysql_writer:
        	async for items in getter:
                # do whatever you want with items
                await mysql_writer.write(items)

        # await mysql_config.get_mysql_pool_cli() # aiomysql connection pool
        # mysql_config.connection # one of the connection in previous connection pool
        # mysql_config.cursor # cursor of previous connection
        # you should alaways call 'await mysql_config.get_mysql_pool_cli()' before use connection and cursor
        # provided by GetterConfig.RMySQLConfig and WriterConfig.WMySQLConfig

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


##### MongoDB to redis

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

	async def example():
        mongo_config = GetterConfig.RMongoConfig("coll_name")
        mongo_getter = ProcessFactory.create_getter(mongo_config)
        redis_config = WriterConfig.WRedisConfig("my_key")
        with ProcessFactory.create_writer(redis_config) as redis_writer:
        	async for items in mongo_getter:
                # do whatever you want with items
                await mysql_writer.write(items)

        # print(mongo_config.get_mysql_pget_mongo_cli()) # motor's AsyncIOMotorClient instance
        # provided by GetterConfig.RMongoConfig and WriterConfig.WMongoConfig

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())



##### Bulk API to ES or MongoDB or Json

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    """
    RAPIConfig support parameter:
    max_limit: get at most max_limit items, if not set, get all
    max_retry: if request fail, retry max_retry times
    filter_: run "transform --help" to see command line interface explanation for detail
    """

    async def example():
        # urls can be any iterable object, each item can be api url or RAPIConfig
        # RAPIBulkConfig accept a parameter: interval，means interval between each async generator return
        # if you set interval to 2 seconds，the async for will wait for 2 seconds before return，every data you get between these 2 seconds will be returned

        urls = ["http://xxxx", "http://xxxx", GetterConfig.RAPIConfig("http://xxxx"), ...]
        api_bulk_config = GetterConfig.RAPIBulkConfig(urls, concurrency=100)
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        es_config = WriterConfig.WESConfig("profile201712", "user")
        with ProcessFactory.create_writer(es_config) as es_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                await es_writer.write(items)

    def url_generator():
        for i in range(10000):
            yield url % (i, ) # yield RAPIConfig(url  % (i, )) will be OK

    async def example2mongo():
        urls = url_generator()
        api_bulk_config = GetterConfig.RAPIBulkConfig(urls, concurrency=50)
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        # you can config host.port in configure file，or pass as parameters，parameters have higher priority than configure file
        mongo_config = WriterConfig.WMongoConfig("my_coll")
        with ProcessFactory.create_writer(mongo_config) as mongo_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                await mongo_writer.write(items)

    async def example2json():
        urls = url_generator()
        api_bulk_config = GetterConfig.RAPIBulkConfig(urls, concurrency=30)
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        json_config = WriterConfig.WJsonConfig("./result.json")
        with ProcessFactory.create_writer(json_config) as json_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                json_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### Extract error info from API

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig

	async def example_simple():
        # if return_fail set to true, after retry 3(default) times,
        # still unable to get data error info will be returned in "bad_items"
        url = "xxx"
        config = GetterConfig.RAPIConfig(url, return_fail=True)
        reader = ProcessFactory.create_getter(config)
        async for good_items, bad_items in reader:
        	print(good_items)
            if len(bad_items) > 0:
            	err_obj = bad_items[0]
                print(err_obj.response) # http body, if network down，it will be None
                print(err_obj.tag) # tag you pass to RAPIConfig, default None
                print(err_obj.source) #  url you pass to RAPIConfig
                print(err_obj.error_url) # the url that elicit error

    async def example():
        unfinished_id_set = {'246834800', '376796200', '339808400', ...}
        config = GetterConfig.RAPIBulkConfig((RAPIConfig(base_url % (i,), return_fail=True, tag=i) for i in unfinished_id_set), return_fail=True, concurrency=100)
        reader = ProcessFactory.create_getter(config)
        async for good_items, bad_items in reader:
            # A: Whrn you set RAPIBulkConfig's return_fail to True,
            # 1）normal url will retuen error info
            # 2) RAPIConfig Object with return_fail set to True will retuen error info
            # 3) RAPIConfig Object with return_fail set to False will not retuen error info
            # B: Whrn you set RAPIBulkConfig's return_fail to False(default)
            # None of the previous situitions will return error info, same as the API to ES example above
        	print(bad_items)

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example_simple())


##### REDIS Usage

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    async def example_simple():
        # default key_type is LIST in redis
        # you can pass parameter "encoding" to specify how to encode before write to redis, default utf8
        json_lists = [...]
        wredis_config = WriterConfig.WRedisConfig("my_key")
        writer = ProcessFactory.create_writer(wredis_config)
        await writer.write(json_lists)

        # get async redis client
        client = await self.get_redis_pool_cli()

    async def example():
        # specify redis's key_type to HASH, default is LIST
        # compress means string object is compressed by zlib before write to redis,
        # we need to decompress it before turn to json object
        # you can pass parameter "need_del" to specify whether need to del the key after get object from redis, default false
        # you can pass parameter "direction" to specify whether read data from left to right or right to left, default left to right(only work for LIST key type)
        getter_config = GetterConfig.RRedisConfig("my_key_hash", key_type="HASH", compress=True)
        async for items in reader:
            print(items)


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### call_back

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    """
    call_back can be normal function or async funcion
     only RAPIConfig support call_back parameter, call_back will be used after filter，
    whatever call_back return will return to user
    """

    async def fetch_next_day(self, items):
        """ for each item, fetch seven days in order, combine data and return to user """
        prev_item = items[0]
        date_obj = # get a timestamp or datetime object from prev_item
        if date_obj not in seven_days:
            call_back = None
        else:
            call_back = self.fetch_next_day
        getter_config = GetterConfig.RAPIConfig(url, call_back=call_back)
        getter = ProcessFactory.create_getter(getter_config)
        async for items in getter:
            item = items[0]
            item = combine(item, prev_item)
            return [item]

    async def fetch_next_day_with_return_fail(self, good_items, bad_items):
        """ for each item, fetch seven days in order, combine data and return to user """
        if bad_items:
            # error in current level
            pass
        prev_item = good_items[0]
        date_obj = # get a timestamp or datetime object from prev_item
        if date_obj not in seven_days:
            call_back = None
        else:
            call_back = self.fetch_next_day_with_return_fail
        getter_config = GetterConfig.RAPIConfig(url, call_back=call_back, return_fail=True)
        getter = ProcessFactory.create_getter(getter_config)
        async for good_items, bad_items in getter:
            if bad_items:
                # next level retrn error, how to handle it?
                return None, bad_items

            item = good_items[0]
            item = combine(item, prev_item)
            return [item], bad_items

    async def start(self):
		id_set = {...a set of id...}
        url_generator = (GetterConfig.RAPIConfig(base_url % (id_, ), call_back=fetch_next_day) for id_ in self.id_set)
        bulk_config = GetterConfig.RAPIBulkConfig(url_generator, concurrency=20, interval=1)
        bulk_getter = ProcessFactory.create_getter(bulk_config)
        with ProcessFactory.create_writer(...) as writer:
            async for items in bulk_getter:
                await writer.write(items)

    async def start_with_return_fail(self):
		id_set = {...a set of id...}
        url_generator = (GetterConfig.RAPIConfig(base_url % (id_, ), call_back=fetch_next_day) for id_ in self.id_set, return_fail=True)
        bulk_config = GetterConfig.RAPIBulkConfig(url_generator, concurrency=20, interval=1, return_fail=True)
        bulk_getter = ProcessFactory.create_getter(bulk_config)
        with ProcessFactory.create_writer(...) as writer:
            async for items in bulk_getter:
                await es_writer.write([i for i in good_items if i is not None])


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start())



#### ES Base Operation

##### Read data from ES

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig

	async def example():
        # max_limit: means get at most max_limit items, if you don't provide it, means read all items
        # you can provide your query_body to es_config
        es_config = GetterConfig.RESConfig("post20170630", "news", max_limit=1000)
        es_getter = ProcessFactory.create_getter(es_config)
        async for items in es_getter:
            print(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### Write data to ES

    import asyncio
	from idataapi_transform import ProcessFactory, WriterConfig

    def my_hash_func(item):
        # generate ES_ID by my_hash_func
        return hashlib.md5(item["id"].encode("utf8")).hexdigest()

	async def example():
        json_lists = [#lots of json object]
        # actions support create, index, update default index
        # you can ignore "id_hash_func" to use default function to create ES_ID, see "API to ES in detail" below
        es_config = WriterConfig.WESConfig("post20170630", "news", actions="create")
        es_writer = ProcessFactory.create_getter(es_config)
        await es_writer.write(json_lists)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### DELETE data from ES

    import asyncio
    import json
    from idataapi_transform import ProcessFactory, WriterConfig

	async def example():
        # wrapper of delete_by_query API
        body = {"size": 100,  "query": {"bool": {"must": [{"term": {"createDate": "1516111225"}}]}}}
        writer = ProcessFactory.create_writer(WriterConfig.WESConfig("post20170630", "news"))
        r = await writer.delete_all(body=body)
        print(json.dumps(r))

	async def example_no_body():
        # same as above, without , delete all
        writer = ProcessFactory.create_writer(WESConfig("post20170630", "news"))
        r = await writer.delete_all()
        print(json.dumps(r))

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### API to ES in detail

    import time
    import asyncio
    from idataapi_transform import ProcessFactory, WriterConfig, GetterConfig
    """
    Every es document need an _id
    There are two rules to generate _id for ES inside this tool
    1) iF privide "id_hash_func" parameter when create WESConfig Object, _id will be id_hash_func(item)
    2) if rule 1 fail to match and the data(dictionary object) has key "id" and key "appCode"，_id will be md5(appCode_id)
    3) if rule 1 and rule 2 both fail to match, _id will be md5(str(item))
    """

    # global variables
    now_ts = int(time.time())

	def my_filter(item):
        # I am a filter
        # Every getter or writer created by ProcessFactory.create can set up a filter
        # every data will be pass to filter before return from getter, or before write to writer
        # you can alter data here, or drop data here
        if "posterId" in item:
        	return item
        # if don't return anything(return None) means drop this data

    async def example():
        # urls can be any iterable object, each item can be api url or RAPIConfig
        urls = ["http://xxxx", "http://xxxx", "http://xxxx", RAPIConfig("http://xxxx", max_limit=10)]
        # set up filter，drop every item without "posterId"
        api_bulk_config = GetterConfig.RAPIBulkConfig(urls, concurrency=100, filter_=my_filter)
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        # you can also set up filter here
        # createDate parameter set same "createDate" for every data written by this es_writer
        # Of course，you can ignore "createDate", es_writer will set every data's "createDate" to the current system's timestamp when it performs write operation, you can disable it by parameter auto_insert_createDate=False
        # add parameter "appCode" for every data so that it can generate _id by rule 1
        es_config = WriterConfig.WESConfig("profile201712", "user", createDate=now_ts)
        with ProcessFactory.create_writer(es_config) as es_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                await es_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


##### Get ES Client

    import asyncio
    import json
	from idataapi_transform import ProcessFactory, WriterConfig

	async def example():
        writer = ProcessFactory.create_writer(WriterConfig.WESConfig("post20170630", "news"))
        client = writer.config.es_client
        # a client based on elasticsearch-async, you can read offical document

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())



-------------------

#### doc string

	from idataapi_transform import GetterConfig, WriterConfig

    # run help on config to see detail
    help(GetterConfig.RAPIConfig)
    """
    ...
    will request until no more next_page to get, or get "max_limit" items

    :param source: API to get, i.e. "http://..."
    :param per_limit: how many items to get per time
    :param max_limit: get at most max_limit items, if not set, get all
    :param max_retry: if request fail, retry max_retry times
    :param random_min_sleep: if request fail, random sleep at least random_min_sleep seconds before request again
    :param random_max_sleep: if request fail, random sleep at most random_min_sleep seconds before request again
    :param session: aiohttp session to perform request
    :param args:
    :param kwargs:

    Example:
        api_config = RAPIConfig("http://...")
        api_getter = ProcessFactory.create_getter(api_config)
        async for items in api_getter:
            print(items)
    ...
	"""


-------------------

#### Update
v 1.4.1
* call_back support
* mongodb auth support and motor 2.0 support
* mongodb support
* fix APIBulkGetter incompleted data bug
* 3.5 compatiable
* ESGetter get all data instead of half
* compatible with elasticsearch-async-6.1.0
* ESClient singleton

v 1.2.0
* mysql support
* redis support
* retry 3 times for every write operation
* ES create operation
* shorter import directory

v.1.0.1 - 1.1.1
* fix es getter log error
* unclose session error for elasticsearch
* fix ES infinity scroll
* fix bug (cli)
* es_client msearch support
* fix XLSX reader
* return_fail for APIGetter
* compatible for aiohttp 3.x

v.1.0
* fix ESWriter log bug
* timeout add for ESWriter

v.0.9
* filter for every getter
* createDate for ESWriter
* APIGetter per_liimt bug fix
* new session for all RAPIBulkConfig

v.0.8
* error logging when unable to insert to target for ESWriter
* actions parameter add for WESConfig
* id_hash func change for ESWriter


v.0.7
* remove APIGetter infinity loop for empty result

v.0.6
* No error when read empty item from ESGetter

v.0.5
* fetch more items for ESGetter in CLI per request
* per_limit param fix for ESGetter in CLI

v.0.4
* appCode for ESWriter Config
* ESGetter CLI bug fix

v.0.3
* doc string for each config
* RAPIBulkConfig support

-------------------

#### License

http://rem.mit-license.org