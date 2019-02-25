# idataapi-transform

[IDataAPI](http://www.idataapi.cn/) 转换工具

* [English](https://github.com/zpoint/idataapi-transform/blob/master/README.md)

提供

* [命令行支持](#命令行支持)
* [Python模块支持](#Python模块支持)

![示意图](https://github.com/zpoint/idataapi-transform/blob/master/idataapi-transform.png)

可以从以下任意 **格式/文件/数据库** 读取数据

 * **API(http)**
 * **ES(ElasticSearch)**
 * **CSV**
 * **XLSX**
 * **JSON**
 * **Redis**
 * **MySQL**
 * **MongoDB**

并存储到以下任意 **格式/文件/数据库**

 * **CSV**
 * **XLSX**
 * **JSON**
 * **TXT**
 * **ES**
 * **Redis**
 * **MySQL**
 * **MongoDB**

Features:

* 所有的网络通讯均通过 asyncio原生/或第三方lib 提供异步支持
* 各模块间通过相同的方法调用
* 基于 Template Method 以及 Factory Method 完成
* 所有的网络读取操作均默认重试三次，三次后错误输出至错误日志
* 所有的网络写入操作均默认重试三次，三次后错误输出至错误日志
* 所有不同的格式均提供简便的命令行操作，在模块中提供更丰富的参数支持
* 每一个 Getter 和 Writer 对象都支持过滤器，你可以在过滤器中修改或者过滤每一条数据
* 自动生成表头(csv/xlsx)/表格(mysql)
* 对于APIGetter, 会自动翻页，每次翻页会自动重试到限制次数再进行报错
* APIBulkGetter 支持指定并发数后批量处理 APIGetter/url 对象, 可接受可迭代对象或者异步迭代器
-------------------

### 目录

* [环境要求](#环境要求)
* [安装指南](#安装指南)
* [命令行支持及示例](#命令行支持及示例)
	* [Elasticsearch to CSV](#从-elasticsearch-读取数据-转换为-csv-格式)
	* [API to XLSX](#从-api-读取数据-转换为-xlsx-格式)
	* [JSON to CSV](#从-json-文件读取数据-转换为-csv-格式)
	* [CSV to XLSX](#从-csv-读取数据-转换至-xlsx)
	* [Elasticsearch to CSV with parameters](#从-elasticsearch-读取数据-转换至-csv)
	* [API to Redis](#从-api-读取数据-存储至-redis)
	* [Redis to CSV](#从-redis-读取数据-存储至-csv)
	* [API to MySQL](#从-api-读取数据-写入-mysql)
	* [MySQL to redis](#从-mysql-读取数据-写入-redis)
	* [MongoDB to csv](#从-mongodb-读取数据-写入-csv)
* [Python模块支持](#Python模块支持)
    * [ES to CSV](#es-to-csv)
    * [API to XLSX](#api-to-xlsx)
	* [CSV to xlsx](#csv-to-xlsx)
    * [API to Redis](#api-to-redis)
    * [redis to MySQL](#redis-to-mysql)
    * [MongoDB to redis](#mongodb-to-redis)
    * [并发从不同的 API 读取数据 并写入到 ES/MongoDB/Json](#并发从不同的-api-读取数据-并写入到-es或mongodb或json)
    * [访问API出错时 提取错误信息](#访问api出错时-提取错误信息)
    * [call_back](#callback)
    * [filter](#redis-to-mysql)
    * [done_if](#api-to-xlsx)
    * [持久化任务到硬盘(断点续传)](#persistent-to-disk)
* [REDIS 基本示例](#redis-基本示例)
* [ES 基本操作](#es-基本操作)
	* [从ES读取数据](#从es读取数据)
	* [写数据进ES](#写数据进es)
	* [删除ES中的数据](#删除es中的数据)
	* [从API读取并写数据进入ES](#从api读取并写数据进入es)
	* [获得ES Client](#获得es-client)
* [配置](#配置)
	* [配置文件路径](#配置文件路径)
	* [运行时指定配置](#运行时指定配置)
* [说明](#说明)
* [升级](#changelog)
* [许可](#许可)

-------------------

#### 环境要求
* python 版本号 >= 3.5.2
* 如果你需要使用 MySQL 模块, 你的 python 版本号要 >= 3.5.3
* 如果你需要使用 MongoDB 模块，你需要在非 Windows 下

-------------------

#### 安装指南

	python3 -m pip install idataapi-transform
    # 安装完成后在终端跑如下命令
    transform --help # 解释各个参数的作用以及创建默认的配置文件
    # 编辑配置文件 ~/idataapi-transform.ini 配置 ElasticSearch, redis, mysql 主机, 端口, 默认并发数等参数

    # 如果你的 python 版本 >= 3.5.3, 并且需要安装 MySQL 模块
    python3 -m pip install 'PyMySQL>=0.7.5,<0.9'
    python3 -m pip install aiomysql

    # 如果你不在 Windows 下, 并且需要安装 MongoDB 模块
    python3 -m pip install motor

-------------------

#### 命令行支持及示例

* 从以下任意一格式读数据 **[API, ES, CSV, XLSX, JSON, Redis, MySQL, MongoDB]**
* 写数据至以下任意一格式 **[CSV, XLSX, JSON, TXT, ES, Redis, MySQL, MongoDB]**

##### 从 Elasticsearch 读取数据 转换为 CSV 格式

从ES中 **index**: **knowledge20170517**, **doc_type**: **question**, 中最多读取 **500** 条数据, 写入到 ./result.csv

	transform ES csv "knowledge20170517:question" --max_limit=500

##### 从 API 读取数据 转换为 XLSX 格式

会从提供的http请求读取所有数据(翻到最后一页为止), 并写入到 /Users/zpoint/Desktop/result.xlsx 中, **写入文件为可选参数, 可以不填, 默认参数是 ./result.xlsx**

	transform API xlsx "http://xxx/post/dengta?kw=中国石化&apikey=xxx" "/Users/zpoint/Desktop/result"

##### 从 JSON 文件读取数据 转换为 CSV 格式

JSON 为一行一条数据的 JSON 文件
会从 /Users/zpoint/Desktop/a.json 读取数据, 并写入 **./result.csv** (默认参数)

	transform JSON csv "/Users/zpoint/Desktop/a.json"

##### 从 CSV 读取数据 转换至 xlsx

会从 ./a.csv 读取数据, 并保存至 **./result.xlsx**

	transform CSV xlsx "./a.csv"


##### 从 Elasticsearch 读取数据 转换至 CSV
* 以下示例详细展开了部分参数
* 以 "gbk" **(--w_encoding)** 编码保存 CSV 文件
* 指定 ES 的 index: knowledge20170517, doc_type: question **(knowledge20170517:question)**
* 指定如下过滤条件 **(--query_body)**

    	body = {
        	"size": 100,
        	"_source": {
            	"includes": ["location", "title", "city", "id"]
                }
              }

* 在写入 CSV 之前, 为每一条获取到的数据增加时间戳，以及移除 "city" 字段为空的对象 **(--filter)**

        # 创建一个文件叫做 my_filter.py (随便什么名字都行)
        import time
        def my_filter(item): # 函数名必须为 "my_filter"
        	# item 是一条数据，在这里是一个字段对象
            item["createtime"] = int(time.time())
            if item["city"]:
                return item # item 会被写入你指定的目的地
            # 执行到了这里, 说明返回 None, 这一条 item 会被抛弃，不会被写入目的地

* 终端:

    	transform ES csv "knowledge20170517:question" --w_encoding gbk --query_body '{"size": 100, "_source": {"includes": ["location", "title", "city", "id"]}}' --filter ./my_filter.py

##### 从 API 读取数据 存储至 Redis

* 键名称为 my_key
* redis 存储/读取 支持 LIST, 以及 HASH 两种数据结构, 默认为 LIST, 可用参数 --key_type 指明

会从 ./a.csv 读取数据, 并保存至 **./result.xlsx**

	transform API redis "http://xxx/post/dengta?kw=中国石化&apikey=xxx" "/Users/zpoint/Desktop/result"

##### 从 Redis 读取数据 存储至 csv

会从 my_key 中读取至多100条数据， 并保存至 **./result.csv**

	transform Redis csv my_key --max_limit 100

##### 从 API 读取数据 写入 MySQL

* 当表格不存在时自动创建

会至多从API获取50条数据， 写入 MySQL 表格: **my_table**

	transform API MYSQL 'http://xxx' my_table --max_limit=50

##### 从 MySQL 读取数据 写入 redis

会从 MySQL 表格 **table** 获取数据，每次网络请求60条数据，写入 redis LIST 结构，默认键名称为 result

	transform MYSQL redis my_table --per_limit=60

##### 从 MongoDB 读取数据 写入 csv

* 你也可以提供 --query_body 参数进行过滤查询

会从 my_coll 中读取至多50条数据， 并保存至 **./result.csv**

	transform MONGO csv my_coll --max_limit=50

-------------------

#### Python模块支持


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

    yesterday_ts = int(time.time()) - 24 * 60 * 60

    def my_done_if(items):
        # RAPIConfig 会自动翻页直到以下其中的一种情况发生
        # 1. 没有下一页
        # 2. 达到你传入的 max_limit 值
        # 3. 发生某种错误三次以上
        # 如果你想要提供自己的中止条件，比如时间戳小于昨天的某个值就停止翻页，可以提供done_if参数
        if items[-1]["publishDate"] < yesterday_ts:
        	return True
        return False

	async def example():
        api_config = GetterConfig.RAPIConfig("http://xxxx")
        # 你也可以使用: api_config = GetterConfig.RAPIConfig("http://xxxx", done_if=my_done_if)
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
    # 我是一个过滤器
    # 每一个 ProcessFactory.create 产生的 getter 或者 writer 都可以配置一个过滤器
    # 每一条数据在返回之前会经过这个过滤器，过滤器可以修改这条item, 也可以选择过滤这条item
    if item["viewCount"] > 10:
        return item
    # 如果 retuen None 或者不 return 任何东西，则表示过滤这一条数据

	async def example():
        api_config = GetterConfig.RAPIConfig("http://xxxx", filter_=my_filter)
        getter = ProcessFactory.create_getter(api_config)
        mysql_config = WriterConfig.WMySQLConfig("my_table")
        with ProcessFactory.create_writer(mysql_config) as mysql_writer:
        	async for items in getter:
                # do whatever you want with items
                await mysql_writer.write(items)

        # await mysql_config.get_mysql_pool_cli() # aiomysql 连接池
        # mysql_config.connection # 连接池中的其中一个连接
        # mysql_config.cursor # 这个连接当前的光标
        # 在使用 cursor 和 connection 之前，需要 'await mysql_config.get_mysql_pool_cli()' 初始化
        # GetterConfig.RMySQLConfig 和 WriterConfig.WMySQLConfig 均提供

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

        # print(mongo_config.get_mysql_pget_mongo_cli()) # motor 的 AsyncIOMotorClient 实例
        # GetterConfig.RMongoConfig 和 WriterConfig.WMongoConfig 均提供

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


##### 并发从不同的 API 读取数据 并写入到 ES或MongoDB或Json

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    """
    GetterConfig.RAPIConfig 支持如下参数:
    max_limit: 最多获取到多少条
    max_retry: 如果请求失败，最多重试多少次
    filter_: 过滤器
    其他参数可以跑 help(GetterConfig.RAPIConfig) 查看
    """

	def url_generator():
    	for i in range(10000):
        	yield url % (i, ) # yield RAPIConfig(url  % (i, )) 也是可以的

    async def example():
        # urls 可以是任何可迭代对象, 列表，迭代器等, urls 里面的元素可以是一条 url, 也可以使配置好的 RAPIConfig 对象
        # RAPIBulkConfig 有个 interval 参数，表示每一次异步迭代器返回的时间间隔，
        # 比如 interval 设置为 2 秒钟，则 async for 一次会等待两秒，在这两秒钟请求到的数据会一起返回
        urls = ["http://xxxx", "http://xxxx", "http://xxxx", GetterConfig.RAPIConfig("http://xxxx", max_limit=10)]
        api_bulk_config = GetterConfig.RAPIBulkConfig(urls, concurrency=100) # 指定并发数
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        es_config = WriterConfig.WESConfig("profile201712", "user")
        with ProcessFactory.create_writer(es_config) as es_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                await es_writer.write(items)

    async def example2mongo():
        urls = url_generator()
        api_bulk_config = GetterConfig.RAPIBulkConfig(urls, concurrency=50)
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        # host.port 那些在配置文件配置，或者在这里传入也行，这里传入优先级高于配置文件
        mongo_config = WriterConfig.WMongoConfig("my_coll")
        with ProcessFactory.create_writer(mongo_config) as mongo_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                await mongo_writer.write(items)

    # ******************************************************
    # 下面是使用 "async generator" 异步迭代器给 BulkGetter 传送任务的示例
    # async 函数中使用 yield 的方法只在 python3.6+的版本支持，3.5+ 请参考下面资料定义异步迭代器
    # https://github.com/python-trio/async_generator
    # idataapi-transform 版本需 >= 1.4.4
    # ******************************************************

    async def put_task2redis():
        writer = ProcessFactory.create_writer(WriterConfig.WRedisConfig("test"))
        await writer.write([
            {"keyword": "1"},
            {"keyword": "2"},
            {"keyword": "3"}
        ])

    async def async_generator():
        """
        I am async generator
        """
        getter = ProcessFactory.create_getter(GetterConfig.RRedisConfig("test"))
        async for items in getter:
            for item in items:
                r = GetterConfig.RAPIConfig("http://xxx%sxxx" % (item["keyword"], ), max_limit=100)
                yield r

    async def example2json():
        # await put_task2redis()
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

##### 访问API出错时 提取错误信息

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig

	async def example_simple():
        # 如果 return_fail 设置为 true, 当在重试次数过后还是无法获得数据，
        # 则错误信息会返回在 bad_items 中
        url = "xxx"
        config = GetterConfig.RAPIConfig(url, return_fail=True)
        reader = ProcessFactory.create_getter(config)
        async for good_items, bad_items in reader:
        	print(good_items)
            if len(bad_items) > 0:
            	err_obj = bad_items[0]
                print(err_obj.response) # 返回的 http body, 如果网络故障，则为None
                print(err_obj.tag) # RAPIConfig 传入的 tag, 这里没有传入，故为None
                print(err_obj.source) # RAPIConfig 传入的 url
                print(err_obj.error_url) # 发生错误的url, 可能是翻到某页产生的错误

    async def example():
        unfinished_id_set = {'246834800', '376796200', '339808400', ...}
        config = GetterConfig.RAPIBulkConfig((RAPIConfig(base_url % (i,), return_fail=True, tag=i) for i in unfinished_id_set), return_fail=True, concurrency=100)
        reader = ProcessFactory.create_getter(config)
        async for good_items, bad_items in reader:
            # A: RAPIBulkConfig 的 return_fail 为 True 时, 内部
            # 1）普通 url 的错误会被返回
            # 2) RAPIConfig 指定了 return_fail 为 True 的错误会被返回
            # 3) RAPIConfig 指定了 return_fail 为 False 的错误不会被返回
            # B: RAPIBulkConfig 的 return_fail 为 False 时(默认), 内部
            # 以上三种情况的错误都不会返回，迭代器一次也只返回一个列表，同上一个示例
        	print(bad_items)

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example_simple())


##### call_back

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    """
    call_back 可以是普通函数，也可以是 async 函数
    call_back 参数当前仅支持 RAPIConfig, call_back 顺序在过滤器之后，
    并且 call_back 返回的结果会直接返回给调用者
    """

    async def fetch_next_day(self, items):
        """ 对于一个item, 按顺序连续获取七天数据, 合并后再返回 """
        prev_item = items[0]
        date_obj = # 从 prev_item 获取到一个时间戳/datetime object
        if date_obj 不在距离当天七天内的时候:
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
        """ 对于一个item, 按顺序连续获取七天数据, 合并后再返回 """
        if bad_items:
            # 到达当前这一层出错?
            pass
        prev_item = good_items[0]
        date_obj = # 从 prev_item 获取到一个时间戳/datetime object
        if date_obj 在距离当天七天内的时候:
            call_back = None
        else:
            call_back = self.fetch_next_day_with_return_fail
        getter_config = GetterConfig.RAPIConfig(url, call_back=call_back, return_fail=True)
        getter = ProcessFactory.create_getter(getter_config)
        async for good_items, bad_items in getter:
            if bad_items:
                # 下一层返回错误结果?
                return None, bad_items

            item = good_items[0]
            item = combine(item, prev_item)
            return [item], bad_items

    async def start(self):
        id_set = {...一堆id...}
        url_generator = (GetterConfig.RAPIConfig(base_url % (id_, ), call_back=fetch_next_day) for id_ in self.id_set)
        bulk_config = GetterConfig.RAPIBulkConfig(url_generator, concurrency=20, interval=1)
        bulk_getter = ProcessFactory.create_getter(bulk_config)
        with ProcessFactory.create_writer(...) as writer:
            async for items in bulk_getter:
                await writer.write(items)

    async def start_with_return_fail(self):
        id_set = {...一堆id...}
        url_generator = (GetterConfig.RAPIConfig(base_url % (id_, ), call_back=fetch_next_day) for id_ in self.id_set, return_fail=True)
        bulk_config = GetterConfig.RAPIBulkConfig(url_generator, concurrency=20, interval=1, return_fail=True)
        bulk_getter = ProcessFactory.create_getter(bulk_config)
        with ProcessFactory.create_writer(...) as writer:
            async for items in bulk_getter:
                await es_writer.write([i for i in good_items if i is not None])


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start())


##### persistent to disk

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    ### persistent -> 只有 RAPIBulkConfig 支持 persistent 参数，为 bool 值, 表示是否每隔 interval 时间间隔就将这个 bulk 里面已经做了的任务数持久化到硬盘，默认为 False, 表示不开启
    ### 如果 persistent 设置为 True, 比如传入一批任务给到 RAPIBulkConfig, 因为基于事件驱动的无阻塞请求，无法预测或者记录完成任务的顺序，不能简单的记录一个位置信息，所以采用如下方案，在当前目录下创建一个json文件，记录已经完成的任务的source的哈希值，当程序意外中断，下次程序启动时，会预先加载这个文件，重复的任务(source相同)将不会被执行，只会执行那些未被记录过的任务
    ### persistent_key -> 为json文件名称，用来定位是哪一批任务的记录，默认会以这一批任务的第一个任务(source)的哈希值作为 persistent_key, 如果不提供 persistent_key 参数，请保证两次程序的批量任务的第一个任务名称是相同的
    ### persistent_start_fresh_if_done -> 如果这一批任务全部做完，是否移除这个记录文件，如果不移除的话，所有任务都做完，并且所有任务都在记录文件中，下次启动程序的时候就有可能会出现没有任务可以做的情况，因为这一批任务全部匹配上了记录文件里面的任务, 默认为 True
    ### persistent_to_disk_if_give_up ->  每个任务除了成功，还有可能重试到默认重试次数后失败，这个值表示是否要把失败的任务记录到 记录文件 中，默认为 True

    async def exapmle():
        urls = [
            "http://xxx",
            "http://xxx",
            GetterConfig.RAPIConfig("http://xxx", persistent_to_disk_if_give_up=True)
        ]
        getter = ProcessFactory.create_getter(GetterConfig.RAPIBulkConfig(urls, persistent=True, persistent_to_disk_if_give_up=False))
        async for items in getter:
            print(items)

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


##### REDIS 基本示例

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig, WriterConfig

    async def example_simple():
        # 默认 key_type 是 Redis 的 LIST 结构
        # 你也可以指定 encoding 参数表示在写入 redis 之前进行什么编码，默认utf8
        json_lists = [...]
        wredis_config = WriterConfig.WRedisConfig("my_key")
        writer = ProcessFactory.create_writer(wredis_config)
        await writer.write(json_lists)

        # 获取异步redis客户端
        client = await wredis_config.get_redis_pool_cli()
        # 如果你用的是 getter_config, 你也可以通过如下方法获得 'getter_config.get_redis_pool_cli()'
        # 之后你就可以对 redis 进行操作
        r = await client.hset("xxx", "k1", "v1")
        print(r)

    async def example():
        # 指定 redis 的 key_type 为 HASH, 默认为 LIST
        # compress 参数表示数据在redis中被 zlib 压缩过，取出来要进行解压才能做json处理
        # 你可以指定 need_del 参数表示是否要在读取完数据后删除redis的key值, 默认 false
        # 你可以指定 "direction" 参数表示数据是从左往右读取还是从右往左读取(仅对 LIST 类型有效)
        getter_config = GetterConfig.RRedisConfig("my_key_hash", key_type="HASH", compress=True)
        async for items in reader:
            print(items)


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


#### ES 基本操作

##### 从ES读取数据

    import asyncio
	from idataapi_transform import ProcessFactory, GetterConfig

	async def example():
        # max_limit 表示最多读取多少条数据，不提供表示读取全部
        # 若要提供过滤条件，请提供 query_body 参数
        es_config = GetterConfig.RESConfig("post20170630", "news", max_limit=1000)
        es_getter = ProcessFactory.create_getter(es_config)
        async for items in es_getter:
            print(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

##### 写数据进ES

    import asyncio
	from idataapi_transform import ProcessFactory, WriterConfig

    def my_hash_func(item):
        # 按照 my_hash_func 规则生成 ES_ID
        # 也可以按照其他规则，往下看 从API读取并写数据进入ES 的示例
        return hashlib.md5(item["id"].encode("utf8")).hexdigest()

    async def example():
        json_lists = [#一堆json object]
        # actions 支持 create, index, update 默认 index
        # id_hash_func 是选填参数，可以不填则按照默认规则，往下看 从API读取并写数据进入ES 的示例
        es_config = WriterConfig.WESConfig("post20170630", "news", id_hash_func=my_hash_func)
        es_writer = ProcessFactory.create_writer(es_config)
        await es_writer.write(json_lists)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


##### 删除ES中的数据

    import asyncio
    import json
    from idataapi_transform import ProcessFactory, WriterConfig

	async def example():
        # 这个是封装了 delete_by_query 的API
        body = {"size": 100,  "query": {"bool": {"must": [{"term": {"createDate": "1516111225"}}]}}}
        writer = ProcessFactory.create_writer(WriterConfig.WESConfig("post20170630", "news"))
        r = await writer.delete_all(body=body)
        print(json.dumps(r))

	async def example_no_body():
        # 和上面的一样基于 delete_by_query，但是不提供body, 全部删除
        writer = ProcessFactory.create_writer(WriterConfig.WESConfig("post20170630", "news"))
        r = await writer.delete_all()
        print(json.dumps(r))

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

从API读取并写数据进入ES

    import time
    import asyncio
    from idataapi_transform import ProcessFactory, WriterConfig, GetterConfig

    """
    写入ES的数据都要产生一个 ES_ID
    本工具按照以下两个规则生成 ES_ID
    1）当用户创建 WESConfig 时提供了 id_hash_func 参数时，ES_ID 为 id_hash_func(item)
    2) 以上条件不满足，并且当 appCode 和 id 字段同时存在这个 item 里面时，ES_ID 为 md5(appCode_id)
    3) 以上条件均不满足时, ES_ID 为 md5(str(item))
    """

    # 全局变量，一会用
    now_ts = int(time.time())

	def my_filter(item):
        # 我是一个过滤器
        # 每一个 ProcessFactory.create 产生的 getter 或者 writer 都可以配置一个过滤器
        # 每一条数据在返回之前会经过这个过滤器，过滤器可以修改这条item, 也可以选择过滤这条item
        if "posterId" in item:
        	return item
        # 如果 retuen None 或者不 return 任何东西，则表示过滤这一条数据

    async def example():
        # urls 可以是任何可迭代对象, 列表，迭代器等, urls 里面的元素可以是一条 url, 也可以使配置好的 RAPIConfig 对象
        urls = ["http://xxxx", "http://xxxx", "http://xxxx", RAPIConfig("http://xxxx", max_limit=10)]
        # 安装过滤器，过滤掉所有没有 posterId 的对象
        api_bulk_config = GetterConfig.RAPIBulkConfig(urls, concurrency=100, filter_=my_filter)
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        # 也可以在这里安装过滤器
        # createDate 参数 为所有通过这个 es_writer 写入的数据创建同样的 createDate
        # 当然，你也可以忽略这个参数，此时 es_writer 会自动为没有createDate的每一条数据创建一个 createDate 并指定为当前系统时间， 可以设置 auto_insert_createDate=False 禁用自动创建
        # 设置 appCode 参数, 为每一条数据增加 appCode 从而可以以条件 1) 的方式生成 ES_ID
        # 你也可以不指定 appCode 参数, 并在过滤器中设置 appCode 从而满足条件 1)
        # 如果你不设置 appCode 并且数据中不含该关键字，则依次按条件 2) 3) 生成 ES_ID
        es_config = WriterConfig.WESConfig("profile201712", "user", createDate=now_ts, appCode="ifeng")
        with ProcessFactory.create_writer(es_config) as es_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                await es_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


获得ES Client

    import asyncio
    import json
	from idataapi_transform import ProcessFactory, WriterConfig

	async def example():
        writer = ProcessFactory.create_writer(WriterConfig.WESConfig("post20170630", "news"))
        client = writer.config.es_client
        # 一个基于 elasticsearch-async 的 client, 你可以看官方文档使用

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


其他的文件格式均提供了相同的Pattern, 相同的参数，直接使用便可

-------------------

#### 配置

##### 配置文件路径

默认情况下，程序按照以下顺序读取配置文件
* 自己指定的日志文件(往下看)
* ./idataapi-transform.ini
* ~/idataapi-transform.ini

如果以上文件都不存在，程序会自动创建以下文件并将其当成配置文件  **~/idataapi-transform.ini**

##### 运行时指定配置

默认情况下，程序会把日志格式化，输出到 **idataapi-transform.ini** 指定的目录下， 并且输出到终端
如果你不想要这些输出，或者不想要当前程序的日志格式

    from idataapi_transform import ManualConfig
    ManualConfig.disable_log()

如果你想要自己指定配置文件

    from idataapi_transform import ManualConfig
    ManualConfig.set_config("/Users/zpoint/Desktop/idataapi-transform.ini")

如果在程序运行时更改日志文件路径

    from idataapi_transform import ManualConfig
    # 每个日志文件最多只有5MB大小
    ManualConfig.set_log_path("/Users/zpoint/Desktop/logs/", 5242880)

-------------------

#### 说明

	from idataapi_transform import WriterConfig, GetterConfig

    # 用help命令查看各个Config支持的参数
    help(GetterConfig.RAPIConfig)
    """
    ...
    会访问直到没有下一页或者达到 max_limit 条数据为止

    :param source: API to get, i.e. "http://..."
    :param per_limit: 每一次迭代器返回会获取多少条数据
    :param max_limit: 最对获取多少条数据，如果不设置则获取全部
    :param max_retry: 如果某次请求失败，最多对该请求重试多少次后停止并报错
    :param random_min_sleep: 如果某次请求失败，最少随机休眠多少秒钟再进行下一次请求
    :param random_max_sleep: 如果某次请求失败，最多随机休眠多少秒钟再进行下一次请求
    :param session: aiohttp的会话，重复的HTTP长连接时可减少握手次数，提高性能
    :param args:
    :param kwargs:

    示例:
        api_config = GetterConfig.RAPIConfig("http://...")
        api_getter = ProcessFactory.create_getter(api_config)
        async for items in api_getter:
            print(items)
    ...
	"""


-------------------

#### ChangeLog
v 1.6.3 - 1.6.4
* persistent to disk
* debug mode support

v 1.5.1 - 1.6.1
* random sleep float seconds support
* es specific host, headers
* RAPIGetter HTTP POST support
* xlsx/csv headers, append mode support

v 1.4.7 - 1.5.1
* done_if param support
* manual success_ret_code config for user
* xlsxWriter replace ilegal characters automatically

v 1.4.4 - 1.4.6
* RAPIBulkGetter support async generator
* ini config relative path support, manual config support

v 1.4.3
* fix logging bug
* max_limit limit number of data before filter
* report_interval add for APIGetter

v 1.4.1
* call_back support
* mongodb auth support and motor 2.0 support
* mongodb support
* fix APIBulkGetter incompleted data
* 3.5 compatiable
* ESGetter get all data instead of half
* compatible with elasticsearch-async-6.1.0
* ESClient singleton

v.1.2.0
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

#### 许可

http://rem.mit-license.org