# idataapi-transform

[IDataAPI](http://www.idataapi.cn/)转换工具

* [English](https://github.com/zpoint/idataapi-transform/blob/master/README.md)

提供

* [命令行支持](#命令行支持)
* [Python模块支持](#Python模块支持)

可以从以下任意一种格式

 * **API(http)**
 * **ES(ElasticSearch)**
 * **CSV**
 * **XLSX**
 * **JSON**

转换为以下任意一种格式

 * **CSV**
 * **XLSX**
 * **JSON**
 * **TXT**
 * **ES**

* 通过asyncio提供异步支持
* 各模块间通过相同的方法调用

-------------------

### 目录

* [环境要求](#环境要求)
* [安装指南](#安装指南)
* [命令行支持及示例](#命令行支持及示例)
* [Python模块支持](#Python模块支持)
* [说明](#说明)
* [升级](#升级)
* [证书](#证书)

-------------------

#### 环境要求
* python 版本号 >= 3.5.2

-------------------

#### 安装指南

	python3 -m pip install idataapi-transform
    # 安装完成后在终端跑如下命令
    transform --help # 解释各个参数的作用以及创建默认的配置文件
    # 编辑配置文件 ~/idataapi-transform.ini 配置 ElasticSearch 主机, 端口, 默认并发数等参数

-------------------

#### 命令行支持及示例

* 从以下任意一格式读数据 **[API, ES, CSV, XLSX, JSON]**
* 写数据至以下任意一格式 **[CSV, XLSX, JSON, TXT, ES]**

##### 从 Elasticsearch 读取数据, 转换为 CSV 格式

从ES中 **index**: **knowledge20170517**, **doc_type**: **question**, 中最多读取 **500** 条数据, 写入到 ./result.csv

	transform ES csv "knowledge20170517:question" --max_limit=500

##### 从 API 读取数据, 转换为 XLSX 格式

会从提供的http请求读取所有数据(翻到最后一页为止), 并写入到 /Users/zpoint/Desktop/result.xlsx 中, **写入文件为可选参数, 可以不填, 默认参数是 ./result.xlsx**

	transform API xlsx "http://xxx/post/dengta?kw=中国石化&apikey=xxx" "/Users/zpoint/Desktop/result"

##### 从 JSON 文件读取数据, 转换为 CSV 格式

JSON 为一行一条数据的 JSON 文件
会从 /Users/zpoint/Desktop/a.json 读取数据, 并写入 **./result.csv** (默认参数)

	transform JSON csv "/Users/zpoint/Desktop/a.json"

##### 从 CSV 读取数据, 转换至 xlsx

会从 ./a.csv 读取数据, 并保存至 **./result.xlsx**

	transform CSV xlsx "./a.csv"


##### 从 Elasticsearch 读取数据, 转换至 CSV (复杂示例)
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

-------------------

#### Python模块支持

ES to csv

    import asyncio
    from idataapi_transform.DataProcess.ProcessFactory import ProcessFactory
    from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RESConfig
    from idataapi_transform.DataProcess.Config.ConfigUtil.WriterConfig import WCSVConfig

	async def example():
        body = {
            "size": 100,
            "_source": {
                "includes": ["likeCount", "id", "title"]
                }
        }
        es_config = RESConfig("post20170630", "news", max_limit=1000, query_body=body)
        es_getter = ProcessFactory.create_getter(es_config)
        csv_config = WCSVConfig("./result.csv")
        with ProcessFactory.create_writer(csv_config) as csv_writer:
            async for items in es_getter:
                # do whatever you want with items
                csv_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


API to xlsx

    import asyncio
    from idataapi_transform.DataProcess.ProcessFactory import ProcessFactory
    from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RAPIConfig
    from idataapi_transform.DataProcess.Config.ConfigUtil.WriterConfig import WXLSXConfig

	async def example():
        api_config = RAPIConfig("http://xxxx")
        getter = ProcessFactory.create_getter(api_config)
        xlsx_config = WXLSXConfig("./result.xlsx")
        with ProcessFactory.create_writer(xlsx_config) as xlsx_writer:
        	async for items in getter:
                # do whatever you want with items
                xlsx_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

CSV to xlsx

    import asyncio
    from idataapi_transform.DataProcess.ProcessFactory import ProcessFactory
    from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RCSVConfig
    from idataapi_transform.DataProcess.Config.ConfigUtil.WriterConfig import WXLSXConfig


    async def example():
        csv_config = RCSVConfig("./result.csv")
        getter = ProcessFactory.create_getter(csv_config)
        xlsx_config = WXLSXConfig("./result.xlsx")
        with ProcessFactory.create_writer(xlsx_config) as xlsx_writer:
            for items in getter:
                # do whatever you want with items
                xlsx_writer.write(items)

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


并发从不同的 API 读取数据, 并写入到 ES

    import asyncio
    from idataapi_transform.DataProcess.ProcessFactory import ProcessFactory
    from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RAPIBulkConfig
    from idataapi_transform.DataProcess.Config.ConfigUtil.WriterConfig import WESConfig


    async def example():
        # urls 可以是任何可迭代对象, 列表，迭代器等, urls 里面的元素可以是一条 url, 也可以使配置好的 RAPIConfig 对象
        urls = ["http://xxxx", "http://xxxx", "http://xxxx", RAPIConfig("http://xxxx", max_limit=10)]
        api_bulk_config = RAPIBulkConfig(urls, concurrency=100) # 指定并发数
        api_bulk_getter = ProcessFactory.create_getter(api_bulk_config)
        es_config = WESConfig("profile201712", "user")
        with ProcessFactory.create_writer(es_config) as es_writer:
            async for items in api_bulk_getter:
                # do whatever you want with items
                await es_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


-------------------

#### 说明

	from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RAPIConfig, RCSVConfig, RESConfig, RJsonConfig, RXLSXConfig, RAPUBulkConfig
    from idataapi_transform.DataProcess.Config.ConfigUtil.WriterConfig import WCSVConfig, WESConfig, WJsonConfig, WTXTConfig, WXLSXConfig

    # 用help命令查看各个Config支持的参数
    help(RAPIConfig)
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
        api_config = RAPIConfig("http://...")
        api_getter = ProcessFactory.create_getter(api_config)
        async for items in api_getter:
            print(items)
    ...
	"""


-------------------

#### 升级
v.1.0.1 && 1.0.2 && 1.0.3 && 1.0.4 && 1.0.5 && 1.0.6
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

#### 证书

http://rem.mit-license.org