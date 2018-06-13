# idataapi-transform

Toolkit for [IDataAPI](http://www.idataapi.cn/) for efficiency work

* [中文文档](https://github.com/zpoint/idataapi-transform/blob/master/README_CN.md)

Provide

* [Command line interface](#command-line-interface-example)
* [Python API](#build-complex-routine-easily)

You can read data from one of

 * **API(http)**
 * **ES(ElasticSearch)**
 * **CSV**
 * **XLSX**
 * **JSON**

and convert to

 * **CSV**
 * **XLSX**
 * **JSON**
 * **TXT**
 * **ES**

with asyncio support and share same API.

-------------------

### catalog

* [Requirment](#requirment)
* [Installation](#installation)
* [Command line interface Example](#command-line-interface-example)
* [Python API to build complex routine](#build-complex-routine-easily)
* [doc string](#doc-string)
* [Update](#ipdate)
* [License](#license)

-------------------

#### Requirment
* python version >= 3.5.2

-------------------

#### Installation

	python3 -m pip install idataapi-transform
    # shell, run
    transform --help # explanation of each parameter and create configure file
    # edit ~/idataapi-transform.ini to config elasticsearch hosts and etc...

-------------------

#### Command line interface Example

* Read data from **[API, ES, CSV, XLSX, JSON]**
* Write data to **[CSV, XLSX, JSON, TXT, ES]**

##### read data from Elasticsearch, convert to CSV

will read at most **500** items from given **index**: **knowledge20170517**, **doc_type**: **question**, and write to ./result.csv

	transform ES csv "knowledge20170517:question" --max_limit=500

##### read data from API, convert to XLSX

will read all items from given api url, until no more next page, and save to dest(/Users/zpoint/Desktop/result.xlsx), **dest is optional, default is ./result.xlsx**

	transform API xlsx "http://xxx/post/dengta?kw=中国石化&apikey=xxx" "/Users/zpoint/Desktop/result"

##### read data from JSON, convert to csv

will read items from json file, and save to **./result.csv**

	transform JSON csv "/Users/zpoint/Desktop/a.json"

##### read data from CSV, convert to xlsx

will read items from csv file, and save to **./result.xlsx**

	transform CSV xlsx "./a.csv"


##### read data from Elasticsearch, convert to CSV
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

-------------------

#### build complex routine easily

Read data from ES

    import asyncio
    from idataapi_transform.DataProcess.ProcessFactory import ProcessFactory
    from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RESConfig

	async def example():
    	# max_limit 表示最多读取多少条数据，不提供表示读取全部
        # 若要提供过滤条件，请提供 query_body 参数
        es_config = RESConfig("post20170630", "news", max_limit=1000)
        es_getter = ProcessFactory.create_getter(es_config)
        async for items in es_getter:
            print(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())

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


concurrent read lots of items from API, to ES

    import asyncio
    from idataapi_transform.DataProcess.ProcessFactory import ProcessFactory
    from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RAPIBulkConfig
    from idataapi_transform.DataProcess.Config.ConfigUtil.WriterConfig import WESConfig


    async def example():
        # urls can be any iterable object, each item can be api url or RAPIConfig
        urls = ["http://xxxx", "http://xxxx", "http://xxxx", ...]
        api_bulk_config = RAPIBulkConfig(urls, concurrency=100)
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

#### doc string

	from idataapi_transform.DataProcess.Config.ConfigUtil.GetterConfig import RAPIConfig, RCSVConfig, RESConfig, RJsonConfig, RXLSXConfig, RAPUBulkConfig
    from idataapi_transform.DataProcess.Config.ConfigUtil.WriterConfig import WCSVConfig, WESConfig, WJsonConfig, WTXTConfig, WXLSXConfig

    # run help on config to see detail
    help(RAPIConfig)
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