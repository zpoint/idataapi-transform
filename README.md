# idataapi-transform

data transform toolkit for [IDataAPI](http://www.idataapi.cn/) for efficiency work

#### Requirment
* python version >= 3.5

#### Installation

	python3 -m pip install idataapi-transform
    # shell, run
    transform --help # explanation of each parameter and create configure file
    # edit ~/idataapi-transform.ini to config elasticsearch hosts and etc...

#### Command line interface

* Read data from **[API, ES, CSV, XLSX, JSON]**
* Write data to **[CSV, XLSX, JSON, TXT, ES]**

##### read data from API, convert to XLSX

will read all items from given api url, until no more next page, and save to dest(/Users/zpoint/Desktop/result.xlsx), **dest is optional, default is ./result.xlsx**

	transform API xlsx "http://xxx/post/dengta?kw=中国石化&apikey=xxx" "/Users/zpoint/Desktop/result"

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


#### build complex routine easily

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
        csv_config = WCSVConfig("./result.csv", encoding="utf8", headers=["likeCount", "id", "title"])
        with ProcessFactory.create_writer(csv_config) as csv_writer:
            async for items in es_getter:
                # do whatever you want with items
                csv_writer.write(items)

	if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())
