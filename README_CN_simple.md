# idataapi-transform



-------------------

#### 命令行支持及示例

* 从以下任意一格式读数据 **[API, ES, CSV, XLSX, JSON, Redis, MySQL, MongoDB]**
* 写数据至以下任意一格式 **[CSV, XLSX, JSON, TXT, ES, Redis, MySQL, MongoDB]**

##### 从 API 读取数据 转换为 XLSX 格式

会从提供的http请求读取所有数据(翻到最后一页为止), 并写入 **./result.xlsx** (默认参数)

	transform API xlsx "http://xxx/post/dengta?kw=中国石化&apikey=xxx"

##### 从 API 读取数据 转换为 XLSX 格式

会从提供的http请求读取所有数据(翻到最后一页为止), 并写入到 /Users/zpoint/Desktop/result.xlsx 中, **写入文件为可选参数, 可以不填, 默认参数是 ./result.xlsx**

	transform API xlsx "http://xxx/post/dengta?kw=中国石化&apikey=xxx" "/Users/zpoint/Desktop/result"

##### 从 API 读取数据 转换为 CSV 格式

会从提供的http请求读取所有数据(翻到最后一页为止), 并写入 **./result.csv** (默认参数)

	transform API csv "http://xxx/post/dengta?kw=中国石化&apikey=xxx"

##### 从 API 读取数据 转换为 CSV 格式

w_encoding 表示写入文件的编码，默认为 utf8
会从提供的http请求读取所有数据(翻到最后一页为止), 并写入 **./result.csv** (默认参数), ./result.csv 以 gbk 编码保存

	transform API csv "http://xxx/post/dengta?kw=中国石化&apikey=xxx" --w_encoding=gbk


##### 从 API 读取数据 转换为 JSON 格式

JSON 为一行一条数据的 JSON 文件
会从提供的http请求读取所有数据(翻到最后一页为止), 并写入 **./result.json** (默认参数)

	transform API json "http://xxx/post/dengta?kw=中国石化&apikey=xxx"

##### 从 API 读取数据 转换为 JSON 格式

max_limit 表示最多只获取到这么多条数据
会从提供的http请求读取所有数据(翻到最后一页或者获取到超过100条为止), 并写入 **./result.json** (默认参数)

	transform API json "http://xxx/post/dengta?kw=中国石化&apikey=xxx" --max_limit=100

##### 从 CSV 读取数据 转换至 xlsx

会从 ./a.csv 读取数据, 并保存至 **./result.xlsx**

	transform CSV xlsx "./a.csv"


##### 从 Elasticsearch 读取数据 转换至 CSV (复杂示例)
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

* 当表格不存在是自动创建

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
