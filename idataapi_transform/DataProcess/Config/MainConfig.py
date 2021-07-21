import os
import json
import configparser
from os.path import expanduser
from .LogConfig import init_log, remove_log
from .ESConfig import init_es


default_configure_content = """
[main]
# default max concurrency value for APIGetter
concurrency = 50

# buffer size
per_limit = 100

# fetch at most max_limit items
max_limit = None

# max retry for getter before give up if fail to get data
max_retry = 3

# sleep interval if fail
random_min_sleep = 1
random_max_sleep = 3

[es]
# elasticsearch host
# hosts = ["localhost:9393"]

# elasticsearch headers when perform http request
# headers = {"Host": "localhost", "value": "value"}

# request timeout, seconds
# timeout = 10

# http auth
# http_auth = ["user", "passwd"]

[log]
# a directory to save log file
# path = /Users/zpoint/Desktop/idataapi-transform/logs/

# max byte per log file
# log_byte = 5242880
"""

redis_config_content = """
[redis]
host = localhost
port = 0
db = 0
password = 
timeout = 3
encoding = utf8
# whether need to del the key after get object from redis, 0 means false, 1 means true
need_del = 0
# default direction when read/write , "L" means lpop/lpush, "R" means rpop/rpush
direction = L
"""

mysql_config_content = """
[mysql]
host = localhost
port = 0
user = root
password = 
database = 
# default charset
encoding = utf8
"""

mongo_config_content = """
[mongo]
protocal = mongodb  # or mongodb+srv
host = localhost
port = 0
username = 
password = 
database = test_database
other_params = 
"""

kafka_config_content = """
[kafka]
bootstrap.servers = localhost:9092
"""

main_config_box = None


class MainConfig(object):
    def __init__(self, ini_path=None):
        global main_config_box
        main_config_box = self
        # singleton
        if not hasattr(self, "__instance"):
            if not ini_path:
                home = expanduser("~")
                ini_path = home + "/idataapi-transform.ini"

            if not os.path.exists(ini_path):
                with open(ini_path, "w") as f:
                    f.write(default_configure_content + redis_config_content + mysql_config_content + mongo_config_content)

            if os.path.exists("./idataapi-transform.ini"):
                ini_path = "./idataapi-transform.ini"

            self.read_config(ini_path)

    def read_config(self, ini_path):
        self.ini_path = ini_path
        self.__instance = configparser.ConfigParser()

        self.__instance.read(ini_path)
        MainConfig.__instance = self.__instance

        self.has_log_file = self.__instance.has_log_file = self.config_log()
        self.has_es_configured = self.__instance.has_es_configured = self.config_es()
        self.has_redis_configured = self.__instance.has_redis_configured = self.config_redis()
        self.has_mysql_configured = self.__instance.has_mysql_configured = self.config_mysql()
        self.has_mongo_configured = self.__instance.has_mongo_configured = self.config_mongo()
        self.has_kafka_configured = self.__instance.has_kafka_configured = self.config_kafka()

        self.__instance.ini_path = self.ini_path

    def __call__(self):
        return self.__instance

    def config_log(self, log_path=None, max_log_file_bytes=None):
        remove_log()
        if log_path:
            manual = True
        else:
            max_log_file_bytes = self.__instance["log"].getint("log_byte")
            log_path = self.__instance["log"].get("path")
            manual = False
        return init_log(log_path, max_log_file_bytes, self.ini_path, manual=manual)

    def config_es(self):
        hosts = self.__instance["es"].get("hosts")
        timeout = self.__instance["es"].getint("timeout")
        http_auth = self.__instance["es"].get("http_auth")
        if hosts:
            try:
                hosts = json.loads(hosts)
            except Exception as e:
                raise ValueError("es host must be json serialized")

        headers = self.__instance["es"].get("headers")
        if headers and headers != "None":
            try:
                headers = json.loads(headers)
            except Exception as e:
                raise ValueError("es headers must be json serialized")
        if http_auth and http_auth != "None":
            try:
                http_auth = json.loads(http_auth)
            except Exception as e:
                raise ValueError("es http_auth must be json serialized")
        else:
            headers = None
        return init_es(hosts, headers, timeout, http_auth)

    def config_redis(self):
        try:
            self.__instance["redis"].get("port")
        except KeyError as e:
            with open(self.ini_path, "a+") as f:
                f.write(redis_config_content)
            self.__instance.read(self.ini_path)

        port = self.__instance["redis"].getint("port")
        return port > 0

    def config_mysql(self):
        try:
            self.__instance["mysql"].get("port")
        except KeyError as e:
            with open(self.ini_path, "a+") as f:
                f.write(mysql_config_content)
            self.__instance.read(self.ini_path)

        port = self.__instance["mysql"].getint("port")
        return port > 0

    def config_mongo(self):
        try:
            self.__instance["mongo"].get("port")
        except KeyError as e:
            with open(self.ini_path, "a+") as f:
                f.write(mongo_config_content)
            self.__instance.read(self.ini_path)

        port = self.__instance["mongo"].getint("port")
        return port > 0

    def config_kafka(self):
        try:
            self.__instance["kafka"].get("bootstrap.servers")
        except KeyError as e:
            with open(self.ini_path, "a+") as f:
                f.write(kafka_config_content)
            self.__instance.read(self.ini_path)

        return "bootstrap.servers" in  self.__instance["kafka"]


main_config = MainConfig()
