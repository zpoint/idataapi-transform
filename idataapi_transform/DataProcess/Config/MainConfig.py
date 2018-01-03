import os
import json
import configparser
from os.path import expanduser
from .LogConfig import init_log
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

[log]
# a directory to save log file
# path = /Users/zpoint/Desktop/idataapi-transform/logs/

# max byte per log file
# log_byte = 5242880
"""


class MainConfig(object):
    def __init__(self, ini_path=None):
        # singleton
        if not hasattr(self, "__instance"):
            self.ini_path = ini_path
            if not self.ini_path:
                home = expanduser("~")
                self.ini_path = home + "/idataapi-transform.ini"

            if not os.path.exists(self.ini_path):
                with open(self.ini_path, "w") as f:
                    f.write(default_configure_content)

            self.__instance = configparser.ConfigParser()
            self.__instance.read(self.ini_path)
            MainConfig.__instance = self.__instance

            self.has_log_file = self.config_log()
            self.has_es_configured = self.config_es()

    def __call__(self):
        return self.__instance

    def config_log(self):
        max_log_file_bytes = self.__instance["log"].getint("log_byte")
        log_path = self.__instance["log"].get("path")
        return init_log(log_path, max_log_file_bytes, self.ini_path)

    def config_es(self):
        hosts = self.__instance["es"].get("hosts")
        timeout = self.__instance["es"].getint("timeout")
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

        else:
            headers = None
        return init_es(hosts, headers, timeout)


main_config = MainConfig()
