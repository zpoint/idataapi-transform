import os
import hashlib
from .MainConfig import main_config


class DefaultValObject(object):
    def __init__(self):
        self.refresh()

    def refresh(self):
        self.main_config = main_config()
        self.per_limit = self.main_config["main"].getint("per_limit")
        self.max_limit = self.main_config["main"].get("max_limit")
        if self.max_limit != "None":
            self.max_limit = int(self.max_limit)
        else:
            self.max_limit = None
        self.max_retry = self.main_config["main"].getint("max_retry")
        self.random_min_sleep = self.main_config["main"].getint("random_min_sleep")
        self.random_max_sleep = self.main_config["main"].getint("random_max_sleep")

        # redis
        self.redis_host = self.main_config["redis"].get("host")
        self.redis_port = self.main_config["redis"].getint("port")
        self.redis_db = self.main_config["redis"].get("db")
        self.redis_password = self.main_config["redis"].get("password")
        self.redis_timeout = self.main_config["redis"].getint("timeout")
        self.redis_encoding = self.main_config["redis"].get("encoding")
        self.redis_direction = self.main_config["redis"].get("direction")
        self.redis_compress = self.main_config["redis"].getboolean("compress")
        self.redis_need_del = self.main_config["redis"].getboolean("need_del")

        # mysql config
        self.mysql_host = self.main_config["mysql"].get("host")
        self.mysql_port = self.main_config["mysql"].getint("port")
        self.mysql_user = self.main_config["mysql"].get("user")
        self.mysql_password = self.main_config["mysql"].get("password")
        self.mysql_database = self.main_config["mysql"].get("database")
        self.mysql_encoding = self.main_config["mysql"].get("encoding")

        # mongo config
        self.mongo_host = self.main_config["mongo"].get("host")
        self.mongo_port = self.main_config["mongo"].getint("port")
        self.mongo_username = self.main_config["mongo"].get("username")
        self.mongo_password = self.main_config["mongo"].get("password")
        self.mongo_database = self.main_config["mongo"].get("database")

    default_file_mode_r = "r"
    default_file_mode_w = "w"
    default_encoding = "utf8"
    new_line = "\n"
    join_val = " "
    title = "example"
    qsn = None

    query_body = None
    dest_without_path = "result"
    dest = os.getcwd() + "/" + dest_without_path
    interval = 5
    concurrency = 50
    default_key_type = "LIST"
    report_interval = 10

    @staticmethod
    def default_id_hash_func(item):
        if "appCode" in item and item["appCode"] and "id" in item and item["id"]:
            value = (item["appCode"] + "_" + item["id"]).encode("utf8")
        else:
            value = str(item).encode("utf8")
        return hashlib.md5(value).hexdigest()


DefaultVal = DefaultValObject()
