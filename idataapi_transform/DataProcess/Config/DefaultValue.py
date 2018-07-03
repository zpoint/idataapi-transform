import os
import hashlib
from .MainConfig import main_config

base_config = main_config()


class DefaultVal(object):
    per_limit = base_config["main"].getint("per_limit")
    max_limit = base_config["main"].get("max_limit")
    if max_limit != "None":
        max_limit = int(max_limit)
    else:
        max_limit = None

    max_retry = base_config["main"].getint("max_retry")
    random_min_sleep = base_config["main"].getint("random_min_sleep")
    random_max_sleep = base_config["main"].getint("random_max_sleep")

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

    @staticmethod
    def default_id_hash_func(item):
        if "appCode" in item and item["appCode"] and "id" in item and item["id"]:
            value = (item["appCode"] + "_" + item["id"]).encode("utf8")
        else:
            value = str(item).encode("utf8")
        return hashlib.md5(value).hexdigest()
