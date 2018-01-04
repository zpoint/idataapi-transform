import os
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
    dest = os.getcwd() + "/result"
    interval = 5
    concurrency = 50
