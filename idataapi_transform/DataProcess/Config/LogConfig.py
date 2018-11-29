import os
import logging
from logging.handlers import RotatingFileHandler

format_str = "%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s"
date_formatter_str = '[%Y-%m-%d %H:%M:%S]'
formatter = logging.Formatter(format_str, datefmt=date_formatter_str)


class SingleLevelFilter(logging.Filter):
    def __init__(self, passlevel, reject):
        super(SingleLevelFilter, self).__init__()
        self.passlevel = passlevel
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return record.levelno != self.passlevel
        else:
            return record.levelno == self.passlevel


def init_log(log_dir, max_log_file_bytes, ini_path, manual=False):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    # console
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root_logger.addHandler(console)
    if log_dir:
        if not os.path.exists(log_dir):
            logging.error("log_dir(%s)%s not exists, I will not log to file" % (log_dir, "" if manual else " in configure file(%s)" % (ini_path, )))
            return False
        if not max_log_file_bytes:
            logging.error("log_byte not set, please %s, or I will not log to file" % ("pass log_byte as parameters" if manual else "configure log_byte in configure file(%s)" % (ini_path, )))
            return False
        # info
        h1 = RotatingFileHandler("%s/info.log" % (log_dir, ), mode="a", maxBytes=max_log_file_bytes,
                                 encoding="utf8", backupCount=1)
        h1.setFormatter(formatter)
        f1 = SingleLevelFilter(logging.INFO, False)
        h1.addFilter(f1)
        root_logger.addHandler(h1)

        # error
        h1 = RotatingFileHandler("%s/error.log" % (log_dir, ), mode="a", maxBytes=max_log_file_bytes,
                                 encoding="utf8", backupCount=1)
        h1.setFormatter(formatter)
        f1 = SingleLevelFilter(logging.ERROR, False)
        h1.addFilter(f1)
        root_logger.addHandler(h1)
        # logging.info("log dir set to: %s" % (log_dir, ))
        return True
    return False


def remove_log():
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
