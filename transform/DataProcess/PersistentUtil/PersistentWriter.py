import os
import json
import time
import hashlib
import logging

class PersistentWriter(object):
    def __init__(self, persistent_key):
        self.f_name = persistent_key + ".json"
        self.latest_record = set()
        self.load_last_record()
        self.f_out = open(self.f_name, "a+", encoding="utf8")
        self.prev_latest_record_num = len(self.latest_record)

    def load_last_record(self):
        if os.path.exists(self.f_name):
            try:
                with open(self.f_name, "r", encoding="utf8") as f:
                    self.latest_record = set(json.loads(f.read())["record"])
            except Exception:
                logging.error("Broken record file: %s, recreating file" % (self.f_name, ))
                self.remove_file()

    def write(self):
        if len(self.latest_record) == self.prev_latest_record_num:
            return
        else:
            self.prev_latest_record_num = len(self.latest_record)

        self.truncate()
        self.f_out.seek(0)
        ts = int(time.time())
        struct_time = time.localtime(ts)
        dt = time.strftime('%Y-%m-%d %H:%M:%S', struct_time)
        record = {
            "record": list(self.latest_record),
            "record_length": len(self.latest_record),
            "timestamp": ts,
            "date": dt,
            "filename": self.f_name
        }
        self.f_out.write(json.dumps(record))
        logging.info("persistent to disk, f_name: %s, total_task_num: %d" % (self.f_name, len(self.latest_record)))

    def add(self, key):
        key = hashlib.md5(key.encode("utf8")).hexdigest()
        self.latest_record.add(key)

    def __contains__(self, item):
        key = hashlib.md5(item.encode("utf8")).hexdigest()
        return key in self.latest_record

    def sync(self):
        self.f_out.flush()

    def remove_file(self):
        os.unlink(self.f_name)

    def truncate(self):
        self.f_out.truncate(0)

    def clear(self, start_fresh_if_done):
        self.latest_record = None
        if start_fresh_if_done:
            self.remove_file()
