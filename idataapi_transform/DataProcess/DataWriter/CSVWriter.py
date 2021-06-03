import os
import csv
import types
import logging
from .BaseWriter import BaseWriter


class CSVWriter(BaseWriter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.file_already_exists = os.path.exists(self.config.filename) and os.path.getsize(self.config.filename)
        self.f_out = open(self.config.filename, self.config.mode, encoding=self.config.encoding, newline="")
        self.f_csv = None
        self.headers = dict() if not self.config.headers else self.config.headers
        self.total_miss_count = 0
        self.success_count = 0
        # logging.info("self.config.quotechar: %s, %s", self.config.quotechar, repr(self.config.quotechar))

    def write(self, responses):
        miss_count = 0

        # filter
        if self.config.filter:
            new_result = list()
            for each_response in responses:
                each_response = self.config.filter(each_response)
                if not each_response:
                    miss_count += 1
                    continue
                new_result.append(each_response)
            responses = new_result
            self.total_miss_count += miss_count

        # all filtered
        if not responses:
            logging.info("%s write 0 item, filtered %d item" % (self.config.filename, miss_count))
            return

        # expand
        if self.config.expand:
            responses = [self.expand_dict(i, max_expand=self.config.expand) for i in responses]
        else:
            responses = [i for i in responses] if isinstance(responses, types.GeneratorType) else responses

        # headers
        if not self.f_csv:
            if "a" in self.config.mode and self.file_already_exists:
                self.headers = self.generate_headers(responses, append_mode=True)
                self.f_csv = csv.DictWriter(self.f_out, self.headers, quotechar=self.config.quotechar)
            else:
                if not self.headers:
                    self.headers = self.generate_headers(responses)
                self.f_csv = csv.DictWriter(self.f_out, self.headers, quotechar=self.config.quotechar)
                self.f_csv.writeheader()


        # encoding process
        for each_response in responses:
            for k, v in each_response.items():
                if v is None:
                    each_response[k] = ""

                elif self.config.qsn and v != "" and (isinstance(v, (int, float)) or isinstance(v, str) and all(i.isdigit() for i in v)):
                    each_response[k] = repr(str(v))

                elif self.config.encoding not in ("utf8", "utf-8"):
                    each_response[k] = str(v).encode(self.config.encoding, "ignore").decode(self.config.encoding)

            self.success_count += 1
            self.f_csv.writerow(each_response)
        logging.info("%s write %d item, filtered %d item" % (self.config.filename, len(responses), miss_count))

    def generate_headers(self, responses, append_mode=False):
        headers = set()
        for r in responses:
            for key in r.keys():
                headers.add(key)

        if append_mode:
            f_in = open(self.config.filename, "r", encoding=self.config.encoding, newline="")
            reader = csv.DictReader(f_in)
            exists_fields = reader.fieldnames
            if set(exists_fields) != headers:
                raise ValueError("append mode for csv file: %s, but header field mismatch, exist fields: %s, generated fields: %s" % (self.config.filename, repr(exists_fields), repr(headers)))
            return exists_fields
        return list(headers)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f_out.close()
        logging.info("%s write done, total filtered %d item, total write %d item" %
                     (self.config.filename, self.total_miss_count, self.success_count))
