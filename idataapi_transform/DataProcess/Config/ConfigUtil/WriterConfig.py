from .BaseConfig import BaseWriterConfig
from ..ESConfig import get_es_client
from ..DefaultValue import DefaultVal


class WCSVConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 headers=None, filter_=None, expand=None, qsn=DefaultVal.qsn, **kwargs):
        super().__init__()
        self.filename = filename
        self.encoding = encoding
        self.mode = mode
        self.headers = headers
        self.filter = filter_
        self.expand = expand
        self.qsn = qsn


class WESConfig(BaseWriterConfig):
    def __init__(self, indices, doc_type, filter_=None, expand=None, id_hash_func=None, **kwargs):
        super().__init__()
        self.indices = indices
        self.doc_type = doc_type
        self.filter = filter_
        self.expand = expand
        self.id_hash_func = id_hash_func
        self.es_client = get_es_client()

    def __del__(self):
        self.es_client.transport.close()


class WJsonConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 expand=None, filter_=None, new_line=DefaultVal.new_line, **kwargs):
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.expand = expand
        self.filter = filter_
        self.new_line = new_line


class WTXTConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 expand=None, filter_=None, new_line=DefaultVal.new_line, join_val=DefaultVal.join_val, **kwargs):
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.expand = expand
        self.filter = filter_
        self.new_line = new_line
        self.join_val = join_val


class WXLSXConfig(BaseWriterConfig):
    def __init__(self, filename, title=DefaultVal.title, expand=None, filter_=None, **kwargs):
        super().__init__()
        self.filename = filename
        self.title = title
        self.expand = expand
        self.filter = filter_
