from .BaseConfig import BaseGetterConfig

from ..ESConfig import get_es_client
from ..DefaultValue import DefaultVal
from ..ConnectorConfig import session_manger


class RAPIConfig(BaseGetterConfig):
    def __init__(self, source, per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit,
                 max_retry=DefaultVal.max_retry, random_min_sleep=DefaultVal.random_min_sleep,
                 random_max_sleep=DefaultVal.random_max_sleep, session=None, *args, **kwargs):
        super().__init__()
        self.source = source
        self.per_limit = per_limit
        self.max_limit = max_limit
        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep
        self.session = session_manger.get_session() if not session else session


class RCSVConfig(BaseGetterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_r, encoding=DefaultVal.default_encoding,
                 per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit, **kwargs):
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.per_limit = per_limit
        self.max_limit = max_limit


class RESConfig(BaseGetterConfig):
    def __init__(self, indices, doc_type, per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit,
                 scroll="1m", query_body=None, return_source=True, max_retry=DefaultVal.max_retry,
                 random_min_sleep=DefaultVal.random_min_sleep, random_max_sleep=DefaultVal.random_max_sleep, **kwargs):
        super().__init__()
        if not query_body:
            query_body = {
                "size": per_limit,
                "query": {
                    "match_all": {}
                }
            }
        self.query_body = query_body
        self.indices = indices
        self.doc_type = doc_type
        self.per_limit = per_limit
        self.max_limit = max_limit
        self.scroll = scroll
        self.es_client = get_es_client()
        self.return_source = return_source
        self.max_retry = max_retry
        self.random_min_sleep = random_min_sleep
        self.random_max_sleep = random_max_sleep

    def __del__(self):
        self.es_client.transport.close()


class RJsonConfig(BaseGetterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_r, encoding=DefaultVal.default_encoding,
                 per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit,
                 **kwargs):
        super().__init__()
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.per_limit = per_limit
        self.max_limit = max_limit


class RXLSXConfig(BaseGetterConfig):
    def __init__(self, filename, per_limit=DefaultVal.per_limit, max_limit=DefaultVal.max_limit, **kwargs):
        super().__init__()
        self.filename = filename
        self.per_limit = per_limit
        self.max_limit = max_limit
