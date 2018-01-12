from .BaseConfig import BaseWriterConfig
from ..ESConfig import get_es_client
from ..DefaultValue import DefaultVal


class WCSVConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 headers=None, filter_=None, expand=None, qsn=DefaultVal.qsn, **kwargs):
        """
        :param filename: filename to write
        :param mode: file open mode, i.e "w" or "a+"
        :param encoding: file encoding i.e "utf8"
        :param headers: csv headers in first row, if not set, automatically extract in first bulk of items
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param qsn: run "transform --help" to see command line interface explanation for detail
        :param kwargs:

        Example:
            ...
            csv_config = WCSVConfig("./result.csv", encoding="utf8", headers=["likeCount", "id", "title"])
            with ProcessFactory.create_writer(csv_config) as csv_writer:
                async for items in es_getter:
                    # do whatever you want with items
                    csv_writer.write(items)
        """
        super().__init__()
        self.filename = filename
        self.encoding = encoding
        self.mode = mode
        self.headers = headers
        self.filter = filter_
        self.expand = expand
        self.qsn = qsn


class WESConfig(BaseWriterConfig):
    def __init__(self, indices, doc_type, filter_=None, expand=None, id_hash_func=None, appCode=None,
                 actions=None, createDate=None, error_if_fail=True, timeout=None, **kwargs):
        """
        :param indices: elasticsearch indices
        :param doc_type: elasticsearch doc_type
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param id_hash_func: function to generate id_ for each item
        :param appCode: if not None, add appCode to each item before write to es
        :param actions: if not None, will set actions to user define actions, else default actions is 'index'
        :param appCode: if not None, add createDate to each item before write to es
        :param error_if_fail: if True, log to error if fail to insert to es, else log nothing
        :param timeout: http connection timeout when connect to es, seconds
        :param kwargs:

        Example:
            ...
            es_config = WCSVConfig("post20170630", "news")
            with ProcessFactory.create_writer(es_config) as es_writer:
                    # asyncio function must call with await
                    await csv_writer.write(items)
        """
        super().__init__()
        self.indices = indices
        self.doc_type = doc_type
        self.filter = filter_
        self.expand = expand
        self.id_hash_func = id_hash_func
        self.es_client = get_es_client()
        self.app_code = appCode
        self.actions = actions
        self.create_date = createDate
        self.error_if_fail = error_if_fail
        self.timeout = timeout

    def __del__(self):
        self.es_client.transport.close()


class WJsonConfig(BaseWriterConfig):
    def __init__(self, filename, mode=DefaultVal.default_file_mode_w, encoding=DefaultVal.default_encoding,
                 expand=None, filter_=None, new_line=DefaultVal.new_line, **kwargs):
        """
        :param filename: filename to write
        :param mode: file open mode, i.e "w" or "a+"
        :param encoding: file encoding i.e "utf8"
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param new_line: new_line seperator for each item, default is "\n"
        :param kwargs:

        Example:
            ...
            json_config = WJsonConfig("./result.json")
            with ProcessFactory.create_writer(csv_config) as json_writer:
                async for items in es_getter:
                    json_writer.write(items)
        """
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
        """
        :param filename: filename to write
        :param mode: file open mode, i.e "w" or "a+"
        :param encoding: file encoding i.e "utf8"
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param new_line: new_line seperator for each item, default is "\n"
        :param join_val: space seperator for each key in each item, default is " "
        :param kwargs:

        Example:
            ...
            txt_config = WTXTConfig("./result.txt")
            with ProcessFactory.create_writer(txt_config) as txt_writer:
                async for items in es_getter:
                    txt_writer.write(items)
        """
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
        """
        :param filename: filename to write
        :param title: sheet title
        :param expand: run "transform --help" to see command line interface explanation for detail
        :param filter_: run "transform --help" to see command line interface explanation for detail
        :param kwargs:

        Example:
            ...
            xlsx_config = WXLSXConfig("./result.xlsx")
            with ProcessFactory.create_writer(xlsx_config) as xlsx_writer:
                async for items in es_getter:
                    xlsx_writer.write(items)
        """
        super().__init__()
        self.filename = filename
        self.title = title
        self.expand = expand
        self.filter = filter_
