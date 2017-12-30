import logging
from .BaseWriter import BaseWriter
from ..Config.MainConfig import main_config


class ESWriter(BaseWriter):
    def __init__(self, config):
        if not main_config.has_es_configured:
            raise ValueError("You must config es_hosts before using ESWriter, Please edit configure file: %s" % (main_config.ini_path, ))

        super().__init__()
        self.config = config
        self.total_miss_count = 0
        self.success_count = 0

    async def write(self, responses):
        origin_length = len(responses)
        if self.config.filter:
            responses = [self.config.filter(i) for i in responses]
            responses = [i for i in responses if i]
        miss_count = origin_length - len(responses)
        self.total_miss_count += miss_count
        if responses:
            if self.config.expand:
                responses = [self.expand_dict(i) for i in responses]

            r = await self.config.es_client.add_dict_to_es(self.config.indices, self.config.doc_type, responses, self.config.id_hash_func)
            self.success_count += len(responses)
        else:
            r = None
        logging.info("Write %d items to index: %s, doc_type: %s" % (len(responses), self.config.indices, self.config.doc_type))
        return r

    async def delete_all(self):
        """
        inefficient delete
        """
        body = {
            "query": {
                "match_all": {}
            }
        }
        result = await self.config.es_client.delete_by_query(index=self.config.indices, doc_type=self.config.doc_type,
                                                             body=body)
        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info("%s->%s write done, total filtered %d item, total write %d item" %
                     (self.config.indices, self.config.doc_type, self.total_miss_count, self.success_count))
