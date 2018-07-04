import asyncio
import random
import logging
import traceback
from .BaseGetter import BaseGetter


class ESScrollGetter(BaseGetter):
    def __init__(self, config):
        super().__init__(self)
        self.config = config
        self.es_client = config.es_client

        self.total_size = None
        self.result = None
        self.scroll_id = None
        self.miss_count = 0
        self.total_count = 0

    def __aiter__(self):
        return self

    def init_val(self):
        self.total_size = None
        self.result = None
        self.scroll_id = None
        self.miss_count = 0
        self.total_count = 0

    async def __anext__(self, retry=1):
        if self.total_size is None:
            self.result = await self.es_client.search(self.config.indices, self.config.doc_type,
                                                      scroll=self.config.scroll, body=self.config.query_body)
            self.total_size = self.result['hits']['total']
            self.total_size = self.config.max_limit if (self.config.max_limit and self.config.max_limit < self.result['hits']['total']) else self.total_size
            self.total_count += len(self.result['hits']['hits'])
            logging.info("Get %d items from %s, percentage: %.2f%%" %
                         (len(self.result['hits']['hits']), self.config.indices + "->" + self.config.doc_type,
                          (self.total_count / self.total_size * 100) if self.total_size else 0))

            origin_length = len(self.result['hits']['hits'])
            if self.config.return_source:
                results = [i["_source"] for i in self.result['hits']['hits']]
            else:
                results = self.result
            if self.config.filter:
                results = [self.config.filter(i) for i in results]
                results = [i for i in results if i]
                self.miss_count += origin_length - len(results)
            self.get_score_id_and_clear_result()
            return results

        if self.scroll_id and self.total_count < self.total_size:
            try:
                self.result = await self.es_client.scroll(scroll_id=self.scroll_id,
                                                          scroll=self.config.scroll)
            except Exception as e:
                if retry < self.config.max_retry:
                    logging.error("retry: %d, %s" % (retry, str(e)))
                    await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                    return await self.__anext__(retry+1)
                else:
                    logging.error("Give up es getter, After retry: %d times, still fail to get result: %s, "
                                  "total get %d items, total filtered: %d items, reason: %s" %
                                  (self.config.max_retry, self.config.indices + "->" + self.config.doc_type,
                                   self.total_count, self.miss_count, traceback.format_exc()))
                    raise StopAsyncIteration

            logging.info("Get %d items from %s, filtered: %d items, percentage: %.2f%%" %
                         (len(self.result['hits']['hits']), self.config.indices + "->" + self.config.doc_type,
                          self.miss_count, (self.total_count / self.total_size * 100) if self.total_size else 0))

            origin_length = len(self.result['hits']['hits'])
            self.total_count += origin_length
            if self.config.return_source:
                results = [i["_source"] for i in self.result['hits']['hits']]
            else:
                results = self.result
            if self.config.filter:
                results = [self.config.filter(i) for i in results]
                results = [i for i in results if i]
                self.miss_count += origin_length - len(results)

            self.get_score_id_and_clear_result()
            if origin_length > 0:
                return results
            else:
                # if scroll empty item, means no more next page
                logging.info("empty result, terminating scroll, scroll id: %s" % (str(self.scroll_id), ))

        logging.info("get source done: %s, total get %d items, total filtered: %d items" %
                     (self.config.indices + "->" + self.config.doc_type, self.total_count, self.miss_count))
        self.init_val()
        raise StopAsyncIteration

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
                                                             body=body, params={"conflicts": "proceed"})
        return result

    def __iter__(self):
        raise ValueError("ESGetter must be used with async generator, not normal generator")

    def get_score_id_and_clear_result(self):
        if "_scroll_id" in self.result and self.result["_scroll_id"]:
            self.scroll_id = self.result["_scroll_id"]
        else:
            self.scroll_id = None
        self.result = dict()
