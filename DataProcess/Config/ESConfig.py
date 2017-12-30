import asyncio
import aiohttp
import hashlib
import json
from elasticsearch_async.connection import AIOHttpConnection as OriginAIOHttpConnection
from aiohttp.client_exceptions import ServerFingerprintMismatch
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, SSLError
from elasticsearch.compat import urlencode

from elasticsearch_async import AsyncElasticsearch

es_hosts = None


def init_es(hosts, es_headers):
    global es_hosts, AsyncElasticsearch
    es_hosts = hosts
    if not es_hosts:
        return False

    class AIOHttpConnection(OriginAIOHttpConnection):
        """
        Override default AIOHttpConnection.perform_request to add headers
        """

        @asyncio.coroutine
        def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=()):
            url_path = url
            if params:
                url_path = '%s?%s' % (url, urlencode(params or {}))
            url = self.base_url + url_path

            start = self.loop.time()
            response = None
            try:
                with aiohttp.Timeout(timeout or self.timeout):
                    response = yield from self.session.request(method, url, data=body, headers=es_headers)
                    raw_data = yield from response.text()
                duration = self.loop.time() - start

            except Exception as e:
                self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=e)
                if isinstance(e, ServerFingerprintMismatch):
                    raise SSLError('N/A', str(e), e)
                if isinstance(e, asyncio.TimeoutError):
                    raise ConnectionTimeout('TIMEOUT', str(e), e)
                raise ConnectionError('N/A', str(e), e)

            finally:
                if response is not None:
                    yield from response.release()

            # raise errors based on http status codes, let the client handle those if needed
            if not (200 <= response.status < 300) and response.status not in ignore:
                self.log_request_fail(method, url, url_path, body, duration, status_code=response.status,
                                      response=raw_data)
                self._raise_error(response.status, raw_data)

            self.log_request_success(method, url, url_path, body, response.status, raw_data, duration)

            return response.status, response.headers, raw_data

    class MyAsyncElasticsearch(AsyncElasticsearch):
        @staticmethod
        def default_id_hash_func(item):
            if "appCode" in item and item["appCode"] and "id" in item and item["id"]:
                value = (item["appCode"] + item["id"]).encode("utf8")
            else:
                value = str(item).encode("utf8")
            return hashlib.md5(value).hexdigest()

        async def add_dict_to_es(self, indices, doc_type, items, id_hash_func=None):
            if not id_hash_func:
                id_hash_func = self.default_id_hash_func
            body = ""
            for item in items:
                action = {
                    "index": {
                        "_index": indices,
                        "_type": doc_type,
                        "_id": id_hash_func(item)
                    }
                }
                body += json.dumps(action) + "\n" + json.dumps(item) + "\n"
            r = await self.transport.perform_request("POST", "/_bulk?pretty", body=body)
            return r

    if es_headers:
        OriginAIOHttpConnection.perform_request = AIOHttpConnection.perform_request

    AsyncElasticsearch = MyAsyncElasticsearch
    return True


def get_es_client():
    return AsyncElasticsearch(hosts=es_hosts)
