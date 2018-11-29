import asyncio
import aiohttp
import json
import time
import logging
import copy
from elasticsearch_async.connection import AIOHttpConnection as OriginAIOHttpConnection
from elasticsearch_async.transport import AsyncTransport as OriginAsyncTransport
from elasticsearch_async.transport import ensure_future
from elasticsearch import TransportError
from aiohttp.client_exceptions import ServerFingerprintMismatch
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, SSLError
from elasticsearch.compat import urlencode
from elasticsearch.connection_pool import ConnectionPool, DummyConnectionPool


from elasticsearch_async import AsyncTransport
from elasticsearch_async import AsyncElasticsearch
from elasticsearch.client import _make_path, query_params

es_hosts = None

if hasattr(aiohttp, "Timeout"):
    async_timeout_func = aiohttp.Timeout
else:
    import async_timeout
    async_timeout_func = async_timeout.timeout


def init_es(hosts, es_headers, timeout_):
    global es_hosts, AsyncElasticsearch, AsyncTransport
    es_hosts = hosts
    if not es_hosts:
        return False

    class MyAsyncTransport(OriginAsyncTransport):
        """
        Override default AsyncTransport to add timeout
        """
        def perform_request(self, method, url, params=None, body=None, timeout=None, headers=None):
            if body is not None:
                body = self.serializer.dumps(body)

                # some clients or environments don't support sending GET with body
                if method in ('HEAD', 'GET') and self.send_get_body_as != 'GET':
                    # send it as post instead
                    if self.send_get_body_as == 'POST':
                        method = 'POST'

                    # or as source parameter
                    elif self.send_get_body_as == 'source':
                        if params is None:
                            params = {}
                        params['source'] = body
                        body = None

            if body is not None:
                try:
                    body = body.encode('utf-8')
                except (UnicodeDecodeError, AttributeError):
                    # bytes/str - no need to re-encode
                    pass

            ignore = ()
            if params:
                ignore = params.pop('ignore', ())
                if isinstance(ignore, int):
                    ignore = (ignore,)

            return ensure_future(self.main_loop(method, url, params, body,
                                                ignore=ignore,
                                                timeout=timeout, headers=headers),
                                 loop=self.loop)

        @asyncio.coroutine
        def main_loop(self, method, url, params, body, ignore=(), timeout=None, headers=None):
            for attempt in range(self.max_retries + 1):
                connection = self.get_connection()

                try:
                    status, headers, data = yield from connection.perform_request(
                        method, url, params, body, ignore=ignore, timeout=timeout, headers=headers)
                except TransportError as e:
                    if method == 'HEAD' and e.status_code == 404:
                        return False

                    retry = False
                    if isinstance(e, ConnectionTimeout):
                        retry = self.retry_on_timeout
                    elif isinstance(e, ConnectionError):
                        retry = True
                    elif e.status_code in self.retry_on_status:
                        retry = True

                    if retry:
                        # only mark as dead if we are retrying
                        self.mark_dead(connection)
                        # raise exception on last retry
                        if attempt == self.max_retries:
                            raise
                    else:
                        raise

                else:
                    if method == 'HEAD':
                        return 200 <= status < 300

                    # connection didn't fail, confirm it's live status
                    self.connection_pool.mark_live(connection)
                    if data:
                        data = self.deserializer.loads(data, headers.get('content-type'))
                    return data

    class AIOHttpConnection(OriginAIOHttpConnection):
        """
        Override default AIOHttpConnection.perform_request to add headers
        """

        @asyncio.coroutine
        def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=(), headers=None):
            url_path = url
            if params:
                url_path = '%s?%s' % (url, urlencode(params or {}))
            url = self.base_url + url_path

            start = self.loop.time()
            response = None
            local_headers = headers if headers else es_headers
            try:
                with async_timeout_func(timeout or timeout_ or self.timeout):
                    response = yield from self.session.request(method, url, data=body, headers=local_headers)
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
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if "headers" in kwargs:
                self.headers = kwargs["headers"]
            else:
                self.headers = None

        async def add_dict_to_es(self, indices, doc_type, items, id_hash_func, app_code=None, actions=None,
                                 create_date=None, error_if_fail=True, timeout=None, auto_insert_createDate=True):
            if not actions:
                actions = "index"
            body = ""
            for item in items:
                if app_code:
                    item["appCode"] = app_code
                if auto_insert_createDate and "createDate" not in item:
                    if create_date:
                        item["createDate"] = create_date
                    else:
                        item["createDate"] = int(time.time())

                action = {
                    actions: {
                        "_index": indices,
                        "_type": doc_type,
                        "_id": id_hash_func(item)
                    }
                }
                if actions == "update":
                    item = {"doc": item}
                body += json.dumps(action) + "\n" + json.dumps(item) + "\n"
            try:
                success = fail = 0
                r = await self.transport.perform_request("POST", "/_bulk?pretty", body=body, timeout=timeout, headers=self.headers)
                if r["errors"]:
                    for item in r["items"]:
                        for k, v in item.items():
                            if "error" in v:
                                if error_if_fail:
                                    # log error
                                    logging.error(json.dumps(v["error"]))
                                fail += 1
                            else:
                                success += 1
                else:
                    success = len(r["items"])
                return success, fail, r
            except Exception as e:
                import traceback
                logging.error(traceback.format_exc())
                logging.error("elasticsearch Exception, give up: %s" % (str(e), ))
                return None, None, None

        @query_params('_source', '_source_exclude', '_source_include',
                      'allow_no_indices', 'allow_partial_search_results', 'analyze_wildcard',
                      'analyzer', 'batched_reduce_size', 'default_operator', 'df',
                      'docvalue_fields', 'expand_wildcards', 'explain', 'from_',
                      'ignore_unavailable', 'lenient', 'max_concurrent_shard_requests',
                      'pre_filter_shard_size', 'preference', 'q', 'request_cache', 'routing',
                      'scroll', 'search_type', 'size', 'sort', 'stats', 'stored_fields',
                      'suggest_field', 'suggest_mode', 'suggest_size', 'suggest_text',
                      'terminate_after', 'timeout', 'track_scores', 'track_total_hits',
                      'typed_keys', 'version')
        def search(self, index=None, doc_type=None, body=None, params=None):
            # from is a reserved word so it cannot be used, use from_ instead
            if 'from_' in params:
                params['from'] = params.pop('from_')

            if doc_type and not index:
                index = '_all'
            return self.transport.perform_request('GET', _make_path(index, doc_type, '_search'), params=params, body=body, headers=self.headers)

        def __del__(self):
            """
            compatible with elasticsearch-async-6.1.0
            """
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.transport.close())
            except Exception:
                pass

    OriginAIOHttpConnection.perform_request = AIOHttpConnection.perform_request
    OriginAsyncTransport.perform_request = MyAsyncTransport.perform_request
    OriginAsyncTransport.main_loop = MyAsyncTransport.main_loop

    AsyncElasticsearch = MyAsyncElasticsearch
    return True


global_client = None


def get_es_client(hosts=None, headers=None):
    global global_client
    if not hosts:
        if global_client is None:
            global_client = AsyncElasticsearch(hosts=es_hosts)
        return global_client
    else:
        return AsyncElasticsearch(hosts=hosts, headers=headers)


"""
below add in version 1.0.9, compatible for "close()" ===> future object
"""


def close(self):
    try:
        asyncio.ensure_future(self.connection.close())
    except TypeError:
        pass


def connection_pool_close(self):
    for conn in self.orig_connections:
        try:
            asyncio.ensure_future(conn.close())
        except TypeError:
            pass


ConnectionPool.close = connection_pool_close
DummyConnectionPool.close = close
