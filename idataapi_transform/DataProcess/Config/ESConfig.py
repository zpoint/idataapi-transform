import json
import time
import logging
from collections.abc import Iterable
from elasticsearch._async.transport import AsyncTransport as OriginAsyncTransport
from elasticsearch._async.client.utils import _make_path
from elasticsearch import TransportError
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
from elasticsearch import AsyncElasticsearch

es_hosts = None
http_auth = None


def init_es(hosts, es_headers, timeout_, http_auth_):
    global es_hosts, http_auth, AsyncElasticsearch, AsyncTransport
    es_hosts = hosts
    http_auth = tuple(http_auth_) if isinstance(http_auth_, Iterable) else None
    if not es_hosts:
        return False

    class MyAsyncTransport(OriginAsyncTransport):
        """
        Override default AsyncTransport to add timeout
        """
        async def perform_request(self, method, url, params=None, body=None, timeout=None, headers=None):
            await self._async_call()

            method, headers, params, body, ignore, __timeout = self._resolve_request_args(
                method, headers, params, body
            )

            for attempt in range(self.max_retries + 1):
                connection = self.get_connection()

                try:
                    status, headers, data = await connection.perform_request(
                        method,
                        url,
                        params,
                        body,
                        headers=headers,
                        ignore=ignore,
                        timeout=timeout,
                    )
                except TransportError as e:
                    if method == "HEAD" and e.status_code == 404:
                        return False

                    retry = False
                    if isinstance(e, ConnectionTimeout):
                        retry = self.retry_on_timeout
                    elif isinstance(e, ConnectionError):
                        retry = True
                    elif e.status_code in self.retry_on_status:
                        retry = True

                    if retry:
                        try:
                            # only mark as dead if we are retrying
                            self.mark_dead(connection)
                        except TransportError:
                            # If sniffing on failure, it could fail too. Catch the
                            # exception not to interrupt the retries.
                            pass
                        # raise exception on last retry
                        if attempt == self.max_retries:
                            raise e
                    else:
                        raise e

                else:
                    # connection didn't fail, confirm it's live status
                    self.connection_pool.mark_live(connection)

                    if method == "HEAD":
                        return 200 <= status < 300

                    if data:
                        data = self.deserializer.loads(data, headers.get("content-type"))
                    return data

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
                r = await self.transport.perform_request(
                    "POST", "/_bulk?pretty", body=body, timeout=timeout or timeout_, headers=self.headers or es_headers)
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

        async def search(
                self, body=None, index=None, doc_type=None, params=None, headers=None
        ):
            if "from_" in params:
                params["from"] = params.pop("from_")

            return await self.transport.perform_request(
                "POST",
                _make_path(index, doc_type, "_search"),
                params=params,
                headers=headers if headers else self.headers,
                body=body,
            )

    OriginAsyncTransport.perform_request = MyAsyncTransport.perform_request

    AsyncElasticsearch = MyAsyncElasticsearch
    return True


global_client = None


def get_es_client(hosts=None, headers=None):
    global global_client
    if not hosts:
        if global_client is None:
            global_client = AsyncElasticsearch(hosts=es_hosts, headers=headers, http_auth=http_auth)
        return global_client
    else:
        return AsyncElasticsearch(hosts=hosts, headers=headers, http_auth=http_auth)
