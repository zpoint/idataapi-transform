import logging
from .BaseWriter import BaseWriter

import asyncio
import confluent_kafka
from confluent_kafka import KafkaException
from threading import Thread
import json



class AIOProducer:
    def __init__(self, configs, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, value):
        """
        An awaitable produce method.
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)
        self._producer.produce(topic, value, on_delivery=ack)
        return result

    def produce2(self, topic, value, on_delivery):
        """
        A produce method in which delivery notifications are made available
        via both the returned future and on_delivery callback (if specified).
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(
                    result.set_result, msg)
            if on_delivery:
                self._loop.call_soon_threadsafe(
                    on_delivery, err, msg)
        self._producer.produce(topic, value, on_delivery=ack)
        return result


class KafkaWriter(BaseWriter):
    def __init__(self, config, topic, loop=None):
        super().__init__()
        self.topic = topic
        self.total_miss_count = 0
        self.success_count = 0
        self.producer = AIOProducer(configs={"bootstrap.servers": config.bootstrap_servers}, loop=loop)

    async def write(self, responses):
        for each_response in responses:
            if isinstance(each_response, dict):
                each_response = json.dumps(each_response, indent=2).encode('utf-8')
            await self.producer.produce(self.topic, each_response)
            self.success_count += 1
        logging.info("%s write %d item" % (self.topic, len(responses)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.close()
        logging.info("%s write done, total write %d item" %
                     (self.topic, self.success_count))

    def __enter__(self):
        return self
