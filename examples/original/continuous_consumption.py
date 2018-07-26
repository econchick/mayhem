#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

# adapted from http://asyncio.readthedocs.io/en/latest/producer_consumer.html
import asyncio
import random
import time


class PubSubMessage:
    def __init__(self, msg_id, expires=10):
        self.msg_id = msg_id
        self._expires = expires
        self.expiry = None
        self.published_at = None
        self.acked = False

    def publish(self):
        self.published_at = time.monotonic()
        self.expiry = self._published_at + self._expires

    def ack(self):
        self.acked = True

    def extend(self, seconds):
        self.expiry = self.expiry + seconds

    def is_expired(self):
        return time.monotonic() >= self.expiry


async def produce(queue, n):
    for x in range(1, n + 1):
        msg = PubSubMessage(msg_id=x,)
        # produce an item
        print('producing {}/{}'.format(x, n))
        # simulate i/o operation using sleep
        await asyncio.sleep(random.random())
        item = str(x)
        # put the item in the queue
        await queue.put(item)

    # indicate the producer is done
    await queue.put(None)


async def consume(queue):
    while True:
        # wait for an item from the producer
        item = await queue.get()
        if item is None:
            # the producer emits None to indicate that it is done
            break

        # process the item
        print('consuming item {}...'.format(item))
        # simulate i/o operation using sleep
        await asyncio.sleep(random.random())


queue = asyncio.Queue()
producer_coro = produce(queue, 10)
consumer_coro = consume(queue)
asyncio.run(producer_coro)
asyncio.run(consumer_coro)
