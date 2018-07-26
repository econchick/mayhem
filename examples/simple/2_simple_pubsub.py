#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

# adapted from http://asyncio.readthedocs.io/en/latest/producer_consumer.html
import asyncio
import logging
import random
import string

import attr


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
)


@attr.s
class PubSubMessage:
    msg_id = attr.ib(repr=False)
    instance_name = attr.ib()
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


async def publish(queue, n):
    for x in range(1, n + 1):
        host_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(msg_id=x, instance_name=instance_name)
        # publish an item
        logging.info(f'Published {x} of {n} messages')
        # put the item in the queue
        await queue.put(msg)

    # indicate the publisher is done
    await queue.put(None)


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        # simulate i/o operation using sleep
        await asyncio.sleep(random.random())


if __name__ == '__main__':
    queue = asyncio.Queue()
    publisher_coro = publish(queue, 5)
    consumer_coro = consume(queue)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(publisher_coro)
    loop.run_until_complete(consumer_coro)
