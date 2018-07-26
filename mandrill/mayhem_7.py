#!/usr/bin/env python3.7

# multiple publishers

import asyncio
import functools
import logging
import random
import string
import uuid

import attr

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


@attr.s
class PubSubMessage:
    msg_id = attr.ib(repr=False)
    instance_name = attr.ib()
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'

async def publish(queue, publisher_id):
    while True:
        msg_id = str(uuid.uuid4())
        choices = string.ascii_lowercase + string.digits
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(msg_id=msg_id, instance_name=instance_name)
        # publish an item
        logging.info(f'[{publisher_id}] Published message {msg}')
        # put the item in the queue
        await queue.put(msg)
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())

async def handle_exception(coro, loop):
    try:
        await coro
    except Exception:
        logging.error('Caught exception')
        loop.stop()

if __name__ == '__main__':
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    coros = [handle_exception(publish(queue, i), loop) for i in range(1, 4)]

    try:
        [loop.create_task(coro) for coro in coros]
        loop.run_forever()
    # don't actually do this!
    except KeyboardInterrupt:
        logging.info('Interrupted')
    finally:
        logging.info('Cleaning up')
        loop.stop()
