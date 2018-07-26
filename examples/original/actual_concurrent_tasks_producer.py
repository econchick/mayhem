#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

import asyncio
import functools
import logging
import random
import uuid

import attr

@attr.s
class PubSubMessage:
    msg_id = attr.ib()
    data = attr.ib(repr=False)

async def produce(queue):
    while True:
        msg_id = str(uuid.uuid4())
        msg = PubSubMessage(msg_id=msg_id, data='Hello, world')
        # produce an item
        logging.info(f'Published message {msg}')
        # put the item in the queue
        await queue.put(msg)
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO
    )

    queue = asyncio.Queue()
    producer_coro = produce(queue)

    loop = asyncio.get_event_loop()
    loop.create_task(producer_coro)
    loop.run_forever()
