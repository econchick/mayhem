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

async def produce(queue, producer_id):
    while True:
        msg_id = str(uuid.uuid4())
        msg = PubSubMessage(msg_id=msg_id, data='Hello, world')
        # produce an item
        logging.info(f'Producer {producer_id} published message {msg}')
        # put the item in the queue
        await queue.put(msg)
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO
    )

    queue = asyncio.Queue()
    producer_coro1 = produce(queue, 1)
    producer_coro2 = produce(queue, 2)
    producer_coro3 = produce(queue, 3)

    loop = asyncio.get_event_loop()
    try:
        loop.create_task(producer_coro1)
        loop.create_task(producer_coro2)
        loop.create_task(producer_coro3)
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info('Interrupted')
    finally:
        logging.info('Cleaning up')
        loop.stop()
