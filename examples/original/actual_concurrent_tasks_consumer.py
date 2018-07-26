#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

import asyncio
import functools
import logging
import random
import uuid

import attr


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO
)

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

async def process(message):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Processed {message}')

async def pull_message(queue):
    message = await queue.get()
    logging.info(f'Pulled {message}')
    await process(message)

async def consume(queue):
    coroutines = set()
    while True:
        coro = pull_message(queue)
        coroutines.add(coro)
        _, coroutines = await asyncio.wait(coroutines, timeout=1)

if __name__ == '__main__':
    queue = asyncio.Queue()
    producer_coro = produce(queue)
    consumer_coro = consume(queue)

    loop = asyncio.get_event_loop()
    try:
        loop.create_task(producer_coro)
        loop.create_task(consumer_coro)
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info('Interrupted')
    finally:
        logging.info('Cleaning up')
        loop.stop()
