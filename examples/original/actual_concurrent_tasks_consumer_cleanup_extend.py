#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

import asyncio
import functools
import logging
import random
import string
import uuid

import attr


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO
)

@attr.s
class PubSubMessage:
    msg_id = attr.ib(repr=False)
    instance_name = attr.ib()
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'

async def produce(queue):
    while True:
        msg_id = str(uuid.uuid4())
        host_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(msg_id=msg_id, instance_name=instance_name)
        # produce an item
        logging.info(f'Published message {msg}')
        # put the item in the queue
        await queue.put(msg)
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())

async def restart_host(message):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Restarted {message.hostname}')

async def save(message):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Saved {message} into database')

async def cleanup(message):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Done. Acked {message}')

async def extend(message, event):
    while not event.is_set():
        logging.info(f'Extended deadline by 2 seconds for {message}')
        # want to sleep for less than the deadline amount
        await asyncio.sleep(1)
    else:
        await cleanup(message)

async def pull_message(queue):
    message = await queue.get()
    logging.info(f'Pulled {message}')

    event = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.create_task(extend(message, event))

    save_coro = save(message)
    restart_coro = restart_host(message)

    await asyncio.gather(save_coro, restart_coro)
    event.set()

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
