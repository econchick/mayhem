#!/usr/bin/env python3.7

# monitor tasks

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

async def publish(queue):
    while True:
        msg_id = str(uuid.uuid4())
        choices = string.ascii_lowercase + string.digits
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(msg_id=msg_id, instance_name=instance_name)
        # publish an item
        logging.debug(f'Published message {msg}')
        # put the item in the queue
        await queue.put(msg)
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())

async def restart_host(msg):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.randrange(1,3))
    logging.info(f'Restarted {msg.hostname}')

async def save(msg):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Saved {msg} into database')

async def cleanup(msg):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Done. Acked {msg}')

async def extend(msg, event):
    while not event.is_set():
        logging.info(f'Extended deadline by 3 seconds for {msg}')
        # want to sleep for less than the deadline amount
        await asyncio.sleep(2)
    else:
        await cleanup(msg)

async def handle_message(msg):
    event = asyncio.Event()
    asyncio.create_task(extend(msg, event))

    save_coro = save(msg)
    restart_coro = restart_host(msg)

    await asyncio.gather(save_coro, restart_coro)
    event.set()

async def consume(queue):
    while True:
        msg = await queue.get()
        logging.info(f'Pulled {msg}')
        asyncio.create_task(handle_message(msg))

async def handle_exception(fn, loop):
    try:
        await fn()
    except Exception:
        logging.error('Caught exception')
        loop.stop()

if __name__ == '__main__':
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    publisher_fn = functools.partial(publish, queue)
    consumer_fn = functools.partial(consume, queue)
    publisher_coro = handle_exception(publisher_fn, loop)
    consumer_coro = handle_exception(consumer_fn, loop)

    try:
        loop.create_task(publisher_coro)
        loop.create_task(consumer_coro)
        loop.run_forever()
    # don't actually do this!
    except KeyboardInterrupt:
        logging.info('Interrupted')
    finally:
        logging.info('Cleaning up')
        loop.stop()
