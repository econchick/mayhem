#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

import asyncio
import functools
import logging
import random
import signal
import string
import uuid

import attr

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(msg)s')


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
        host_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
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

async def cleanup(msg, event):
    await event.wait()
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Done. Acked {msg}')

async def extend(msg, event):
    while not event.is_set():
        logging.info(f'Extended deadline by 3 seconds for {msg}')
        # want to sleep for less than the deadline amount
        await asyncio.sleep(2)

async def pull_message(queue):
    msg = await queue.get()
    logging.info(f'Pulled {msg}')

    event = asyncio.Event()

    save_coro = save(msg)
    restart_coro = restart_host(msg)

    asyncio.create_task(extend(msg, event))
    asyncio.create_task(cleanup(msg, event))
    await asyncio.gather(save_coro, restart_coro)
    event.set()

async def consume(queue):
    coroutines = set()
    while True:
        coro = pull_message(queue)
        coroutines.add(coro)
        logging.debug(f'Pulling for messages...')
        _, coroutines = await asyncio.wait(coroutines, timeout=0.1)

async def shutdown(signal, loop):
    logging.info(f'Received exit signal {signal.name}...')
    logging.info('Closing database connections')
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    for i, task in enumerate(tasks):
        task.cancel()

    logging.info('Cancelling outstanding tasks')
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()
    logging.info('Shutdown complete.')

if __name__ == '__main__':
    queue = asyncio.Queue()
    publisher_coro = publish(queue)
    consumer_coro = consume(queue)

    loop = asyncio.get_event_loop()

    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.ensure_future(shutdown(s, loop)))

    try:
        loop.create_task(publisher_coro)
        loop.create_task(consumer_coro)
        loop.run_forever()
    except Exception:
        logging.error('Caught exception')
    finally:
        logging.info('Cleaning up')
        loop.close()
