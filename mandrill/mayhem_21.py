#!/usr/bin/env python3

"""
Notice! This requires: google-cloud-pubsub==0.35.4
"""

# working w threaded sync code - asyncio.create_task

import asyncio
import concurrent.futures
import json
import logging
import os
import random
import signal
import string
import threading

from google.cloud import pubsub


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


TOPIC = 'projects/europython18/topics/ep18-topic'
SUBSCRIPTION = 'projects/europython18/subscriptions/ep18-sub'
PROJECT = 'europython18'
CHOICES = string.ascii_lowercase + string.digits


def get_publisher():
    client = pubsub.PublisherClient()
    try:
        client.create_topic(TOPIC)
    except Exception as e:
        # already created
        pass

    return client


def get_subscriber():
    client = pubsub.SubscriberClient()
    try:
        client.create_subscription(SUBSCRIPTION, TOPIC)
    except Exception:
        # already created
        pass
    return client


def publish_sync(publisher):
    for msg in range(1, 6):
        msg_data = {'msg_id': ''.join(random.choices(CHOICES, k=4))}
        bytes_message = bytes(json.dumps(msg_data), encoding='utf-8')
        publisher.publish(TOPIC, bytes_message)
        logging.debug(f'Published {msg_data["msg_id"]}')

async def restart_host(msg):
    # faked error
    rand_int = random.randrange(1, 3)
    if rand_int == 2:
        raise Exception(f'Could not restart {msg.hostname}')
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.randrange(1,3))
    logging.info(f'Restarted {msg.hostname}')

async def save(msg):
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Saved {msg} into database')

async def cleanup(msg, event):
    # this will block the rest of the coro until `event.set` is called
    await event.wait()
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    msg.ack()
    logging.info(f'Done. Acked {msg}')

async def extend(msg, event):
    while not event.is_set():
        logging.info(f'Extended deadline by 3 seconds for {msg}')
        # want to sleep for less than the deadline amount
        await asyncio.sleep(2)

def handle_results(results):
    for result in results:
        if isinstance(result, Exception):
            logging.error(f'Caught exception: {result}')

async def handle_message(msg):
    logging.info(f'Handling {msg["msg_id"]}')
    event = asyncio.Event()

    save_coro = save(msg)
    restart_coro = restart_host(msg)

    asyncio.create_task(cleanup(msg, event))

    results = await asyncio.gather(
        save_coro, restart_coro, return_exceptions=True
    )
    handle_results(results)
    event.set()

def consume_sync():
    client = get_subscriber()
    def callback(msg):
        data = json.loads(msg.data.decode('utf-8'))
        logging.info(f'Consumed {data["msg_id"]}')
        current_thread = threading.current_thread()
        logging.info(f'Current thread: {current_thread.name}')
        try:
            loop = asyncio.get_running_loop()
            logging.info(f'Found loop: {loop}')
        except RuntimeError:
            logging.error('No event loop in thread')
        asyncio.create_task(handle_message(data))

    client.subscribe(SUBSCRIPTION, callback)


async def publish(executor, loop):
    publisher = get_publisher()
    while True:
        to_exec = loop.run_in_executor(executor, publish_sync, publisher)
        asyncio.ensure_future(to_exec)
        await asyncio.sleep(3)


async def run_pubsub():
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=5, thread_name_prefix='Mandrill')

    consume_coro = loop.run_in_executor(executor, consume_sync)

    asyncio.ensure_future(consume_coro)
    loop.create_task(publish(executor, loop))


async def run_something_else():
    while True:
        logging.info('Running something else')
        await asyncio.sleep(random.random())


async def run():
    coros = [run_pubsub(), run_something_else()]
    await asyncio.gather(*coros)


async def shutdown(signal, loop):
    logging.info(f'Received exit signal {signal.name}...')
    loop.stop()
    logging.info('Shutdown complete.')

if __name__ == '__main__':
    assert os.environ.get('PUBSUB_EMULATOR_HOST'), 'You should be running the emulator'

    loop = asyncio.get_event_loop()

    # for simplicity
    loop.add_signal_handler(
        signal.SIGINT,
        lambda: asyncio.create_task(shutdown(signal.SIGINT, loop))
    )

    try:
        loop.create_task(run())
        loop.run_forever()
    finally:
        logging.info('Cleaning up')
        loop.stop()
