#!/usr/bin/env python3

"""
Notice! This requires: google-cloud-pubsub==0.35.4
"""

# working w threadpool execs

import asyncio
import concurrent.futures
import json
import logging
import os
import random
import signal
import string

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
    rand_max = random.randrange(1, 6)
    for msg in range(1, rand_max):
        msg_data = {'msg_id': ''.join(random.choices(CHOICES, k=4))}
        bytes_message = bytes(json.dumps(msg_data), encoding='utf-8')
        publisher.publish(TOPIC, bytes_message)
        logging.debug(f'Published {msg_data["msg_id"]}')


def consume_sync():
    client = get_subscriber()
    def callback(msg):
        msg.ack()
        data = json.loads(msg.data.decode('utf-8'))
        logging.info(f'Consumed {data["msg_id"]}')

    client.subscribe(SUBSCRIPTION, callback)


async def publish(executor, loop):
    publisher = get_publisher()
    while True:
        to_exec = loop.run_in_executor(executor, publish_sync, publisher)
        asyncio.ensure_future(to_exec)
        await asyncio.sleep(.11)


async def run_pubsub():
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    consume_coro = loop.run_in_executor(executor, consume_sync)

    asyncio.ensure_future(consume_coro)
    loop.create_task(publish(executor, loop))


async def run_something_else():
    while True:
        logging.info('Running something else')
        await asyncio.sleep(.1)


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
