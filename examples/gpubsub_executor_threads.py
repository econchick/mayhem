#!/usr/bin/env python3

"""
Notice! This requires: google-cloud-pubsub==0.35.4
"""

import asyncio
import concurrent.futures
import datetime
import json
import logging
import os
import random
import time
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


def produce_messages():
    publisher = get_publisher()
    futures = []
    for msg in range(1, 6):
        # now = datetime.datetime.utcnow()
        # now = now.strftime('%H:%M:%S.%f')
        # msg_data = {'msg_id': msg, 'published_at': now}
        msg_data = {'msg_id': msg}
        bytes_message = bytes(json.dumps(msg_data), encoding='utf-8')
        publisher.publish(TOPIC, bytes_message)
        logging.debug(f'Publishing {msg}')


def consume_messages():
    client = get_subscriber()
    def callback(msg):
        msg.ack()
        data = json.loads(msg.data.decode('utf-8'))
        logging.debug(f'Consumed {data["msg_id"]}')
        time.sleep(1)

    client.subscribe(SUBSCRIPTION, callback)
    # logging.info('XXXXXXXXXXXXXXX')


async def publish(executor, loop):
    while True:
        asyncio.ensure_future(loop.run_in_executor(executor, produce_messages))
        # await asyncio.sleep(random.random())
        await asyncio.sleep(1)


async def run_pubsub():
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    consume_coro = loop.run_in_executor(executor, consume_messages)

    asyncio.ensure_future(consume_coro)
    loop.create_task(publish(executor, loop))

async def run_something_else():
    while True:
        # unhelpful simulation of i/o work
        logging.debug('Something else is running')
        # await asyncio.sleep(random.random())
        await asyncio.sleep(.3)


async def thread_watcher():
    while True:
        threads = threading.enumerate()
        logging.info(f'Current thread count: {len(threads)}')
        logging.info('Current threads:')
        for thread in threads:
            logging.info(f'{thread.name}')
        await asyncio.sleep(4)


async def run():
    coros = [run_pubsub(), run_something_else(), thread_watcher()]
    await asyncio.gather(*coros)


if __name__ == '__main__':
    # safety net, wouldn't want to do anything in prod
    assert os.environ.get('PUBSUB_EMULATOR_HOST'), 'You should be running the emulator'
    loop = asyncio.get_event_loop()

    loop.create_task(run())
    loop.run_forever()
