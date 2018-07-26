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

import attr
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


@attr.s
class PubSubMessage:
    msg_id = attr.ib(repr=False)
    instance_name = attr.ib()
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


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
        msg_data = {'instance_name': ''.join(random.choices(CHOICES, k=4))}
        bytes_message = bytes(json.dumps(msg_data), encoding='utf-8')
        publisher.publish(TOPIC, bytes_message)
        logging.debug(f'Published {msg_data["instance_name"]}')

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

async def cleanup(pubsub_msg, event):
    # this will block the rest of the coro until `event.set` is called
    await event.wait()
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    pubsub_msg.ack()
    logging.info(f'Done. Acked {pubsub_msg.message_id}')

async def extend(msg, event):
    while not event.is_set():
        logging.info(f'Extended deadline by 3 seconds for {msg}')
        # want to sleep for less than the deadline amount
        await asyncio.sleep(2)

def handle_results(results):
    for result in results:
        if isinstance(result, Exception):
            logging.warning(f'Caught exception: {result}')

async def handle_message(pubsub_msg):
    msg_data = json.loads(pubsub_msg.data.decode('utf-8'))
    msg = PubSubMessage(
        msg_id=pubsub_msg.message_id,
        instance_name=msg_data['instance_name']
    )
    logging.info(f'Handling {msg}')
    event = asyncio.Event()

    save_coro = save(msg)
    restart_coro = restart_host(msg)

    asyncio.create_task(cleanup(pubsub_msg, event))

    results = await asyncio.gather(
        save_coro, restart_coro, return_exceptions=True
    )
    handle_results(results)
    event.set()

def consume_sync(loop):
    client = get_subscriber()
    def callback(pubsub_msg):
        logging.info(f'Consumed {pubsub_msg.message_id}')
        loop.create_task(handle_message(pubsub_msg))

    client.subscribe(SUBSCRIPTION, callback)

    # future = client.subscribe(SUBSCRIPTION, callback)

    # try:
    #     future.result()
    # except Exception as e:
    #     logging.error(f'Caught future exception: {e}')


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

    consume_coro = loop.run_in_executor(executor, consume_sync, loop)

    asyncio.ensure_future(consume_coro)
    loop.create_task(publish(executor, loop))


async def run_something_else():
    while True:
        logging.info('Running something else')
        await asyncio.sleep(random.random())

async def drain():
    client = get_subscriber()
    def callback(pubsub_msg):
        print(f'acking msg: {pubsub_msg}')
        pubsub_msg.ack()

    print('draining')
    client.subscribe(SUBSCRIPTION, callback)

async def run():
    # coros = [drain(), run_something_else()]
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
    except KeyboardInterrupt:
        logging.info('Interrupted')
    finally:
        logging.info('Cleaning up')
        loop.stop()
