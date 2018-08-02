#!/usr/bin/env python3
# Copyright (c) 2018 Lynn Root
"""
Working with threaded blocking code - incorrect approach #2, works by
accident.

Notice! This requires:
 - attrs==18.1.0
 - google-cloud-pubsub==0.35.4

You probably also want to run the Pub/Sub emulator to avoid calling/
setting up production Pub/Sub. For more details, see
https://cloud.google.com/pubsub/docs/emulator
"""

import asyncio
import concurrent.futures
import functools
import json
import logging
import os
import random
import signal
import string
import threading

import attr
from google.cloud import pubsub


# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ('foo %s' % 'bar') is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


TOPIC = 'projects/europython18/topics/ep18-topic'
SUBSCRIPTION = 'projects/europython18/subscriptions/ep18-sub'
PROJECT = 'europython18'
CHOICES = string.ascii_lowercase + string.digits
GLOBAL_QUEUE = asyncio.Queue()


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


def get_publisher():
    """Get Google Pub/Sub publisher client."""
    client = pubsub.PublisherClient()
    try:
        client.create_topic(TOPIC)
    except Exception as e:
        # already created
        pass

    return client


def get_subscriber():
    """Get Google Pub/Sub subscriber client."""
    client = pubsub.SubscriberClient()
    try:
        client.create_subscription(SUBSCRIPTION, TOPIC)
    except Exception:
        # already created
        pass
    return client


def publish_sync(publisher):
    """Publish messages to Google Pub/Sub."""
    for msg in range(1, 6):
        msg_data = {'instance_name': ''.join(random.choices(CHOICES, k=4))}
        bytes_message = bytes(json.dumps(msg_data), encoding='utf-8')
        publisher.publish(TOPIC, bytes_message)
        logging.debug(f'Published {msg_data["instance_name"]}')


async def restart_host(msg):
    """Consumer client to simulate subscribing to a publisher.

    Args:
        queue (asyncio.Queue): Queue from which to consume messages.
    """
    # faked error
    rand_int = random.randrange(1, 3)
    if rand_int == 2:
        raise Exception(f'Could not restart {msg.hostname}')

    # unhelpful simulation of i/o work
    await asyncio.sleep(random.randrange(1,3))
    logging.info(f'Restarted {msg.hostname}')


async def save(msg):
    """Save message to a database.

    Args:
        msg (PubSubMessage): consumed event message to be saved.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Saved {msg} into database')


async def cleanup(pubsub_msg, event):
    """Cleanup tasks related to completing work on a message.

    Args:
        msg (pubsub.Message): consumed event message that is done being
            processed.
        event (asyncio.Event): event to watch for message cleanup.
    """
    # this will block the rest of the coro until `event.set` is called
    await event.wait()
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    pubsub_msg.ack()
    logging.info(f'Done. Acked {pubsub_msg.message_id}')


def handle_results(results):
    """Parse out successful and errored results."""
    for result in results:
        if isinstance(result, Exception):
            logging.warning(f'Caught exception: {result}')


async def handle_message(pubsub_msg):
    """Kick off tasks for a given message.

    Args:
        pubsub_msg (pubsub.Message): consumed message to process.
    """
    # need to parse a pubsub_msg into our own PubSubMessage
    msg_data = json.loads(pubsub_msg.data.decode('utf-8'))
    msg = PubSubMessage(
        message_id=pubsub_msg.message_id,
        instance_name=msg_data['instance_name']
    )
    logging.info(f'Handling {msg}')
    event = asyncio.Event()
    # no longer need to extend since pubsub lib does for us
    asyncio.create_task(cleanup(pubsub_msg, event))

    results = await asyncio.gather(
        save(msg), restart_host(msg), return_exceptions=True
    )
    handle_results(results)
    event.set()


def consume_sync(loop):
    """Simulates a blocking, third-party consumer client."""
    client = get_subscriber()

    def callback(pubsub_msg):
        logging.info(f'Consumed {pubsub_msg.message_id}')
        # do not do this! refer to `mayhem_25` for correct approach
        loop.create_task(handle_message(pubsub_msg))

    client.subscribe(SUBSCRIPTION, callback)


async def publish(executor):
    """Simulates an external publisher of messages.

    Args:
        executor (concurrent.futures.Executor): Executor to run sync
            functions in.
    """
    loop = asyncio.get_running_loop()
    publisher = get_publisher()
    while True:
        await loop.run_in_executor(executor, publish_sync, publisher)
        await asyncio.sleep(3)


async def run_pubsub():
    """Entrypoint to run pub/sub coroutines."""
    loop = asyncio.get_running_loop()
    # add a prefix to our executor for easier identification of what
    # threads we created versus what the google-cloud-pubsub library
    # created
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=15, thread_name_prefix='Mandrill')

    consume_coro = loop.run_in_executor(executor, consume_sync, loop)

    asyncio.ensure_future(consume_coro)
    loop.create_task(publish(executor))


async def run_something_else():
    """Example of concurrency when using executors."""
    while True:
        logging.info('Running something else')
        await asyncio.sleep(random.random())


async def run():
    """Example of concurrency when using executors."""
    coros = [run_pubsub(), run_something_else()]
    await asyncio.gather(*coros)


async def shutdown(signal, loop):
    """Entrypoint to run all coroutines."""
    logging.info(f'Received exit signal {signal.name}...')
    loop.stop()
    logging.info('Shutdown complete.')


if __name__ == '__main__':
    assert os.environ.get('PUBSUB_EMULATOR_HOST'), 'You should be running the emulator'

    loop = asyncio.get_event_loop()

    # for simplicity, probably want to catch other signals too
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
