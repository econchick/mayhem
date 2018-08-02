#!/usr/bin/env python3
# Copyright (c) 2018 Lynn Root
"""
Working with threaded blocking code - incorrect approach

Notice! This requires:
 - google-cloud-pubsub==0.35.4

You probably also want to run the Pub/Sub emulator to avoid calling/
setting up production Pub/Sub. For more details, see
https://cloud.google.com/pubsub/docs/emulator
"""

import asyncio
import concurrent.futures
import json
import logging
import os
import random
import signal
import string

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


def publish_sync():
    """Publish messages to Google Pub/Sub."""
    publisher = get_publisher()
    for msg in range(1, 6):
        msg_data = {'msg_id': ''.join(random.choices(CHOICES, k=4))}
        bytes_message = bytes(json.dumps(msg_data), encoding='utf-8')
        publisher.publish(TOPIC, bytes_message)
        logging.debug(f'Published {msg_data["msg_id"]}')


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


async def cleanup(msg, event):
    """Cleanup tasks related to completing work on a message.

    Args:
        msg (PubSubMessage): consumed event message that is done being
            processed.
        event (asyncio.Event): event to watch for message cleanup.
    """
    # this will block the rest of the coro until `event.set` is called
    await event.wait()
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    msg.ack()
    logging.info(f'Done. Acked {msg}')


def handle_results(results):
    """Parse out successful and errored results."""
    for result in results:
        if isinstance(result, Exception):
            logging.error(f'Caught exception: {result}')


async def handle_message(msg):
    """Kick off tasks for a given message.

    Args:
        msg (PubSubMessage): consumed message to process.
    """
    logging.info(f'Handling {msg["msg_id"]}')
    event = asyncio.Event()
    # no longer need to extend since pubsub lib does for us
    asyncio.create_task(cleanup(msg, event))

    results = await asyncio.gather(
        save(msg), restart_host(msg), return_exceptions=True
    )
    handle_results(results)
    event.set()


def consume_sync():
    """Simulates a blocking, third-party consumer client."""
    client = get_subscriber()
    def callback(msg):
        data = json.loads(msg.data.decode('utf-8'))
        logging.info(f'Consumed {data["msg_id"]}')

        # To demonstrate this thread doesn't have a running loop
        try:
            loop = asyncio.get_running_loop()
            logging.info(f'Found loop: {loop}')
        except RuntimeError:
            logging.error('No event loop in thread')

        # do not do this! refer to `mayhem_25` for correct approach
        asyncio.create_task(handle_message(data))

    client.subscribe(SUBSCRIPTION, callback)


async def publish(executor):
    """Simulates an external publisher of messages.

    Args:
        executor (concurrent.futures.Executor): Executor to run sync
            functions in.
    """
    loop = asyncio.get_running_loop()
    while True:
        await loop.run_in_executor(executor, publish_sync)
        await asyncio.sleep(3)


async def run_pubsub():
    """Entrypoint to run pub/sub coroutines."""
    loop = asyncio.get_running_loop()
    # add a prefix to our executor for easier identification of what
    # threads we created versus what the google-cloud-pubsub library
    # created
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=5, thread_name_prefix='Mandrill')

    consume_coro = loop.run_in_executor(executor, consume_sync)

    asyncio.ensure_future(consume_coro)
    loop.create_task(publish(executor))


async def run_something_else():
    """Example of concurrency when using executors."""
    while True:
        logging.info('Running something else')
        await asyncio.sleep(random.random())


async def run():
    """Entrypoint to run main coroutines."""
    coros = [run_pubsub(), run_something_else()]
    await asyncio.gather(*coros)


async def shutdown(signal, loop):
    """Simplified shutdown coroutine function."""
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
