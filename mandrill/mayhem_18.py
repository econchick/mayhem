#!/usr/bin/env python3.7
# Copyright (c) 2018 Lynn Root
"""
I'm not sure why I have mayhem_18, it's the same as mayhem_17, but I
don't want to screw up the numbering of mayhem_19+ files, so I'm leaving
this here.

Calling blocking threaded code from async

Notice! This requires:
 - attrs==18.1.0
"""

import asyncio
import concurrent.futures
import logging
import queue
import random
import signal
import string
import time
import uuid

import attr


# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ('foo %s' % 'bar') is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


def publish_sync(queue_sync):
    """Simulated blocking publisher"""
    msg_id = str(uuid.uuid4())
    choices = string.ascii_lowercase + string.digits
    host_id = ''.join(random.choices(choices, k=4))
    instance_name = f'cattle-{host_id}'
    msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
    # publish an item
    queue_sync.put(msg)
    logging.debug(f'Published message {msg}')
    # simulate randomness of publishing messages
    time.sleep(random.random())


async def publish(executor, queue):
    """Simulates an external publisher of messages.

    Args:
        executor (concurrent.futures.Executor): Executor to run sync
            functions in.
        queue (queue.Queue): Queue to publish messages to.
    """
    loop = asyncio.get_running_loop()
    while True:
        coro = loop.run_in_executor(executor, publish_sync, queue)
        await asyncio.wait([coro], timeout=0.1)


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
    logging.info(f'Done. Acked {msg}')


async def extend(msg, event):
    """Periodically extend the message acknowledgement deadline.

    Args:
        msg (PubSubMessage): consumed event message to extend.
        event (asyncio.Event): event to watch for message extention.
    """
    while not event.is_set():
        logging.info(f'Extended deadline by 3 seconds for {msg}')
        # want to sleep for less than the deadline amount
        await asyncio.sleep(2)


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
    event = asyncio.Event()
    asyncio.create_task(extend(msg, event))
    asyncio.create_task(cleanup(msg, event))

    results = await asyncio.gather(
        save(msg), restart_host(msg), return_exceptions=True
    )
    handle_results(results)
    event.set()


def consume_sync(queue_sync):
    """Simulates a blocking, third-party consumer client."""
    try:
        msg = queue_sync.get(block=False)
        logging.info(f'Pulled {msg}')
        return msg
    except queue.Empty:
        return


async def consume(executor, queue):
    """Consumer client to simulate subscribing to a publisher.

    Args:
        executor (concurrent.futures.Executor): Executor to run sync
            functions in.
        queue (queue.Queue): Queue from which to consume messages.
    """
    loop = asyncio.get_running_loop()
    while True:
        msg = await loop.run_in_executor(executor, consume_sync, queue)
        if not msg:  # could be None
            continue
        asyncio.create_task(handle_message(msg))


async def handle_exception(coro, loop):
    """Wrapper for coroutines to catch exceptions & stop loop."""
    try:
        await coro
    except asyncio.CancelledError:
        logging.info(f'Coroutine cancelled')
    except Exception as e:
        logging.error(f'Caught exception: {e}', exc_info=e)
    finally:
        loop.stop()


async def shutdown(signal, loop):
    """Wrapper for coroutines to catch exceptions & stop loop."""
    logging.info(f'Received exit signal {signal.name}...')
    logging.info('Closing database connections')
    logging.info('Nacking outstanding messages')
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info(f'Cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks)
    loop.stop()
    logging.info('Shutdown complete.')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # May want to catch other signals too
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    queue_sync = queue.Queue()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    publisher_coro = handle_exception(publish(executor, queue_sync), loop)
    consumer_coro = handle_exception(consume(executor, queue_sync), loop)

    try:
        loop.create_task(publisher_coro)
        loop.create_task(consumer_coro)
        loop.run_forever()
    finally:
        logging.info('Cleaning up')
        loop.stop()
