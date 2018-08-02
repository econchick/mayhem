#!/usr/bin/env python3.7
# Copyright (c) 2018 Lynn Root
"""
Illustrate concurrency with multiple publisher.

Notice! This requires:
 - attrs==18.1.0
"""

import asyncio
import logging
import random
import string
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


async def publish(queue, publisher_id):
    """Simulates an external publisher of messages.

    Args:
        queue (asyncio.Queue): Queue to publish messages to.
        publisher_id (int): ID of particular publisher.
    """
    choices = string.ascii_lowercase + string.digits

    while True:
        msg_id = str(uuid.uuid4())
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        await queue.put(msg)
        logging.info(f'[{publisher_id}] Published message {msg}')
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())


async def handle_exception(coro, loop):
    """Wrapper for coroutines to catch exceptions & stop loop."""
    try:
        await coro
    except Exception:
        logging.error('Caught exception')
        loop.stop()


if __name__ == '__main__':
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    coros = [handle_exception(publish(queue, i), loop) for i in range(1, 4)]

    try:
        [loop.create_task(coro) for coro in coros]
        loop.run_forever()
    # probably not what you want! See `mayhem_14` for graceful shutdown
    except KeyboardInterrupt:
        logging.info('Interrupted')
    finally:
        logging.info('Cleaning up')
        loop.stop()
