#!/usr/bin/env python3.7
# Copyright (c) 2018 Lynn Root
"""
Catch potential exceptions on the top-level from coroutines.

Notice! This requires:
 - attrs==18.1.0
"""

import asyncio
import logging
import random
import string

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


async def publish(queue, n):
    """Simulates an external publisher of messages.

    Args:
        queue (asyncio.Queue): Queue to publish messages to.
        n (int): Number of messages to publish.
    """
    choices = string.ascii_lowercase + string.digits
    for x in range(1, n + 1):
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=x, instance_name=instance_name)
        # publish an item
        await queue.put(msg)
        logging.info(f'Published {x} of {n} messages')

    # indicate the publisher is done
    await queue.put(None)


async def consume(queue):
    """Consumer client to simulate subscribing to a publisher.

    Args:
        queue (asyncio.Queue): Queue from which to consume messages.
    """
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        # super-realistic simulation of an exception
        if msg.message_id == 4:
            raise Exception('an exception happened!')

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        # simulate i/o operation using sleep
        await asyncio.sleep(random.random())


async def handle_exception(coro, loop):
    """Wrapper for coroutines to catch exceptions & stop loop."""
    try:
        await coro
    except Exception as e:
        logging.error(f'Caught exception: {e}')
        loop.stop()


if __name__ == '__main__':
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    publisher_coro = handle_exception(publish(queue, 5), loop)
    consumer_coro = handle_exception(consume(queue), loop)

    loop.create_task(publisher_coro)
    loop.create_task(consumer_coro)
    try:
        loop.run_forever()
    finally:
        logging.info('Cleaning up')
        loop.close()
