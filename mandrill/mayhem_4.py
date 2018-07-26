#!/usr/bin/env python3.7

# Catch exceptions from coros in main event loop running

import asyncio
import functools
import logging
import random
import string

import attr


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


@attr.s
class PubSubMessage:
    msg_id = attr.ib(repr=False)
    instance_name = attr.ib()
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


async def publish(queue, n):
    choices = string.ascii_lowercase + string.digits
    for x in range(1, n + 1):
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(msg_id=x, instance_name=instance_name)
        # publish an item
        logging.info(f'Published {x} of {n} messages')
        # put the item in the queue
        await queue.put(msg)

    # indicate the publisher is done
    await queue.put(None)


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        # super-realistic simulation of an exception
        if msg.msg_id == 4:
            raise Exception('an exception happened!')

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        # simulate i/o operation using sleep
        await asyncio.sleep(random.random())


async def handle_exception(fn, loop):
    try:
        await fn()
    except Exception as e:
        logging.error(f'Caught exception: {e}') #, exc_info=e)
        loop.stop()


if __name__ == '__main__':
    queue = asyncio.Queue()
    publisher_fn = functools.partial(publish, queue, 5)
    consumer_fn = functools.partial(consume, queue)
    loop = asyncio.get_event_loop()
    publisher_coro = handle_exception(publisher_fn, loop)
    consumer_coro = handle_exception(consumer_fn, loop)

    loop.create_task(publisher_coro)
    loop.create_task(consumer_coro)
    try:
        loop.run_forever()
    finally:
        logging.info('Cleaning up')
        loop.close()
