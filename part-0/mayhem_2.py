#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Instead of `asyncio.run` in `./part-0/mayhem_1.py`, let's use
`loop.run_until_complete` since every time `asyncio.run` is called,
it creates and destroys a loop.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-0/mayhem_2.py

Follow along: https://roguelynn.com/words/asyncio-initial-setup/
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

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        # simulate i/o operation using sleep
        await asyncio.sleep(random.random())


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(publish(queue, 5))
    loop.run_until_complete(consume(queue))
    loop.close()
    logging.info("Successfully shutdown the Mayhem service.")


if __name__ == '__main__':
    main()
