#!/usr/bin/env python3.7
# Copyright (c) 2018 Lynn Root
"""
Non-asyncio version of `mayhem_1` for comparison.

Notice! This requires:
 - attrs==18.1.0
"""

import logging
import queue
import random
import string
import time

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


def publish(queue, n):
    """Simulates an external publisher of messages.

    Args:
        queue (queue.Queue): Queue to publish messages to.
        n (int): Number of messages to publish.
    """
    choices = string.ascii_lowercase + string.digits
    for x in range(1, n + 1):
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=x, instance_name=instance_name)
        # publish an item
        queue.put(msg)
        logging.info(f'Published {x} of {n} messages')

    # indicate the publisher is done
    queue.put(None)


def consume(queue):
    """Consumer client to simulate subscribing to a publisher.

    Args:
        queue (queue.Queue): Queue from which to consume messages.
    """
    while True:
        # wait for an item from the publisher
        msg = queue.get()

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        # simulate i/o operation using sleep
        time.sleep(random.random())


if __name__ == '__main__':
    queue = queue.Queue()
    publish(queue, 5)
    consume(queue)
