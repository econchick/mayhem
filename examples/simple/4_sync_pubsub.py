#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

import queue
import logging
import random
import string
import time

import attr

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

@attr.s
class PubSubMessage:
    msg_id = attr.ib(repr=False)
    instance_name = attr.ib()
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


def publish(queue, n):
    for x in range(1, n + 1):
        host_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(msg_id=x, instance_name=instance_name)
        # publish an item
        logging.info(f'Published {x} of {n} messages')
        # put the item in the queue
        queue.put(msg)

    # indicate the publisher is done
    queue.put(None)


def consume(queue):
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
