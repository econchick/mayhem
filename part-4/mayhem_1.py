#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Simplistic mixing sync & async via ThreadPoolExecutor - not actually
concurrent.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-4/mayhem_1.py

Follow along: https://roguelynn.com/words/asyncio-sync-and-threaded/
"""

import asyncio
import concurrent.futures
import logging
import queue
import random
import string
import time
import uuid

import attr


# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ("foo %s" % "bar") is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


def publish_sync(queue):
    """Simulates an external publisher of messages.

    Args:
        queue (queue.Queue): Queue to publish messages to.
        n (int): Number of messages to publish.
    """
    choices = string.ascii_lowercase + string.digits

    while True:
        msg_id = str(uuid.uuid4())
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        queue.put(msg)
        logging.info(f"Published {msg}")
        # simulate randomness of publishing messages
        time.sleep(random.random())


async def publish(executor, queue):
    logging.info("Starting publisher")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(executor, publish_sync, queue)


def consume_sync(queue):
    while True:
        msg = queue.get()
        logging.info(f"Consumed {msg}")
        # Substitute for handling a message
        time.sleep(random.random())


async def consume(executor, queue):
    logging.info("Starting consumer")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(executor, consume_sync, queue)


def main():
    executor = concurrent.futures.ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    q = queue.Queue()

    try:
        loop.create_task(publish(executor, q))
        loop.create_task(consume(executor, q))
        loop.run_forever()
    finally:
        loop.close()
        logging.info("Successfully shutdown the Mayhem service.")


if __name__ == "__main__":
    main()
