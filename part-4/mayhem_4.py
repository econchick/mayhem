#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Adding back in our graceful shutdown to code with both threads and asyncio -
shutdown blocked by locked thread despite shutting down executor thread.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-4/mayhem_4.py

Follow along: https://roguelynn.com/words/asyncio-sync-and-threaded/
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


def consume_sync(queue):
    while True:
        msg = queue.get()
        logging.info(f"Consumed {msg}")
        # Substitute for handling a message
        time.sleep(random.random())


async def publish(executor, queue):
    logging.info("Starting publisher")
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(loop.run_in_executor(executor, publish_sync, queue))


async def consume(executor, queue):
    logging.info("Starting consumer")
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(loop.run_in_executor(executor, consume_sync, queue))


async def shutdown(loop, executor, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    logging.info("Closing database connections")
    logging.info("Nacking outstanding messages")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)

    logging.info("Shutting down executor")
    executor.shutdown(wait=False)

    logging.info(f"Flushing metrics")
    loop.stop()


def main():
    executor = concurrent.futures.ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(loop, executor, signal=s)))
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
