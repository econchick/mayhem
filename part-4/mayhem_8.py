#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Scheduling coroutines on the main thread even loop from another thread -
attempt #2 - not obvious that we're not threadsafe.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-4/mayhem_8.py

Follow along: https://roguelynn.com/words/asyncio-sync-and-threaded/
"""

import asyncio
import concurrent.futures
import functools
import logging
import queue
import random
import signal
import string
import sys
import threading
import time
import uuid

import attr


# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ("foo %s" % "bar") is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


THREADS = set()

@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)
    restarted     = attr.ib(repr=False, default=False)
    saved         = attr.ib(repr=False, default=False)
    acked         = attr.ib(repr=False, default=False)
    extended_cnt  = attr.ib(repr=False, default=0)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


class RestartFailed(Exception):
    pass


def publish_sync(queue):
    """Simulates an external publisher of messages.

    Args:
        queue (queue.Queue): Queue to publish messages to.
        n (int): Number of messages to publish.
    """
    choices = string.ascii_lowercase + string.digits
    curr_thread = threading.current_thread()
    THREADS.add(curr_thread.ident)

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
    asyncio.ensure_future(
        loop.run_in_executor(executor, publish_sync, queue), loop=loop
    )


async def restart_host(msg):
    """Restart a given host.

    Args:
        msg (PubSubMessage): consumed event message for a particular
            host to be restarted.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    # totally realistic exception
    if random.randrange(1, 5) == 3:
        raise RestartFailed(f"Could not restart {msg.hostname}")
    msg.restart = True
    logging.info(f"Restarted {msg.hostname}")


async def save(msg):
    """Save message to a database.

    Args:
        msg (PubSubMessage): consumed event message to be saved.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    # totally realistic exception
    if random.randrange(1, 5) == 3:
        raise Exception(f"Could not save {msg}")
    msg.save = True
    logging.info(f"Saved {msg} into database")


async def cleanup(msg, event):
    """Cleanup tasks related to completing work on a message.

    Args:
        msg (PubSubMessage): consumed event message that is done being
            processed.
    """
    # this will block the rest of the coro until `event.set` is called
    await event.wait()
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    msg.acked = True
    logging.info(f"Done. Acked {msg}")


async def extend(msg, event):
    """Periodically extend the message acknowledgement deadline.

    Args:
        msg (PubSubMessage): consumed event message to extend.
        event (asyncio.Event): event to watch for message extention or
            cleaning up.
    """
    while not event.is_set():
        msg.extended_cnt += 1
        logging.info(f"Extended deadline by 3 seconds for {msg}")
        # want to sleep for less than the deadline amount
        await asyncio.sleep(2)


def handle_results(results, msg):
    """Handle exception results for a given message."""
    for result in results:
        if isinstance(result, RestartFailed):
            logging.error(f"Retrying for failure to restart: {msg.hostname}")
        elif isinstance(result, Exception):
            logging.error(f"Handling general error: {result}")


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
    handle_results(results, msg)
    event.set()


def consume_sync(queue, loop):
    while True:
        msg = queue.get()
        logging.info(f"Consumed {msg}")
        # the line below works but is not threadsafe
        loop.create_task(handle_message(msg))


async def consume(executor, queue):
    logging.info("Starting consumer")
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(
        loop.run_in_executor(executor, consume_sync, queue, loop), loop=loop
    )


def handle_exception(executor, loop, context):
    # context["message"] will always be there; but context["exception"] may not
    msg = context.get("exception", context["message"])
    logging.error(f"Caught exception: {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(loop, executor))


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

    logging.info("Shutting down ThreadPoolExecutor")
    executor.shutdown(wait=False)

    logging.info(f"Releasing {len(executor._threads)} threads from executor")
    for thread in executor._threads:
        try:
            thread._tstate_lock.release()
        except Exception:
            pass

    logging.info(f"Flushing metrics")
    loop.stop()


def main():
    executor = concurrent.futures.ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(loop, executor, signal=s)))
    handle_exc_func = functools.partial(handle_exception, executor)
    loop.set_exception_handler(handle_exc_func)
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
