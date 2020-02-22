#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Tasks that are kicked off once other tasks are done - callback approach.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-1/mayhem_2.py

Follow along: https://roguelynn.com/words/asyncio-true-concurrency/
"""

import asyncio
import functools
import logging
import random
import string
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
    restarted     = attr.ib(repr=False, default=False)
    saved         = attr.ib(repr=False, default=False)
    acked         = attr.ib(repr=False, default=False)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


async def publish(queue):
    """Simulates an external publisher of messages.

    Args:
        queue (asyncio.Queue): Queue to publish messages to.
    """
    choices = string.ascii_lowercase + string.digits

    while True:
        msg_id = str(uuid.uuid4())
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        asyncio.create_task(queue.put(msg))
        logging.debug(f'Published message {msg}')
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())


async def restart_host(msg):
    """Restart a given host.

    Args:
        msg (PubSubMessage): consumed event message for a particular
            host to be restarted.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Restarted {msg.hostname}')


async def save(msg):
    """Save message to a database.

    Args:
        msg (PubSubMessage): consumed event message to be saved.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    logging.info(f'Saved {msg} into database')


#####
# Illustrates a callback approach
#####
async def cleanup(msg, fut):
    """Cleanup tasks related to completing work on a message.

    Args:
        msg (PubSubMessage): consumed event message that is done being
            processed.
        fut (asyncio.Future): future provided by the callback.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    msg.acked = True
    logging.info(f"Done. Acked {msg}")


async def handle_message(msg):
    """Kick off tasks for a given message.

    Args:
        msg (PubSubMessage): consumed message to process.
    """
    g_future = asyncio.gather(save(msg), restart_host(msg))

    callback = functools.partial(cleanup, msg)
    # add_done_callback requires a non-async func
    g_future.add_done_callback(callback)
    await g_future


async def consume(queue):
    """Consumer client to simulate subscribing to a publisher.

    Args:
        queue (asyncio.Queue): Queue from which to consume messages.
    """
    while True:
        msg = await queue.get()
        logging.info(f"Consumed {msg}")

        asyncio.create_task(handle_message(msg))


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    try:
        loop.create_task(publish(queue))
        loop.create_task(consume(queue))
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Process interrupted")
    finally:
        loop.close()
        logging.info("Successfully shutdown the Mayhem service.")


if __name__ == "__main__":
    main()
