#!/usr/bin/env python3.7

# TODO copyright
# TODO docs

import asyncio
import functools
import logging
import random
import string
import uuid

import attr

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


@attr.s
class PubSubMessage:
    msg_id = attr.ib(repr=False)
    instance_name = attr.ib()
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'

async def publish(queue):
    while True:
        msg_id = str(uuid.uuid4())
        host_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(msg_id=msg_id, instance_name=instance_name)
        # publish an item
        logging.info(f'Published message {msg}')
        # put the item in the queue
        await queue.put(msg)
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())

if __name__ == '__main__':
    queue = asyncio.Queue()
    publisher_coro = publish(queue)

    loop = asyncio.get_event_loop()
    try:
        loop.create_task(publisher_coro)
        loop.run_forever()
    except Exception:
        logging.error('Caught exception')
    except KeyboardInterrupt:
        logging.info('Process interrupted')
    finally:
        logging.info('Cleaning up')
        loop.stop()
