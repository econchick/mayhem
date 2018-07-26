#!/usr/bin/env python3.6

# TODO copyright
# TODO docs

# adapted from http://asyncio.readthedocs.io/en/latest/producer_consumer.html
import asyncio
import functools
import random
import uuid

import attr

@attr.s
class PubSubMessage:
    msg_id = attr.ib()
    data = attr.ib()

    def ack(self):
        print(f'[{self.msg_id}] messaged acked')


async def produce(queue):
    while True:
        msg_id = uuid.uuid4()
        msg = PubSubMessage(msg_id=msg_id, data='Hello, world')
        # produce an item
        print(f'Published message {msg}')
        # put the item in the queue
        await queue.put(msg)
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())


async def process_message(message):
    print(f'Processing message {message.msg_id}')
    count = 0
    while count < 3:
        await asyncio.sleep(random.random())
        count += 1
    else:
        print('yay done')
    message.ack()
    send_metric(message)


def send_metric(message):
    print(f'Sending some metric for {message}')


async def consume(queue):
    to_process = set()
    while True:
        # wait for an item from the producer
        msg = await queue.get()
        if msg is None:
            # the producer emits None to indicate that it is done
            break

        print('processing {}...'.format(msg))
        # simulate i/o operation using sleep

        to_process.add(process_message(msg))
    await asyncio.gather(*to_process)


if __name__ == '__main__':
    queue = asyncio.Queue()
    producer_coro = produce(queue)
    # consumer_coro = consume(queue)

    loop = asyncio.get_event_loop()
    try:
        # loop.create_task(consumer_coro)
        loop.create_task(producer_coro)
        loop.run_forever()
    # except Exception:
    #     print('Caught an exception')
    finally:
        print('Cleaning up')
        loop.close()
