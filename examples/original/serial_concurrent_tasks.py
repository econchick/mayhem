#!/usr/bin/env python3.6

# TODO copyright
# TODO docs

# adapted from http://asyncio.readthedocs.io/en/latest/producer_consumer.html
import asyncio
import functools
import random

import attr

@attr.s
class PubSubMessage:
    msg_id = attr.ib()
    data = attr.ib()

    def extend_deadline(self, seconds):
        print(f'[{self.msg_id}] message deadline extended by {seconds} seconds')

    def ack(self):
        print(f'[{self.msg_id}] messaged acked')


async def produce(queue, n):
    for x in range(1, n + 1):
        msg = PubSubMessage(msg_id=x, data='Hello, world')
        # produce an item
        print('producing {} of {} messages'.format(x, n))
        # put the item in the queue
        await queue.put(msg)

    # indicate the producer is done
    await queue.put(None)


async def process_message(future, message):
    print(f'Processing message {message.msg_id}')
    count = 0
    while not future.done():
        if count == 3:
            future.set_result('yay done')
        else:
            await asyncio.sleep(1)
            count += 1
    message.ack()


async def manage_message_lease(future, message):
    while not future.done():
        message.extend_deadline(2)
        await asyncio.sleep(1)
    message.ack()


def send_metric(message, future):
    print(f'Sending some metric for {message}')


async def consume(queue):
    while True:
        # wait for an item from the producer
        msg = await queue.get()
        if msg is None:
            # the producer emits None to indicate that it is done
            break

        print('processing {}...'.format(msg))
        # simulate i/o operation using sleep

        future = asyncio.Future()
        # asyncio.ensure_future(manage_message_lease(future, msg))
        asyncio.ensure_future(process_message(future, msg))
        callback = functools.partial(send_metric, msg)
        future.add_done_callback(callback)
        await future



if __name__ == '__main__':
    queue = asyncio.Queue()
    producer_coro = produce(queue, 2)
    consumer_coro = consume(queue)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(producer_coro)
        loop.run_until_complete(consumer_coro)
    # except Exception:
    #     print('Caught an exception')
    finally:
        print('Cleaning up')
        loop.close()
