#!/usr/bin/env python3

# TODO: COPYRIGHT
# TODO: DOCS

import asyncio


# TODO message consumer


class Message:
    def __init__(self, msgid):
        self.msgid = msgid

    def extend_deadline(self, seconds):
        print(f'[{self.msgid}] message deadline extended by {seconds} seconds')

    def ack(self):
        print(f'[{self.msgid}] messaged acked')


subscriber = asyncio.Queue()

async def watch_update(message):
    iters = 0
    while True:
        if iters == 3:
            print('updated!')
            return
        iters += 1
        print('not finished updating')
        await asyncio.sleep(1)


async def start_update(message):
    await asyncio.sleep(1)  # an http call
    print('update started')


async def process_message(message, event):
    await start_update(message) # network intensive
    await watch_update(message) # network intensive
    event.set()


async def manage_message_lease(message, event):
    while not event.is_set():
        print('extending deadline')
        message.extend_deadline(10)
        await asyncio.sleep(2)

    print('acking message')
    message.ack()


async def pull_message():
    try:
        message = await subscriber.get()  # TODO
    except Exception:
        # no message
        return

    if message is None:
        print('done')
        return

    event = asyncio.Event()

    task1 = manage_message_lease(message, event)
    task2 = process_message(message, event)

    # notice it's not two awaits, but rather starting two awaitables at the
    # same time
    await asyncio.gather(task1, task2)


async def consume():
    coroutines = set()
    while True:
        coro = pull_message()
        coroutines.add(coro)
        _, coroutines = await asyncio.wait(coroutines, timeout=1)


async def produce():
    msg1 = Message(1)
    msg2 = Message(2)
    msg3 = None

    await subscriber.put(msg1)
    await asyncio.sleep(4)
    await subscriber.put(msg2)
    await asyncio.sleep(2)
    await subscriber.put(msg3)


loop = asyncio.get_event_loop()

task1 = loop.create_task(produce())
task2 = loop.create_task(consume())

loop.run_until_complete(asyncio.gather(task1, task2))
