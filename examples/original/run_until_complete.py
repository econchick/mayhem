#!/usr/bin/env python3

# TODO: COPYRIGHT
# TODO: DOCS

import asyncio
import logging

logging.getLogger('asyncio').setLevel(logging.DEBUG)

async def say(what, when):
    await asyncio.sleep(when)
    print(what)


loop = asyncio.get_event_loop()
loop.set_debug(True)

# don't/unneeded
# first_task = asyncio.ensure_future(say('first hello', 2))
# second_task = asyncio.ensure_future(say('second hello', 1))

# also don't/unneeded
first_task = loop.create_task(say('first hello', 2))
second_task = loop.create_task(say('second hello', 1))
tasks = asyncio.gather(first_task, second_task)

loop.run_until_complete(tasks)

loop.close()
