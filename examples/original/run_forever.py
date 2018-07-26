#!/usr/bin/env python3

# TODO: COPYRIGHT
# TODO: DOCS

import asyncio

async def say(what, when):
    await asyncio.sleep(when)
    print(what)


loop = asyncio.get_event_loop()

# don't/unneeded
# first_task = asyncio.ensure_future(say('first hello', 2))
# second_task = asyncio.ensure_future(say('second hello', 1))

# do/probably what you want
loop.create_task(say('first hello', 2))
loop.create_task(say('second hello', 1))

loop.run_forever()
loop.close()
