#!/usr/bin/env python3

# TODO: COPYRIGHT
# TODO: DOCS

import asyncio
import os

async def say(what, when):
    await asyncio.sleep(when)
    print(what)


loop = asyncio.get_event_loop()

loop.create_task(say('first hello', 2))
loop.create_task(say('second hello', 5))
loop.create_task(say('third hello', 1))
loop.create_task(say('fourth hello', 10))
loop.create_task(say('fifth hello', 60))

print(f'PID: {os.getpid()}')
try:
    loop.run_forever()
except KeyboardInterrupt:
    print('interrupted!')
finally:
    print('in finally')
    loop.stop()
    loop.close()
