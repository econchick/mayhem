# from https://gist.github.com/nvgoldin/30cea3c04ee0796ebd0489aa62bcf00a
import asyncio
import signal
import functools

async def looping_task(loop, task_num):
    try:
        while True:
            print('{0}:in looping_task'.format(task_num))
            await asyncio.sleep(5.0, loop=loop)
    except asyncio.CancelledError:
        return "{0}: I was cancelled!".format(task_num)


async def shutdown(sig, loop):
    print('caught {0}'.format(sig.name))
    tasks = [task for task in asyncio.Task.all_tasks() if task is not
             asyncio.tasks.Task.current_task()]
    list(map(lambda task: task.cancel(), tasks))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print('finished awaiting cancelled tasks, results: {0}'.format(results))
    loop.stop()


loop = asyncio.get_event_loop()
for i in range(5):
    asyncio.ensure_future(looping_task(loop, i), loop=loop)
loop.add_signal_handler(signal.SIGTERM,
                        functools.partial(asyncio.ensure_future,
                                          shutdown(signal.SIGTERM, loop)))
try:
    loop.run_forever()
finally:
    loop.close()
