#!/usr/bin/env python3.7
# Copyright (c) 2018 Lynn Root
"""
asyncio.shield - broken within graceful shutdown
"""

import asyncio
import logging
import signal


# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ('foo %s' % 'bar') is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


async def cant_stop_me():
    logging.info('Hold on...')
    await asyncio.sleep(10)
    logging.info('Slept 10/60 seconds')
    await asyncio.sleep(10)
    logging.info('Slept 20/60 seconds')
    await asyncio.sleep(10)
    logging.info('Slept 30/60 seconds')
    await asyncio.sleep(10)
    logging.info('Slept 40/60 seconds')
    await asyncio.sleep(10)
    logging.info('Slept 50/60 seconds')
    await asyncio.sleep(10)
    logging.info('Slept 60/60 seconds')
    logging.info('Done!')


async def shutdown(signal, loop):
    logging.info(f'Received exit signal {signal.name}...')
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info('Canceling outstanding tasks')
    await asyncio.gather(*tasks)
    logging.info('Outstanding tasks canceled')
    loop.stop()
    logging.info('Shutdown complete.')


if __name__ == '__main__':
    logging.info('Starting script... Interrupt me before 60 seconds.')
    loop = asyncio.get_event_loop()

    # May want to catch other signals too
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    shielded_coro = asyncio.shield(cant_stop_me())

    try:
        loop.run_until_complete(shielded_coro)
    finally:
        logging.info('Cleaning up')
        loop.stop()
