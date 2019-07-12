#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
How to properly shield a task

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-2/mayhem_aside_5.py

Follow along: https://roguelynn.com/words/asyncio-true-concurrency/
"""
import asyncio
import logging
import signal


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


async def cant_stop_me():
    logging.info("Can't stop me...")
    for i in range(12):
        logging.info("Sleeping for 5 seconds...")
        await asyncio.sleep(5)
    logging.info("Done!")


async def parent_task():
    logging.info("Kicking of shielded task")
    await asyncio.shield(cant_stop_me())
    logging.info("Shielded task done")


async def main():
    asyncio.create_task(parent_task())
    while True:
        await asyncio.sleep(61)


async def shutdown(signal, loop):
    logging.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    for task in tasks:
        # skipping over shielded coro still does not help
        if task._coro.__name__ == "cant_stop_me":
            continue
        task.cancel()

    logging.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info("Stopping loop")
    loop.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    try:
        loop.run_until_complete(main())
    finally:
        logging.info("Successfully shutdown service")
        loop.close()
