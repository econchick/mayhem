#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
How to properly shield a task

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-2/mayhem_aside_4.py

Follow along: https://roguelynn.com/words/asyncio-true-concurrency/
"""
import asyncio
import logging


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


async def imma_let_you_speak(task_to_cancel):
    await asyncio.sleep(2)
    logging.info(f"Interrupting {task_to_cancel}")
    task_to_cancel.cancel()


async def wrapper():
    parent_coro = asyncio.create_task(parent_task())
    cancel_coro = imma_let_you_speak(parent_coro)
    await asyncio.gather(parent_coro, cancel_coro)


async def main():
    asyncio.create_task(wrapper())
    await asyncio.sleep(61)


asyncio.run(main())
