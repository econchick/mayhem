#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
A shielded task can still be cancelled by another task.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-2/mayhem_aside_3.py

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


async def imma_let_you_speak(task_to_cancel):
    await asyncio.sleep(2)
    logging.info(f"interrupting {task_to_cancel}")
    task_to_cancel.cancel()


async def main():
    shielded = asyncio.shield(cant_stop_me())
    cancel_coro = imma_let_you_speak(shielded)
    await asyncio.gather(shielded, cancel_coro)


asyncio.run(main())
