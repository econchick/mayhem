#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Aside demonstrating that `asyncio.shield` isn't intuitive even without
signal handlers.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-2/mayhem_aside_2.py

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


async def main():
    await asyncio.shield(cant_stop_me())


asyncio.run(main())
