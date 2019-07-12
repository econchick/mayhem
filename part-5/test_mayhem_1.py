#!/usr/bin/env python3
# Copyright (c) 2019 Lynn Root
"""
Testing asyncio code - running the event loop ourselves.

Notice! This requires:
- pytest==4.3.1

To run:

    $ pytest part-5/test_mayhem_1.py

Follow along: https://roguelynn.com/words/asyncio-testing/
"""

import asyncio

import pytest

import mayhem


@pytest.fixture
def message():
    return mayhem.PubSubMessage(message_id="1234", instance_name="mayhem_test")


def test_save(message):
    assert not message.saved  # sanity check
    asyncio.run(mayhem.save(message))
    assert message.saved
