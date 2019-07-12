#!/usr/bin/env python3
# Copyright (c) 2019 Lynn Root
"""
Testing asyncio code - making use of the plugin `pytest-asyncio` to handle
event loop creation.

Notice! This requires:
- pytest==4.3.1
- pytest-asyncio==0.10.0

To run:

    $ pytest part-5/test_mayhem_2.py

Follow along: https://roguelynn.com/words/asyncio-testing/
"""

import asyncio

import pytest

import mayhem


@pytest.fixture
def message():
    return mayhem.PubSubMessage(message_id="1234", instance_name="mayhem_test")


@pytest.mark.asyncio
async def test_save(message):  # <-- now a coroutine!
    assert not message.saved  # sanity check
    await mayhem.save(message)
    assert message.saved
