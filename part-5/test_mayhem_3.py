#!/usr/bin/env python3
# Copyright (c) 2019 Lynn Root
"""
Mocking coroutines

Notice! This requires:
- pytest==4.3.1
- pytest-asyncio==0.10.0
- pytest-mock==1.10.3

To run:

    $ pytest part-5/test_mayhem_3.py

Follow along: https://roguelynn.com/words/asyncio-testing/
"""

import asyncio

import pytest

import mayhem


# Note: with pytest, I could put these fixtures in `conftest.py` rather than
# copy-paste it to each test file. But I'd rather be explicit
@pytest.fixture
def message():
    return mayhem.PubSubMessage(message_id="1234", instance_name="mayhem_test")


@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    """Create a mock-coro pair.

    The coro can be used to patch an async method while the mock can
    be used to assert calls to the mocked out method.
    """

    def _create_mock_coro_pair(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:
            monkeypatch.setattr(to_patch, _coro)

        return mock, _coro

    return _create_mock_coro_pair


@pytest.fixture
def mock_sleep(create_mock_coro, monkeypatch):
    mock_sleep, _ = create_mock_coro(to_patch="mayhem.asyncio.sleep")
    return mock_sleep


@pytest.mark.asyncio
async def test_save(mock_sleep, message):
    assert not message.saved  # sanity check
    await mayhem.save(message)
    assert message.saved
    assert 1 == mock_sleep.call_count
