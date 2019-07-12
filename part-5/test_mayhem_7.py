#!/usr/bin/env python3
# Copyright (c) 2019 Lynn Root
"""
Testing the event loop - parametrize signals

Notice! This requires:
- pytest==4.3.1
- pytest-asyncio==0.10.0
- pytest-mock==1.10.3

To run:

    $ pytest part-5/test_mayhem_6.py

Follow along: https://roguelynn.com/words/asyncio-testing/
"""

import asyncio
import os
import signal
import time
import threading

import pytest

import mayhem


@pytest.fixture
def mock_queue(mocker, monkeypatch):
    queue = mocker.Mock()
    monkeypatch.setattr(mayhem.asyncio, "Queue", queue)
    return queue.return_value


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
def event_loop(event_loop, mocker):
    new_loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(new_loop)
    new_loop._close = new_loop.close
    new_loop.close = mocker.Mock()

    yield new_loop

    new_loop._close()


@pytest.mark.parametrize(
    "tested_signal", ("SIGINT","SIGTERM", "SIGHUP", "SIGUSR1")
)
def test_main(tested_signal, create_mock_coro, event_loop, mock_queue, mocker):
    tested_signal = getattr(signal, tested_signal)
    mock_asyncio_gather, _ = create_mock_coro("mayhem.asyncio.gather")
    mock_consume, _ = create_mock_coro("mayhem.consume")
    mock_publish, _ = create_mock_coro("mayhem.publish")

    mock_shutdown = mocker.Mock()
    def _shutdown():
        mock_shutdown()
        event_loop.stop()

    event_loop.add_signal_handler(signal.SIGUSR1, _shutdown)

    def _send_signal():
        time.sleep(0.1)
        os.kill(os.getpid(), tested_signal)

    thread = threading.Thread(target=_send_signal, daemon=True)
    thread.start()

    mayhem.main()

    assert tested_signal in event_loop._signal_handlers
    assert mayhem.handle_exception == event_loop.get_exception_handler()

    mock_consume.assert_called_once_with(mock_queue)
    mock_publish.assert_called_once_with(mock_queue)

    if tested_signal is not signal.SIGUSR1:
        mock_asyncio_gather.assert_called_once_with(return_exceptions=True)
        mock_shutdown.assert_not_called()
    else:
        mock_asyncio_gather.assert_not_called()
        mock_shutdown.assert_called_once_with()

    # asserting the loop is stopped but not closed
    assert not event_loop.is_running()
    assert not event_loop.is_closed()
    event_loop.close.assert_called_once_with()
