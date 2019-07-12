#!/usr/bin/env python3
# Copyright (c) 2019 Lynn Root
"""
Testing asyncio code

Notice! This requires:
- pytest==4.3.1
- pytest-asyncio==0.10.0
- pytest-mock==1.10.3

To run:

    $ pytest part-5/test_mayhem.py

Follow along: https://roguelynn.com/words/asyncio-testing/
"""

import asyncio
import logging
import os
import signal
import time
import threading

import pytest

import mayhem


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    return caplog


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
def mock_queue(mocker, monkeypatch):
    queue = mocker.Mock()
    monkeypatch.setattr(mayhem.asyncio, "Queue", queue)
    return queue.return_value


@pytest.fixture
def mock_get(mock_queue, create_mock_coro):
    mock_get, coro_get = create_mock_coro()
    mock_queue.get = coro_get
    return mock_get


@pytest.fixture
def mock_put(mock_queue, create_mock_coro):
    mock_put, coro_put = create_mock_coro()
    mock_queue.put = coro_put
    return mock_put


@pytest.fixture
def mock_sleep(create_mock_coro, monkeypatch):
    mock_sleep, coro_sleep = create_mock_coro("mayhem.asyncio.sleep")
    # mocker.patch("mayhem.asyncio.sleep", coro_sleep)
    return mock_sleep


@pytest.fixture
def mock_choices(mocker, monkeypatch):
    mock_choices = mocker.Mock(side_effect=["1234", "5678", "9876"])
    monkeypatch.setattr(mayhem.random, "choices", mock_choices)
    return mock_choices


@pytest.fixture
def mock_uuid(mocker, monkeypatch):
    mock_uuid = mocker.Mock(side_effect=["1", "2", "3"])
    monkeypatch.setattr(mayhem.uuid, "uuid4", mock_uuid)
    return mock_uuid


@pytest.fixture
def message():
    return mayhem.PubSubMessage(message_id="1", instance_name="cattle-1234")


@pytest.mark.asyncio
async def test_publish(
    mock_put, mock_queue, mock_sleep, mocker, mock_uuid, mock_choices, caplog
):
    with pytest.raises(RuntimeError):  # exhausted mock_uuid list
        await mayhem.publish(mock_queue)

    exp_mock_put_calls = [
        mocker.call(mayhem.PubSubMessage(message_id="1", instance_name="cattle-1234")),
        mocker.call(mayhem.PubSubMessage(message_id="2", instance_name="cattle-5678")),
        mocker.call(mayhem.PubSubMessage(message_id="3", instance_name="cattle-9876")),
    ]
    ret_tasks = [
        t for t in asyncio.all_tasks() if t is not asyncio.current_task()
    ]
    assert 3 == len(ret_tasks)
    assert 3 == len(caplog.records)
    mock_put.assert_not_called()
    await asyncio.gather(*ret_tasks)
    assert exp_mock_put_calls == mock_put.call_args_list


@pytest.mark.asyncio
async def test_restart_host(message, mocker, monkeypatch, mock_sleep, caplog):
    assert not message.restarted  # sanity check

    mock_randrange = mocker.Mock(return_value=1)
    monkeypatch.setattr(mayhem.random, "randrange", mock_randrange)

    await mayhem.restart_host(message)

    assert message.restarted
    assert 1 == mock_sleep.call_count
    assert 1 == len(caplog.records)


@pytest.mark.asyncio
async def test_restart_host_raises(
    message, mocker, monkeypatch, mock_sleep, caplog
):
    mock_randrange = mocker.Mock(return_value=3)
    monkeypatch.setattr(mayhem.random, "randrange", mock_randrange)

    with pytest.raises(mayhem.RestartFailed, match="Could not restart "):
        await mayhem.restart_host(message)

    assert 1 == mock_sleep.call_count
    assert not len(caplog.records)


@pytest.mark.asyncio
async def test_save(mock_sleep, message, mocker, monkeypatch, caplog):
    assert not message.saved  # sanity check

    mock_randrange = mocker.Mock(return_value=2)
    monkeypatch.setattr(mayhem.random, "randrange", mock_randrange)

    await mayhem.save(message)

    assert message.saved
    assert 1 == mock_sleep.call_count
    assert 1 == len(caplog.records)


@pytest.mark.asyncio
async def test_cleanup(mock_sleep, message, caplog):
    assert not message.acked  # sanity check

    async def _set_event(ev):
        for i in range(3):
            if i != 2:
                await asyncio.sleep(0)
            else:
                ev.set()

    event = asyncio.Event()

    await asyncio.gather(_set_event(event), mayhem.cleanup(message, event))

    assert message.acked
    assert 1 == len(caplog.records)


@pytest.mark.asyncio
async def test_extend(message, caplog, mock_sleep, mocker, monkeypatch):
    assert not message.extended_cnt  # sanity check

    mock_event = mocker.Mock()
    mock_event.is_set.side_effect = [False, False, True]

    await mayhem.extend(message, mock_event)

    assert 2 == mock_sleep.call_count
    assert 2 == message.extended_cnt
    assert 2 == len(caplog.records)


@pytest.mark.asyncio
async def test_handle_message(message, create_mock_coro, mocker, monkeypatch):
    mock_save, _ = create_mock_coro("mayhem.save")
    mock_restart, _ = create_mock_coro("mayhem.restart_host")
    mock_extend, _ = create_mock_coro("mayhem.extend")
    mock_cleanup, _ = create_mock_coro("mayhem.cleanup")

    mock_handle_results = mocker.Mock()
    monkeypatch.setattr(mayhem, "handle_results", mock_handle_results)
    mock_event = mocker.Mock()
    monkeypatch.setattr(mayhem.asyncio, "Event", mock_event)

    await mayhem.handle_message(message)

    mock_save.assert_called_once_with(message)
    mock_restart.assert_called_once_with(message)
    mock_extend.assert_called_once_with(message, mock_event.return_value)
    mock_cleanup.assert_called_once_with(message, mock_event.return_value)
    mock_handle_results.assert_called_once_with(
        [mock_save.return_value, mock_restart.return_value], message
    )
    mock_event.return_value.set.assert_called_once_with()


@pytest.mark.xfail
@pytest.mark.asyncio
async def test_consume_wrong(mock_get, mock_queue, message, create_mock_coro):
    mock_get.side_effect = [message, Exception("break while loop")]
    mock_handle_message, _ = create_mock_coro("mayhem.handle_message")

    with pytest.raises(Exception, match="break while loop"):
        await mayhem.consume(mock_queue)

    # expected to fail
    mock_handle_message.assert_called_once_with(message)


@pytest.mark.asyncio
async def test_consume(
    mock_get, mock_queue, message, create_mock_coro, caplog
):
    mock_get.side_effect = [message, Exception("break while loop")]
    mock_handle_message, _ = create_mock_coro("mayhem.handle_message")

    with pytest.raises(Exception, match="break while loop"):
        await mayhem.consume(mock_queue)

    ret_tasks = [
        t for t in asyncio.all_tasks() if t is not asyncio.current_task()
    ]
    assert 1 == len(ret_tasks)
    assert 1 == len(caplog.records)
    mock_handle_message.assert_not_called()
    await asyncio.gather(*ret_tasks)
    mock_handle_message.assert_called_once_with(message)


# avoid `loop.close` to actually _close_ when called in main code
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
