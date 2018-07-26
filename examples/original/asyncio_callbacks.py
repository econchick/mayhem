#!/usr/bin/env python3

# TODO: COPYRIGHT
# TODO: DOCS


import asyncio
import http.client


# TODO setup simple publisher and consumer of published messages
def get_message():
    url = 'localhost:1025'
    conn = http.client.HTTPConnection(host='localhost', port=1025)
    request = conn.request(method='POST', url=url, body='hi there')
    print(request)
    resp = conn.getresponse()
    print(resp)


def handle_result(message, future):
    try:
        result = future.result()
        message.ack()
    except Exception as ex:
        log.exception(ex)


def is_update_done(iters):
    if iters == 2:
        return True
    return False


async def wait_for_the_thing(future, message):
    iters = 0
    while not future.done():
        completed = is_update_done(iters)
        if completed:
            future.set_result('yay done')
            # TODO: should I return result here?
        else:
            await asyncio.sleep(1)
            iters += 1


async def do_something_with_message(future, message):
    try:
        # something network I/O intensive, i.e. start an update
        await asyncio.sleep(1)
    except Exception as ex:
        future.set_exception(ex)
    else:
        await wait_for_the_thing(future, message)


async def prolong_message_deadline(future, message):
    while not future.done():
        message.extend_deadline(10)
        await asyncio.sleep(2)


async def process_message(message):
    future = asyncio.Future()
    asyncio.ensure_future(prolong_message_deadline(future, message))
    asyncio.ensure_future(do_something_with_message(future, message))
    future.add_done_callback(functools.partial(handle_result, message))
    await future
