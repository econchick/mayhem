import asyncio
import logging
import signal


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


async def cant_stop_me():
    logging.info('Hold on...')
    await asyncio.sleep(60)
    logging.info('Done!')


async def handle_exception(coro, loop):
    try:
        await coro
    except asyncio.CancelledError:
        logging.info(f'Coroutine cancelled')
    except Exception :
        logging.error('Caught exception')
    finally:
        loop.stop()


async def shutdown(signal, loop):
    logging.info(f'Received exit signal {signal.name}...')
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info(f'Cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks)
    loop.stop()
    logging.info('Shutdown complete.')

async def main():
    try:
        await asyncio.shield(cant_stop_me())
    except asyncio.CancelledError:
        print('cancelled')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # May want to catch other signals too
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    shielded_coro = asyncio.shield(cant_stop_me())
    # shielded_coro = asyncio.shield(handle_exception(shielded_coro, loop))
    # shielded_coro = handle_exception(shielded_coro, loop)

    try:
        loop.run_until_complete(shielded_coro)
    finally:
        logging.info('Cleaning up')
        loop.stop()
