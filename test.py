import asyncio
import time

async def sleep_test():
    loop = asyncio.get_event_loop()
    print('going to sleep')
    await loop.run_in_executor(None, time.sleep, 5)
    #time.sleep(5)
    print('waking up')

async def parallel():
    # run two sleep_tests in parallel and wait until both finish
    await asyncio.gather(sleep_test(), sleep_test())

asyncio.run(parallel())