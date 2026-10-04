"""Concurrent CPU requests for exercising a local gateway without a model API."""
import asyncio
import time
from streamserve import Request


async def drive(gateway,tenant,alias,count=12,max_tokens=8,prefix='load',parallel=3):
    semaphore = asyncio.Semaphore(parallel)
    results = []
    async def one(index):
        async with semaphore:
            begin = time.monotonic()
            key = prefix+'.'+str(index)
            ticket = await gateway.submit(Request(tenant,key,alias,'load '+str(index),max_tokens))
            events = [event.wire() async for event in gateway.stream(tenant,ticket['id'])]
            results.append({'key':key,'ticket':ticket,'events':events,'elapsed':time.monotonic()-begin})
    await asyncio.gather(*(one(index) for index in range(count)))
    return sorted(results,key=lambda value:value['key'])
