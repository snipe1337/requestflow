import asyncio
from requestflow import OverloadPolicy, QueueFullError, RequestFlow


async def main():
    async with RequestFlow(global_rate_limit=50.0, max_concurrency=10, max_pending=50, overload_policy=OverloadPolicy.REJECT) as flow:
        print('submitting a high-volume burst of 80 requests into a queue sized at 50...')

        async def worker(idx: int):
            await asyncio.sleep(0.05)
            return f'task_{idx}_done'
        accepted = 0
        rejected = 0

        async def attempt_submit(idx: int):
            nonlocal accepted, rejected
            try:
                await flow.submit(worker, idx)
                accepted += 1
            except QueueFullError:
                rejected += 1
        tasks = [asyncio.create_task(attempt_submit(i)) for i in range(80)]
        await asyncio.gather(*tasks)
        print(f'accepted into scheduler queue: {accepted}')
        print(f'rejected under backpressure:   {rejected}')
        print(
            f'current scheduler high water mark: {flow.stats.queue_high_water_mark}')
        await asyncio.sleep(1.0)
        print(f'final completed tasks: {flow.stats.completed}')
if __name__ == '__main__':
    asyncio.run(main())
