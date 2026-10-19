import asyncio
import time
from requestflow import RequestFlow, Priority


async def main():
    async with RequestFlow(global_rate_limit=50.0, max_concurrency=10) as flow:
        print('running continuous batch workload to generate live metrics...\n')

        async def worker(task_id: int):
            await asyncio.sleep(0.02)
            if task_id % 20 == 0:
                raise ValueError('simulated intermittent error')
            return f'task_{task_id}_done'

        async def safe_submit(task_id: int, guild: int):
            try:
                await flow.submit(worker, task_id, guild_id=guild, priority=Priority.NORMAL)
            except Exception:
                pass
        tasks = [asyncio.create_task(safe_submit(
            i, 1000 + i % 3)) for i in range(100)]
        for _ in range(5):
            await asyncio.sleep(0.05)
            s = flow.stats
            print(f"[t={time.strftime('%x')}] completed: {s.completed} | queued: {s.currently_queued} | executing: {s.currently_executing} | throughput: {s.throughput_window_rps} rps | wait p50: {s.p50_queue_wait_ms}ms")
        await asyncio.sleep(0.5)
        print('\nfinal metrics snapshot:')
        print(flow.stats)
if __name__ == '__main__':
    asyncio.run(main())
