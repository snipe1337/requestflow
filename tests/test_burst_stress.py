import asyncio
import time
import pytest
from requestflow.flow import RequestFlow
from requestflow.priorities import Priority


@pytest.mark.asyncio
async def test_burst_of_1000_pacing_and_dispatch_safety():
    rate = 400.0
    async with RequestFlow(global_rate_limit=rate, allow_custom_ceiling=True, max_pending=2000, max_concurrency=50) as flow:
        start_time = time.monotonic()
        results = []

        async def fast_op(idx: int):
            return idx
        tasks = [asyncio.create_task(flow.submit(fast_op, i))
                 for i in range(1000)]
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start_time
        assert len(results) == 1000
        assert elapsed >= 2.0
        assert flow.stats.completed == 1000
        assert flow.stats.failed == 0


@pytest.mark.asyncio
async def test_multi_guild_shared_global_budget():
    rate = 200.0
    async with RequestFlow(global_rate_limit=rate, allow_custom_ceiling=True, max_pending=1000, max_concurrency=30) as flow:
        guilds = [101, 102, 103, 104, 105]
        tasks = []
        start_time = time.monotonic()
        for i in range(500):
            g = guilds[i % len(guilds)]
            tasks.append(flow.submit(
                lambda: 'ok', guild_id=g, priority=Priority.NORMAL))
        res = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start_time
        assert len(res) == 500
        assert elapsed >= 2.0
        assert flow.stats.active_guilds == 0


@pytest.mark.asyncio
async def test_no_leaked_tasks_after_shutdown():
    initial_tasks = {t for t in asyncio.all_tasks() if not t.done()}
    flow = RequestFlow(max_concurrency=5)
    await flow.start()

    async def op():
        await asyncio.sleep(0.01)
        return 'ok'
    tasks = [asyncio.create_task(flow.submit(op)) for _ in range(10)]
    await asyncio.gather(*tasks)
    await flow.close(drain=True)
    await asyncio.sleep(0.05)
    remaining_tasks = {t for t in asyncio.all_tasks() if not t.done()}
    created_scheduler_tasks = [
        t for t in remaining_tasks if t not in initial_tasks and 'requestflow' in (t.get_name() or '')]
    assert len(created_scheduler_tasks) == 0
