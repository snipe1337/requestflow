import asyncio
import pytest
from requestflow.flow import RequestFlow
from requestflow.priorities import Priority


@pytest.mark.asyncio
async def test_cancellation_before_execution():
    async with RequestFlow(global_rate_limit=1.0) as flow:
        async def slow_first():
            await asyncio.sleep(0.2)
            return 'first'

        async def second():
            return 'second'
        t1 = asyncio.create_task(flow.submit(slow_first))
        await asyncio.sleep(0.01)
        t2 = asyncio.create_task(flow.submit(second))
        await asyncio.sleep(0.01)
        t2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t2
        res1 = await t1
        assert res1 == 'first'
        assert flow.stats.cancelled >= 1


@pytest.mark.asyncio
async def test_cancellation_during_execution():
    async with RequestFlow() as flow:
        executed_started = asyncio.Event()

        async def long_running():
            executed_started.set()
            await asyncio.sleep(1.0)
            return 'never'
        task = asyncio.create_task(flow.submit(long_running))
        await executed_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
