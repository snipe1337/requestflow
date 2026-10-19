import asyncio
import pytest
from requestflow.exceptions import SchedulerClosedError, SchedulerNotStartedError
from requestflow.flow import RequestFlow


@pytest.mark.asyncio
async def test_submission_before_startup():
    flow = RequestFlow()

    async def op():
        return 'val'
    with pytest.raises(SchedulerNotStartedError):
        await flow.submit(op)


@pytest.mark.asyncio
async def test_submission_after_shutdown():
    flow = RequestFlow()
    await flow.start()
    await flow.close()

    async def op():
        return 'val'
    with pytest.raises(SchedulerClosedError):
        await flow.submit(op)


@pytest.mark.asyncio
async def test_repeated_shutdown_is_idempotent():
    flow = RequestFlow()
    await flow.start()
    await flow.close()
    await flow.close()
    assert flow.is_closed is True


@pytest.mark.asyncio
async def test_graceful_drain_on_close():
    flow = RequestFlow()
    await flow.start()
    completed_items = []

    async def work(n: int):
        await asyncio.sleep(0.05)
        completed_items.append(n)
        return n
    tasks = [asyncio.create_task(flow.submit(work, i)) for i in range(5)]
    await asyncio.sleep(0.01)
    await flow.close(drain=True, timeout=2.0)
    results = await asyncio.gather(*tasks)
    assert results == [0, 1, 2, 3, 4]
    assert len(completed_items) == 5


@pytest.mark.asyncio
async def test_async_context_manager():
    async with RequestFlow() as flow:
        assert flow.is_started is True
        res = await flow.submit(lambda: 'context_ok')
        assert res == 'context_ok'
    assert flow.is_closed is True
