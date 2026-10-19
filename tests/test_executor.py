import asyncio
import time
import pytest
from requestflow.exceptions import RequestTimeoutError
from requestflow.executor import RequestExecutor
from requestflow.models import RequestItem
from requestflow.policies import RetryPolicy


@pytest.mark.asyncio
async def test_concurrency_bounding():
    max_concurrency = 3
    executor = RequestExecutor(max_concurrency=max_concurrency)
    current_concurrent = 0
    peak_concurrent = 0
    lock = asyncio.Lock()

    async def sample_work():
        nonlocal current_concurrent, peak_concurrent
        async with lock:
            current_concurrent += 1
            if current_concurrent > peak_concurrent:
                peak_concurrent = current_concurrent
        await asyncio.sleep(0.05)
        async with lock:
            current_concurrent -= 1
        return 'done'
    items = [RequestItem(sequence=i, operation=sample_work) for i in range(10)]
    tasks = [asyncio.create_task(executor.execute(it)) for it in items]
    await asyncio.gather(*tasks)
    assert peak_concurrent <= max_concurrency
    assert peak_concurrent == max_concurrency
    for it in items:
        assert it.future.result() == 'done'


@pytest.mark.asyncio
async def test_exception_propagation():
    executor = RequestExecutor()

    class CustomDiscordError(Exception):
        pass

    async def failing_op():
        raise CustomDiscordError('Discord API Error 50001')
    item = RequestItem(sequence=1, operation=failing_op)
    await executor.execute(item)
    assert item.future.done()
    with pytest.raises(CustomDiscordError):
        item.future.result()


@pytest.mark.asyncio
async def test_execution_timeout():
    executor = RequestExecutor()

    async def slow_op():
        await asyncio.sleep(1.0)
        return 'finished'
    item = RequestItem(sequence=1, operation=slow_op, timeout=0.05)
    await executor.execute(item)
    assert item.future.done()
    with pytest.raises(RequestTimeoutError) as exc_info:
        item.future.result()
    assert exc_info.value.timeout_seconds == 0.05


@pytest.mark.asyncio
async def test_retry_policy():
    retry_policy = RetryPolicy(max_retries=2, base_delay=0.01,
                               jitter=False, retryable_exceptions=(ConnectionResetError,))
    executor = RequestExecutor(default_retry_policy=retry_policy)
    attempts = 0

    async def flaky_op():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionResetError('Connection reset by peer')
        return 'recovered'
    item = RequestItem(sequence=1, operation=flaky_op)
    await executor.execute(item)
    assert attempts == 3
    assert item.future.result() == 'recovered'
