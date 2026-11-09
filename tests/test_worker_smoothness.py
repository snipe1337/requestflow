import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock
from discord.http import Route
from requestflow.flow import RequestFlow
from requestflow.priorities import Priority
from requestflow.queue import RequestQueue
from requestflow.models import RequestItem
from requestflow.policies import RetryPolicy


@pytest.mark.asyncio
async def test_queue_no_deadlock_when_routes_cooling_down():
    """Verify that queue.get does not permanently deadlock when routes are in cooldown."""
    q = RequestQueue(max_pending=10)
    cooling_down = True

    async def dummy():
        return 'done'

    item = RequestItem(sequence=1, operation=dummy, route_key='POST /channels/123/messages')
    await q.put(item)

    async def get_worker():
        return await q.get(is_route_limited=lambda r: cooling_down)

    task = asyncio.create_task(get_worker())
    await asyncio.sleep(0.05)
    assert not task.done()  # Waiting because route is cooling down

    # Cooldown expires
    cooling_down = False
    result_item = await asyncio.wait_for(task, timeout=1.0)
    assert result_item is item


@pytest.mark.asyncio
async def test_executor_retry_does_not_block_concurrency():
    """Verify that retrying operations do not hold concurrency permits while backing off."""
    from requestflow.executor import RequestExecutor

    policy = RetryPolicy(max_retries=1, base_delay=0.1, jitter=False, retryable_exceptions=(ValueError,))
    executor = RequestExecutor(max_concurrency=1, default_retry_policy=policy)

    first_attempt = True
    fast_finished = False

    async def flaky_op():
        nonlocal first_attempt
        if first_attempt:
            first_attempt = False
            raise ValueError('temporary failure')
        return 'recovered'

    async def fast_op():
        nonlocal fast_finished
        fast_finished = True
        return 'fast'

    item1 = RequestItem(sequence=1, operation=flaky_op)
    item2 = RequestItem(sequence=2, operation=fast_op)

    t1 = asyncio.create_task(executor.execute(item1))
    await asyncio.sleep(0.02)

    # Item 1 is currently in its 0.1s backoff sleep; concurrency slot should be free for item 2!
    t2 = asyncio.create_task(executor.execute(item2))
    await asyncio.wait_for(t2, timeout=0.08)
    assert fast_finished is True
    assert item2.future.result() == 'fast'

    await asyncio.wait_for(t1, timeout=0.2)
    assert item1.future.result() == 'recovered'


@pytest.mark.asyncio
async def test_global_429_header_pauses_limiter():
    """Verify that X-RateLimit-Global correctly triggers the global rate limiter."""
    import discord
    async with RequestFlow() as flow:
        mock_http = MagicMock()
        mock_response = MagicMock()
        mock_response.headers = {
            'Retry-After': '0.1',
            'X-RateLimit-Global': 'true'
        }
        exc = discord.HTTPException(response=mock_response, message='You are being rate limited.')
        exc.status = 429

        mock_http.request = AsyncMock(side_effect=[exc, {'status': 'success'}])
        mock_bot = MagicMock()
        mock_bot.http = mock_http

        flow.attach(mock_bot)
        route = Route('GET', '/users/@me')

        with pytest.raises(discord.HTTPException):
            await mock_bot.http.request(route)

        # Global rate limiter should now be cooling down!
        assert flow.scheduler.rate_coordinator.global_limiter.is_cooling_down is True
        assert flow.stats.rate_limit_429_count == 1
        assert flow.stats.cooldown_count_global == 1

