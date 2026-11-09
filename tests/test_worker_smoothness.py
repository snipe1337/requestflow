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
            'Retry-After': '0.05',
            'X-RateLimit-Global': 'true'
        }
        exc = discord.HTTPException(response=mock_response, message='You are being rate limited.')
        exc.status = 429

        mock_http.request = AsyncMock(side_effect=[exc, {'status': 'success'}])
        mock_bot = MagicMock()
        mock_bot.http = mock_http

        flow.attach(mock_bot)
        route = Route('GET', '/users/@me')

        # RequestFlow automatically handles the 429, sleeps retry_after, and recovers transparently!
        result = await mock_bot.http.request(route)
        assert result == {'status': 'success'}

        # Global rate limiter recorded the 429 and cooldown count
        assert flow.stats.rate_limit_429_count == 1
        assert flow.stats.cooldown_count_global == 1


@pytest.mark.asyncio
async def test_mode_a_managed_call_handles_429_transparently():
    """Verify that Mode A (flow.submit) also benefits from transparent 429 auto-retry."""
    import discord
    async with RequestFlow() as flow:
        mock_http = MagicMock()
        mock_response = MagicMock()
        mock_response.headers = {'Retry-After': '0.05'}
        exc = discord.HTTPException(response=mock_response, message='Too Many Requests')
        exc.status = 429

        mock_http.request = AsyncMock(side_effect=[exc, {'status': 'mode_a_success'}])
        original_mock = mock_http.request
        mock_bot = MagicMock()
        mock_bot.http = mock_http
        flow.attach(mock_bot)

        route = Route('POST', '/channels/{channel_id}/messages', channel_id=555)

        async def send_msg():
            return await mock_bot.http.request(route, json={'content': 'test'})

        res = await flow.submit(send_msg)
        assert res == {'status': 'mode_a_success'}
        assert original_mock.call_count == 2


@pytest.mark.asyncio
async def test_route_buckets_empty_major_parameter_matching():
    """Verify that candidate matching checks both with and without trailing colon."""
    async with RequestFlow() as flow:
        mock_bot = MagicMock()
        mock_http = MagicMock()
        mock_rl = MagicMock()
        mock_rl.remaining = 0
        mock_rl.is_expired.return_value = False
        mock_rl.expires = None
        mock_http._buckets = {'GET /users/@me:': mock_rl}
        mock_http._bucket_hashes = {}
        mock_bot.http = mock_http
        flow.attach(mock_bot)

        # Discord bucket checker should detect that the route is cooling down
        is_limited = flow.scheduler.rate_coordinator.route_coordinator.is_route_cooling_down('GET /users/@me')
        assert is_limited is True


@pytest.mark.asyncio
async def test_executor_coroutine_object_support():
    """Verify that passing an instantiated coroutine object directly works seamlessly."""
    async with RequestFlow() as flow:
        async def work():
            return 'direct_coro_done'

        res = await flow.submit(work())
        assert res == 'direct_coro_done'


@pytest.mark.asyncio
async def test_route_rate_limit_does_not_block_other_routes_or_guilds():
    """Verify that a rate-limited route in one channel does NOT block other channels or guilds."""
    async with RequestFlow(safety_buffer_seconds=0.01) as flow:
        route_cooling = 'POST /channels/1/messages'
        route_ready_1 = 'POST /channels/2/messages'
        route_ready_2 = 'POST /channels/3/messages'

        # Put route 1 into cooldown for 0.4s
        flow.scheduler.rate_coordinator.route_coordinator.update_route_cooldown(route_cooling, 0.4)

        order_finished = []

        async def op_cooling():
            order_finished.append('cooling_done')
            return 'cooling'

        async def op_ready_1():
            order_finished.append('ready_1_done')
            return 'ready_1'

        async def op_ready_2():
            order_finished.append('ready_2_done')
            return 'ready_2'

        # Submit cooling item first in Guild 100
        t_cooling = asyncio.create_task(flow.submit(op_cooling, guild_id=100, route_key=route_cooling))
        # Submit ready item second in same Guild 100
        t_ready_1 = asyncio.create_task(flow.submit(op_ready_1, guild_id=100, route_key=route_ready_1))
        # Submit ready item in different Guild 200
        t_ready_2 = asyncio.create_task(flow.submit(op_ready_2, guild_id=200, route_key=route_ready_2))

        # Both ready items should finish very quickly (< 0.15s), NOT blocked by cooling route!
        res_ready_1 = await asyncio.wait_for(t_ready_1, timeout=0.2)
        res_ready_2 = await asyncio.wait_for(t_ready_2, timeout=0.2)

        assert res_ready_1 == 'ready_1'
        assert res_ready_2 == 'ready_2'
        assert 'ready_1_done' in order_finished
        assert 'ready_2_done' in order_finished
        assert 'cooling_done' not in order_finished

        # Cooling item finishes after cooldown
        res_cooling = await asyncio.wait_for(t_cooling, timeout=0.8)
        assert res_cooling == 'cooling'
        assert 'cooling_done' in order_finished


