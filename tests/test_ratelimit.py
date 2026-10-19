import asyncio
import time
import pytest
from requestflow.ratelimit import GlobalRateLimiter, RateLimitCoordinator, RouteBucketCoordinator


@pytest.mark.asyncio
async def test_global_rate_limiter_validation():
    with pytest.raises(ValueError):
        GlobalRateLimiter(rate=0)
    with pytest.raises(ValueError):
        GlobalRateLimiter(rate=-10)
    with pytest.raises(ValueError):
        GlobalRateLimiter(rate=60.0, ceiling=50.0, allow_custom_ceiling=False)
    limiter = GlobalRateLimiter(
        rate=100.0, ceiling=50.0, allow_custom_ceiling=True)
    assert limiter.rate == 100.0


@pytest.mark.asyncio
async def test_interaction_bypass_pacing():
    limiter = GlobalRateLimiter(rate=5.0, burst_allowance=1.0)
    t0 = time.monotonic()
    for _ in range(5):
        delay = await limiter.acquire(is_interaction=True)
        assert delay == 0.0
    elapsed = time.monotonic() - t0
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_route_cooldown_independence():
    route_coord = RouteBucketCoordinator()
    route_a = 'POST /channels/1/messages'
    route_b = 'POST /channels/2/messages'
    route_coord.update_route_cooldown(route_a, retry_after=1.0)
    assert route_coord.is_route_cooling_down(route_a) is True
    assert route_coord.is_route_cooling_down(route_b) is False
    assert route_coord.get_route_delay(route_b) == 0.0
    assert route_coord.get_route_delay(route_a) > 0.0


@pytest.mark.asyncio
async def test_route_header_update():
    route_coord = RouteBucketCoordinator()
    route = 'GET /guilds/123/members'
    route_coord.update_from_headers(
        route, {'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset-After': '0.5'})
    assert route_coord.is_route_cooling_down(route) is True
    assert route_coord.get_route_delay(route) > 0.0


@pytest.mark.asyncio
async def test_global_429_cooldown():
    limiter = GlobalRateLimiter(rate=40.0)
    limiter.handle_global_429(retry_after=0.1)
    assert limiter.is_cooling_down is True
    t0 = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.08
    assert limiter.is_cooling_down is False


@pytest.mark.asyncio
async def test_dynamic_rate_update():
    limiter = GlobalRateLimiter(rate=20.0, ceiling=50.0)
    assert limiter.rate == 20.0
    await limiter.update_rate(40.0)
    assert limiter.rate == 40.0
    with pytest.raises(ValueError):
        await limiter.update_rate(60.0)


@pytest.mark.asyncio
async def test_limiter_cancellation_safety():
    limiter = GlobalRateLimiter(rate=2.0)

    async def acquire_task():
        await limiter.acquire()
    await limiter.acquire()
    task = asyncio.create_task(acquire_task())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    t0 = time.monotonic()
    await limiter.acquire()
    assert time.monotonic() - t0 < 0.6
