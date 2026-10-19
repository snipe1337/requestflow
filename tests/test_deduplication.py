import asyncio
import time
import pytest
from requestflow.deduplication import InFlightDeduplicator, ResultCache


@pytest.mark.asyncio
async def test_inflight_deduplication():
    dedup = InFlightDeduplicator()
    call_count = 0

    async def fetch_roles():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return ['Admin', 'Moderator', 'Member']
    res1, res2 = await asyncio.gather(dedup.execute_or_join('guild_1_roles', fetch_roles), dedup.execute_or_join('guild_1_roles', fetch_roles))
    assert res1 == ['Admin', 'Moderator', 'Member']
    assert res2 == ['Admin', 'Moderator', 'Member']
    assert call_count == 1


@pytest.mark.asyncio
async def test_dedup_caller_cancellation_safety():
    dedup = InFlightDeduplicator()

    async def slow_fetch():
        await asyncio.sleep(0.1)
        return 'data'
    task1 = asyncio.create_task(dedup.execute_or_join('key1', slow_fetch))
    task2 = asyncio.create_task(dedup.execute_or_join('key1', slow_fetch))
    await asyncio.sleep(0.01)
    task1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task1
    res2 = await task2
    assert res2 == 'data'


@pytest.mark.asyncio
async def test_result_cache_ttl_and_lru():
    cache = ResultCache(max_capacity=2, default_ttl_seconds=0.1)
    await cache.set('k1', 'v1')
    await cache.set('k2', 'v2')
    assert await cache.get('k1') == 'v1'
    assert await cache.get('k2') == 'v2'
    await cache.set('k3', 'v3')
    assert len(cache._entries) == 2
    await asyncio.sleep(0.15)
    assert await cache.get('k3') is None
