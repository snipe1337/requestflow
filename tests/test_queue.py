import asyncio
import time
import pytest
from requestflow.exceptions import QueueFullError
from requestflow.models import RequestItem
from requestflow.policies import OverloadPolicy
from requestflow.priorities import Priority
from requestflow.queue import RequestQueue


async def dummy_op():
    return 'ok'


def make_item(priority: Priority = Priority.NORMAL, guild_id: int = 1, route_key: str = 'GET /test', enqueue_time: float = 0.0) -> RequestItem:
    return RequestItem(sequence=0, operation=dummy_op, priority=priority, effective_priority=priority, guild_id=guild_id, route_key=route_key, enqueue_time=enqueue_time if enqueue_time > 0 else time.monotonic())


@pytest.mark.asyncio
async def test_fifo_ordering_within_priority():
    q = RequestQueue(max_pending=10)
    item1 = make_item(priority=Priority.NORMAL, guild_id=1)
    item2 = make_item(priority=Priority.NORMAL, guild_id=1)
    item3 = make_item(priority=Priority.NORMAL, guild_id=1)
    await q.put(item1)
    await q.put(item2)
    await q.put(item3)
    assert q.size == 3
    assert await q.get() is item1
    assert await q.get() is item2
    assert await q.get() is item3
    assert q.size == 0


@pytest.mark.asyncio
async def test_priority_ordering():
    q = RequestQueue(max_pending=10)
    low = make_item(priority=Priority.LOW)
    normal = make_item(priority=Priority.NORMAL)
    high = make_item(priority=Priority.HIGH)
    crit = make_item(priority=Priority.CRITICAL)
    await q.put(low)
    await q.put(normal)
    await q.put(high)
    await q.put(crit)
    assert await q.get() is crit
    assert await q.get() is high
    assert await q.get() is normal
    assert await q.get() is low


@pytest.mark.asyncio
async def test_overload_policy_reject():
    q = RequestQueue(max_pending=2, overload_policy=OverloadPolicy.REJECT)
    await q.put(make_item())
    await q.put(make_item())
    with pytest.raises(QueueFullError):
        await q.put(make_item())


@pytest.mark.asyncio
async def test_overload_policy_drop_oldest():
    q = RequestQueue(max_pending=2, overload_policy=OverloadPolicy.DROP_OLDEST)
    item1 = make_item(priority=Priority.NORMAL)
    item2 = make_item(priority=Priority.NORMAL)
    item3 = make_item(priority=Priority.CRITICAL)
    await q.put(item1)
    await q.put(item2)
    await q.put(item3)
    assert item1.cancelled is True
    assert item1.future.done()
    with pytest.raises(QueueFullError):
        item1.future.result()
    assert q.size == 2
    assert await q.get() is item3
    assert await q.get() is item2


@pytest.mark.asyncio
async def test_overload_policy_wait():
    q = RequestQueue(max_pending=1, overload_policy=OverloadPolicy.WAIT)
    item1 = make_item()
    item2 = make_item()
    await q.put(item1)

    async def get_after_delay():
        await asyncio.sleep(0.05)
        return await q.get()
    task = asyncio.create_task(get_after_delay())
    await q.put(item2, timeout=0.5)
    assert q.size == 1
    assert await task is item1
    assert await q.get() is item2


@pytest.mark.asyncio
async def test_starvation_prevention_aging():
    q = RequestQueue(max_pending=10, aging_threshold_seconds=0.05)
    past_time = time.monotonic() - 0.1
    low_item = make_item(priority=Priority.LOW, enqueue_time=past_time)
    await q.put(low_item)
    extracted = await q.get()
    assert extracted is low_item
    assert extracted.effective_priority < Priority.LOW


@pytest.mark.asyncio
async def test_cancellation_removal():
    q = RequestQueue(max_pending=10)
    item = make_item()
    await q.put(item)
    assert q.size == 1
    removed = q.remove_item(item)
    assert removed is True
    assert q.size == 0
