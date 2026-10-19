import asyncio
import pytest
from requestflow.priorities import Priority
from requestflow.queue import RequestQueue
from requestflow.models import RequestItem


async def dummy_work():
    return 'ok'


def make_item(guild_id: int, route_key: str = 'GET /test') -> RequestItem:
    return RequestItem(sequence=0, operation=dummy_work, guild_id=guild_id, route_key=route_key, priority=Priority.NORMAL)


@pytest.mark.asyncio
async def test_cross_guild_fairness_interleaving():
    q = RequestQueue(max_pending=100, guild_quantum=2)
    guild_a = 1001
    guild_b = 2002
    for _ in range(6):
        await q.put(make_item(guild_id=guild_a))
    for _ in range(2):
        await q.put(make_item(guild_id=guild_b))
    pulled_guilds = []
    for _ in range(8):
        item = await q.get()
        pulled_guilds.append(item.guild_id)
    assert pulled_guilds[:2] == [guild_a, guild_a]
    assert pulled_guilds[2:4] == [guild_b, guild_b]
    assert pulled_guilds[4:] == [guild_a, guild_a, guild_a, guild_a]


@pytest.mark.asyncio
async def test_fairness_skips_cooling_down_route():
    q = RequestQueue(max_pending=50, guild_quantum=2)
    guild_a = 1001
    guild_b = 2002
    route_a = 'POST /channels/1/messages'
    route_b = 'POST /channels/2/messages'
    await q.put(make_item(guild_id=guild_a, route_key=route_a))
    await q.put(make_item(guild_id=guild_b, route_key=route_b))

    def is_route_limited(r: str) -> bool:
        return r == route_a
    item = await q.get(is_route_limited=is_route_limited)
    assert item.guild_id == guild_b
    assert item.route_key == route_b
