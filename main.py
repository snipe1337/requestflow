import asyncio
from unittest.mock import AsyncMock
import discord
from discord.http import Route
from requestflow import OverloadPolicy, Priority, RequestFlow, __version__


async def main():
    print('=' * 65)
    print(
        f' requestflow v{__version__} - http scheduling engine for discord.py')
    print('=' * 65)
    async with RequestFlow(global_rate_limit=40.0, rate_limit_ceiling=50.0, max_concurrency=10, max_pending=500, overload_policy=OverloadPolicy.REJECT) as flow:
        print('\n[1] mode a: priority-aware managed operations')
        print('    submitting tasks across multiple priority tiers...')

        async def bot_action(name: str):
            await asyncio.sleep(0.02)
            return f'action completed: {name}'
        t_low = flow.submit(bot_action, 'audit log sync',
                            priority=Priority.LOW, guild_id=101)
        t_norm = flow.submit(bot_action, 'channel message',
                             priority=Priority.NORMAL, guild_id=101)
        t_high = flow.submit(bot_action, 'ban malicious user',
                             priority=Priority.HIGH, guild_id=101)
        t_crit = flow.submit(bot_action, 'slash command interaction response',
                             priority=Priority.CRITICAL, is_interaction=True)
        results = await asyncio.gather(t_crit, t_high, t_norm, t_low)
        for r in results:
            print(f'    - {r}')
        print('\n[2] mode b: transparent discord.py http interception')
        print('    attaching adapter to discord.client...')
        client = discord.Client(intents=discord.Intents.default())
        client.http.request = AsyncMock(
            return_value={'id': 10001, 'content': 'scheduled transparently'})
        flow.attach(client)
        route = Route(
            'POST', '/channels/{channel_id}/messages', channel_id=987654)
        api_res = await client.http.request(route, json={'content': 'hello discord!'})
        print(f'    - intercepted request executed: {api_res}')
        print('\n[3] in-flight deduplication')
        print('    launching 3 concurrent reads with identical dedup_key...')
        fetch_count = 0

        async def fetch_guild_roles():
            nonlocal fetch_count
            fetch_count += 1
            await asyncio.sleep(0.04)
            return ['@everyone', 'moderator', 'vip']
        d1, d2, d3 = await asyncio.gather(flow.submit(fetch_guild_roles, dedup_key='guild_101_roles'), flow.submit(fetch_guild_roles, dedup_key='guild_101_roles'), flow.submit(fetch_guild_roles, dedup_key='guild_101_roles'))
        print(f'    - all 3 callers received: {d1}')
        print(
            f'    - underlying network executions: {fetch_count} (coalesced 3 -> 1)')
        print('\n[4] runtime observability & metrics snapshot')
        s = flow.stats
        print(f'    - total submitted:     {s.submitted}')
        print(f'    - total completed:     {s.completed}')
        print(f'    - peak queue depth:    {s.queue_high_water_mark}')
        print(f'    - median queue wait:   {s.p50_queue_wait_ms:.2f} ms')
        print(f'    - median exec latency: {s.p50_exec_latency_ms:.2f} ms')
        print(
            f'    - rolling throughput:  {s.throughput_window_rps:.2f} req/s')
    print(
        '\n[5] graceful shutdown: all resources, workers, and tasks cleanly released.')
    print('=' * 65)
if __name__ == '__main__':
    asyncio.run(main())
