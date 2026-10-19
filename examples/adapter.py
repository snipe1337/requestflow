import asyncio
from unittest.mock import AsyncMock
import discord
from discord.http import Route
from requestflow import RequestFlow, Priority


async def main():
    client = discord.Client(intents=discord.Intents.default())
    client.http.request = AsyncMock(
        return_value={'id': '9999', 'content': 'hello discord!'})
    async with RequestFlow(bot=client, global_rate_limit=40.0, max_concurrency=10, auto_attach=True) as flow:
        print('requestflow attached transparently to client.http!\n')
        route = Route(
            'POST', '/channels/{channel_id}/messages', channel_id=12345678)
        result = await client.http.request(route, json={'content': 'hello discord!'})
        print('response received transparently:')
        print(f'  {result}')
        print('\nrequestflow recorded metrics for the intercepted call:')
        s = flow.stats
        print(f'  - submitted:  {s.submitted}')
        print(f'  - completed:  {s.completed}')
        print(f'  - in-flight:  {s.currently_executing}')
        print(f'  - wait time:  {s.p50_queue_wait_ms} ms')
        print(f'  - exec time:  {s.p50_exec_latency_ms} ms')
if __name__ == '__main__':
    asyncio.run(main())
