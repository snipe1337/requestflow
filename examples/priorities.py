import asyncio
from requestflow import RequestFlow, Priority


async def main():
    async with RequestFlow(global_rate_limit=20.0, max_concurrency=4) as flow:
        print('submitting operations across different priority levels...\n')

        async def sync_guild_roles():
            await asyncio.sleep(0.05)
            return 'synced 500 guild roles in background'

        async def send_channel_message():
            await asyncio.sleep(0.02)
            return "sent message: 'good morning everyone!'"

        async def ban_spammer():
            await asyncio.sleep(0.02)
            return 'banned spammer from guild 1001'

        async def handle_slash_response():
            return 'immediate ephemeral interaction response (3s deadline preserved)'
        t_low = flow.submit(
            sync_guild_roles, priority=Priority.LOW, guild_id=1001)
        t_norm = flow.submit(send_channel_message,
                             priority=Priority.NORMAL, guild_id=1001)
        t_high = flow.submit(
            ban_spammer, priority=Priority.HIGH, guild_id=1001)
        t_crit = flow.submit(handle_slash_response,
                             priority=Priority.CRITICAL, is_interaction=True)
        results = await asyncio.gather(t_crit, t_high, t_norm, t_low)
        for res in results:
            print(f'result: {res}')
        print(f'\nfinal stats: {flow.stats}')
if __name__ == '__main__':
    asyncio.run(main())
