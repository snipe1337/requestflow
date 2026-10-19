import os
import discord
from discord.ext import commands, tasks
from requestflow import RequestFlow, Priority


class ModerationCog(commands.Cog):
    def __init__(self, bot: commands.Bot, flow: RequestFlow):
        self.bot = bot
        self.flow = flow

    @commands.command()
    async def purge(self, ctx: commands.Context, limit: int = 50):
        await ctx.send(f'purging {limit} messages with high priority...')
        deleted = await ctx.channel.purge(limit=limit)
        await ctx.send(f'deleted {len(deleted)} messages.')


class AuditSyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot, flow: RequestFlow):
        self.bot = bot
        self.flow = flow
        self.background_sync.start()

    def cog_unload(self):
        self.background_sync.cancel()

    @tasks.loop(minutes=5.0)
    async def background_sync(self):
        async def sync_guild(guild: discord.Guild):
            return f'synced guild {guild.id}'
        for guild in self.bot.guilds:
            await self.flow.submit(sync_guild, guild, priority=Priority.LOW, guild_id=guild.id)


async def setup_bot():
    intents = discord.Intents.default()
    bot = commands.Bot(command_prefix='!', intents=intents)
    flow = RequestFlow(bot=bot, global_rate_limit=40.0, max_concurrency=16)

    async def setup_hook():
        await flow.start()
        await bot.add_cog(ModerationCog(bot, flow))
        await bot.add_cog(AuditSyncCog(bot, flow))
    bot.setup_hook = setup_hook
    return (bot, flow)
if __name__ == '__main__':
    print('cogs example ready. wire with bot.run(token).')
