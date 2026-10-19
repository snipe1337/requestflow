import os
import discord
from discord.ext import commands
from requestflow import RequestFlow, Priority
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
flow = RequestFlow(bot=bot, global_rate_limit=40.0,
                   rate_limit_ceiling=50.0, max_concurrency=16, max_pending=2000)


@bot.event
async def setup_hook():
    await flow.start()
    print('requestflow started and attached to bot.http!')


@bot.event
async def on_ready():
    print(f'logged in as {bot.user} (id: {bot.user.id})')
    print(f'current scheduler metrics: {flow.stats}')


@bot.command()
async def ping(ctx: commands.Context):
    await ctx.send('pong! this message was scheduled through requestflow.')


@bot.command()
async def stats(ctx: commands.Context):
    s = flow.stats
    msg = f'**requestflow runtime stats**:\n- submitted: {s.submitted}\n- completed: {s.completed}\n- in-flight: {s.currently_executing}\n- queued: {s.currently_queued}\n- median queue wait: {s.p50_queue_wait_ms:.2f}ms\n- median execution latency: {s.p50_exec_latency_ms:.2f}ms'
    await ctx.send(msg)
if __name__ == '__main__':
    token = os.getenv('discord_bot_token', 'your_bot_token_here')
    print('example ready. provide discord_bot_token and run bot.run(token).')
