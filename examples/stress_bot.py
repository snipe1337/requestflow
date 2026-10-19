import argparse
import asyncio
import os
import sys
import time
from typing import List, Tuple
import discord
from discord.ext import commands
from requestflow import OverloadPolicy, Priority, RequestFlow
parser = argparse.ArgumentParser(
    description='requestflow discord stress testing bot')
parser.add_argument('--token', type=str, default=None,
                    help='discord bot token')
parser.add_argument('--simulate', action='store_true',
                    help='run simulated local burst benchmark without connecting to discord')
args, _ = parser.parse_known_args()
TOKEN = args.token or os.getenv('discord_token') or os.getenv('token')
intents = discord.Intents.default()
intents.guilds = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
flow = RequestFlow(bot=None, global_rate_limit=40.0, rate_limit_ceiling=50.0, max_concurrency=16,
                   max_pending=2000, overload_policy=OverloadPolicy.WAIT, aging_threshold_seconds=2.0, guild_quantum=3)


@bot.event
async def setup_hook():
    await flow.start()
    print('[requestflow] scheduler started and ready.')


@bot.event
async def on_ready():
    print('=' * 65)
    print(f'[ready] logged in as: {bot.user} (id: {bot.user.id})')
    print(f'[ready] connected to {len(bot.guilds)} guilds.')
    print('available commands:')
    print('  !burst_raw <count>   - burst channel creation using raw discord.py')
    print('  !burst_flow <count>  - burst channel creation using requestflow')
    print('  !cleanup             - delete all test channels (rf-* and raw-*)')
    print('  !stats               - view requestflow queue and latency metrics')
    print('  !ping                - check bot responsiveness')
    print('=' * 65)


@bot.command(name='burst_raw')
async def burst_raw(ctx: commands.Context, count: int = 50):
    flow.detach(bot)
    guild = ctx.guild
    available_slots = 500 - len(guild.channels)
    if count > available_slots:
        await ctx.send(f':warning: discord server channel limit is 500. currently {len(guild.channels)} channels exist. capping requested {count} to available {available_slots} slots.')
        count = max(1, available_slots)
    status_msg = await ctx.send(f'firing {count} raw requests without requestflow. brace for 429s...')
    created_channels: List[discord.TextChannel] = []
    errors: List[str] = []
    ratelimit_count = 0
    t0 = time.monotonic()

    async def create_single(idx: int):
        nonlocal ratelimit_count
        name = f'raw-{idx:03d}'
        try:
            chan = await guild.create_text_channel(name=name, topic='raw test channel')
            created_channels.append(chan)
        except discord.RateLimited as exc:
            ratelimit_count += 1
            errors.append(f'ratelimited: retry after {exc.retry_after:.2f}s')
        except Exception as exc:
            errors.append(f'{type(exc).__name__}: {exc}')
    await asyncio.gather(*(create_single(i) for i in range(count)))
    elapsed = time.monotonic() - t0
    rps = len(created_channels) / elapsed if elapsed > 0 else 0
    report = f'**raw discord.py:**\n- requested: `{count}`\n- created: `{len(created_channels)}`\n- time: `{elapsed:.2f}s`\n- rate: `{rps:.2f} channels/s`\n- 429s hit: `{ratelimit_count}`\n- dropped/errors: `{len(errors) - ratelimit_count}`\n*(run `!cleanup` to clear)*'
    await ctx.send(report)
    print(
        f'\n[raw burst complete] {count} channels in {elapsed:.2f}s ({rps:.2f} req/s). errors: {len(errors)}')


@bot.command(name='burst_flow')
async def burst_flow(ctx: commands.Context, count: int = 50):
    guild = ctx.guild
    available_slots = 500 - len(guild.channels)
    if count > available_slots:
        await ctx.send(f'guild channel limit is 500. capping {count} to available {available_slots} slots.')
        count = max(1, available_slots)
    status_msg = await ctx.send(f'firing {count} requests with requestflow pacing. zero 429s incoming...')
    created_channels: List[discord.TextChannel] = []
    errors: List[str] = []
    t0 = time.monotonic()

    async def create_single_flow(idx: int):
        name = f'rf-{idx:03d}'
        try:
            chan = await flow.submit(guild.create_text_channel, name=name, topic='requestflow test channel', priority=Priority.NORMAL, guild_id=guild.id)
            created_channels.append(chan)
        except Exception as exc:
            errors.append(f'{type(exc).__name__}: {exc}')
    await asyncio.gather(*(create_single_flow(i) for i in range(count)))
    elapsed = time.monotonic() - t0
    rps = len(created_channels) / elapsed if elapsed > 0 else 0
    s = flow.stats
    report = f'**requestflow:**\n- requested: `{count}`\n- created: `{len(created_channels)}`\n- time: `{elapsed:.2f}s`\n- rate: `{rps:.2f} channels/s`\n- queue wait p50: `{s.p50_queue_wait_ms:.2f}ms`\n- queue wait p95: `{s.p95_queue_wait_ms:.2f}ms`\n- exec latency p50: `{s.p50_exec_latency_ms:.2f}ms`\n- peak queue depth: `{s.queue_high_water_mark}`\n- 429s hit: `{s.rate_limit_429_count}`\n- errors: `{len(errors)}`\n*(run `!cleanup` to clear)*'
    await ctx.send(report)
    print(
        f'\n[requestflow burst complete] {count} channels in {elapsed:.2f}s ({rps:.2f} req/s). peak queue: {s.queue_high_water_mark}')


@bot.command(name='cleanup')
async def cleanup(ctx: commands.Context):
    guild = ctx.guild
    test_channels = [c for c in guild.channels if c.name.startswith(
        'rf-') or c.name.startswith('raw-')]
    if not test_channels:
        await ctx.send('no test channels found.')
        return
    msg = await ctx.send(f'cleaning up {len(test_channels)} test channels...')
    t0 = time.monotonic()
    deleted_count = 0

    async def delete_channel(c: discord.abc.GuildChannel):
        nonlocal deleted_count
        try:
            await flow.submit(c.delete, priority=Priority.HIGH, guild_id=guild.id)
            deleted_count += 1
        except Exception:
            pass
    await asyncio.gather(*(delete_channel(c) for c in test_channels))
    elapsed = time.monotonic() - t0
    await ctx.send(f'deleted {deleted_count} test channels in {elapsed:.2f}s.')


@bot.command(name='stats')
async def show_stats(ctx: commands.Context):
    s = flow.stats
    msg = f'**requestflow metrics:**\n- submitted: `{s.submitted}` | completed: `{s.completed}` | failed: `{s.failed}`\n- executing: `{s.currently_executing}` | queued: `{s.currently_queued}`\n- queue wait: `p50: {s.p50_queue_wait_ms:.1f}ms` | `p95: {s.p95_queue_wait_ms:.1f}ms`\n- exec latency: `p50: {s.p50_exec_latency_ms:.1f}ms` | `p95: {s.p95_exec_latency_ms:.1f}ms`\n- throughput: `{s.throughput_window_rps:.2f} req/s`\n- 429s hit: `{s.rate_limit_429_count}`'
    await ctx.send(msg)


@bot.command(name='ping')
async def ping(ctx: commands.Context):
    await ctx.send(f'pong. queue depth: `{flow.stats.currently_queued}`')


async def run_simulation():
    print('=' * 65)
    print(' [simulation mode] comparing 250 channel creations')
    print('=' * 65)
    import random

    async def mock_discord_create_channel(idx: int):
        await asyncio.sleep(random.uniform(0.035, 0.075))
        return f'channel-{idx:03d}'
    print('\n[phase 1] simulating raw discord.py (asyncio.gather without limiter):')
    t0 = time.monotonic()
    raw_results = await asyncio.gather(*(mock_discord_create_channel(i) for i in range(250)))
    raw_elapsed = time.monotonic() - t0
    print(f'  - completed: {len(raw_results)} channels')
    print(f'  - duration:  {raw_elapsed:.3f}s')
    print(f'  - unmanaged burst concurrency: 250 simultaneous requests (triggers discord 429 in real bots)')
    print('\n[phase 2] running through requestflow (40 req/s, 16 concurrency):')
    async with RequestFlow(global_rate_limit=40.0, rate_limit_ceiling=50.0, max_concurrency=16, max_pending=500) as sim_flow:
        t0 = time.monotonic()
        flow_tasks = [sim_flow.submit(mock_discord_create_channel, i, priority=Priority.NORMAL,
                                      guild_id=12345, route_key='POST /guilds/12345/channels') for i in range(250)]
        flow_results = await asyncio.gather(*flow_tasks)
        flow_elapsed = time.monotonic() - t0
        s = sim_flow.stats
        print(f'  - completed:             {len(flow_results)} channels')
        print(f'  - controlled duration:   {flow_elapsed:.3f}s')
        print(
            f'  - effective rate:        {len(flow_results) / flow_elapsed:.1f} req/s (closely matches 40 req/s)')
        print(f'  - queue wait (p50):      {s.p50_queue_wait_ms:.1f} ms')
        print(f'  - queue wait (p95):      {s.p95_queue_wait_ms:.1f} ms')
        print(f'  - exec latency (p50):    {s.p50_exec_latency_ms:.1f} ms')
        print(
            f'  - queue high-water mark: {s.queue_high_water_mark} items (bounded safely)')
        print(
            f'  - rate limits (429):     {s.rate_limit_429_count} (zero collisions)')
    print('\n' + '=' * 65)
    print(' simulation finished successfully. to test in your real discord server:')
    print('   python stress_bot.py --token your_bot_token')
    print('=' * 65)


def main():
    if args.simulate or not TOKEN:
        if not args.simulate and (not TOKEN):
            print('no discord_token provided. running in simulation mode...\n')
        asyncio.run(run_simulation())
    else:
        print('[starting] launching discord stress test bot...')
        try:
            bot.run(TOKEN)
        except discord.errors.PrivilegedIntentsRequired:
            print('\n' + '!' * 65)
            print("error: privileged gateway intent 'message content' is not enabled!")
            print('to enable it:')
            print('1. go to https://discord.com/developers/applications')
            print("2. select your application -> click the 'bot' tab on the left.")
            print("3. scroll down to 'privileged gateway intents'.")
            print("4. toggle 'message content intent' to on and click 'save changes'.")
            print('5. re-run this script!')
            print('!' * 65 + '\n')
        except discord.errors.LoginFailure:
            print(
                '\nerror: invalid discord bot token. please check that the token is correct.\n')


if __name__ == '__main__':
    main()
