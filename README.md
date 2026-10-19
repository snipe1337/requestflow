# requestflow

> stop getting 429 ratelimited like an amateur. intelligent http scheduling and pacing for discord.py.

[![pypi](https://img.shields.io/pypi/v/requestflow?color=black&style=flat-square)](https://pypi.org/project/requestflow/)
[![python](https://img.shields.io/badge/python-3.11+-black?style=flat-square)](https://pypi.org/project/requestflow/)
[![stars](https://img.shields.io/github/stars/snipe1337/requestflow?style=flat-square&color=black)](https://github.com/snipe1337/requestflow)
[![discord.py](https://img.shields.io/badge/discord.py-v2-black?style=flat-square)](https://github.com/Rapptz/discord.py)
[![community](https://img.shields.io/badge/community-discord.gg%2Fcse-5865F2?style=flat-square&logo=discord&logoColor=white)](https://discord.gg/cse)
[![license](https://img.shields.io/badge/license-MIT-black?style=flat-square)](LICENSE)

---

### why this exists

when your bot grows, commands collide, background loops spam discord, and unmanaged `asyncio.gather` blasts 300 requests in 10 milliseconds.

discord hits back with `HTTP 429: Global rate limit hit`. your whole bot freezes for everyone. commands fail. messages get dropped.

**requestflow fixes this.** it sits between discord.py and the discord api, intelligently pacing your requests with gcra rate-limiting, cross-guild fairness, and priority queuing.

### raw discord.py vs requestflow (real 300-channel burst test)

```text
raw discord.py (asyncio.gather):
- requested: 300
- created: 240
- dropped/failed: 60 (20% failure rate)
- terminal: spammed dozens of "WARNING Global rate limit hit. Retrying in 0.30s"

requestflow:
- requested: 300
- created: 300 (100% success rate)
- dropped/failed: 0
- terminal: dead silent. zero 429s. zero bans.
```

---

### install

```bash
pip install requestflow
```

---

### quickstart (2 lines)

just attach it to your bot. every single `channel.send()`, `message.delete()`, cog, and slash command is automatically scheduled, paced, and protected.

```python
import discord
from discord.ext import commands
from requestflow import RequestFlow

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

# 1. initialize requestflow
flow = RequestFlow(bot, global_rate_limit=40.0, max_concurrency=16)

@bot.event
async def setup_hook():
    # 2. start the engine
    await flow.start()

@bot.command()
async def ping(ctx):
    # automatically prioritized and paced without changing your code
    await ctx.send("pong")

bot.run("TOKEN")
```

---

### or use managed priorities

want your slash commands and moderation actions to jump ahead of massive background sweeps?

```python
from requestflow import RequestFlow, Priority

async with RequestFlow() as flow:
    # critical interaction responses jump to the front of the queue
    await flow.submit(
        interaction.response.send_message,
        "fast response",
        priority=Priority.CRITICAL,
        is_interaction=True
    )

    # background audits run smoothly without blocking users
    await flow.submit(
        guild.fetch_members,
        limit=1000,
        priority=Priority.LOW,
        guild_id=guild.id
    )
```

---

### features

- **gcra rate pacing**: smooths request bursts before discord gets angry (defaults to 40 req/s, 50 req/s ceiling).
- **zero 429 stampedes**: eliminates the thundering herd problem when rate limits unlock.
- **cross-guild fairness**: deficit round-robin ensures one spammy server never starves your other servers.
- **priority queues**: critical slash commands and moderator bans bypass background syncs.
- **starvation prevention**: low-priority jobs age up over time so they never get stuck forever.
- **in-flight deduplication**: coalesces duplicate reads so identical queries only hit discord once.
- **clean metrics**: real-time p50/p95 latency and queue stats with zero external telemetry.

---

### live metrics

inspect what your bot is doing anytime:

```python
print(flow.stats)
```

```text
MetricsSnapshot(
  submitted=300,
  completed=300,
  failed=0,
  queue_wait_p50=5.0ms,
  exec_latency_p50=32.0ms,
  active_guilds=1,
  rate_limit_429_count=0
)
```

---

### test it yourself

run the included stress test bot against your own discord server:

```bash
python stress_bot.py --token YOUR_TOKEN
```

in discord:
- `!burst_raw 350` — watch raw discord.py drop requests and spam 429 warnings
- `!cleanup` — clear the test channels
- `!burst_flow 350` — watch requestflow create all 200 with 100% success and 0 errors
- `!stats` — see real-time latency and queue percentiles

---

### community & dev

need help, got feature ideas, or building massive bots? join us:

<div align="center">
  <a href="https://discord.gg/cse">
    <img src="https://lanyard.cnrad.dev/api/1420036436505268244?theme=dark&borderRadius=10px&idleMessage=chilling%20with%20requestflow" alt="discord status" />
  </a>
  <br/><br/>
  <a href="https://discord.gg/cse">
    <img src="https://img.shields.io/badge/discord-join%20cse%20community-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="cse discord" />
  </a>
</div>

---

### license

MIT
