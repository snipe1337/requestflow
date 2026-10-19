import asyncio
import random
from dataclasses import dataclass
from typing import List, Tuple
from requestflow.priorities import Priority


@dataclass
class SimulatedDiscordRequest:
    guild_id: int
    route_key: str
    priority: Priority
    is_interaction: bool
    mock_network_delay: float


def generate_mixed_discord_workload(count: int = 500) -> List[SimulatedDiscordRequest]:
    guilds = [100000 + i for i in range(10)]
    channels = [500000 + i for i in range(25)]
    workload = []
    for _ in range(count):
        roll = random.random()
        guild = random.choice(guilds)
        channel = random.choice(channels)
        if roll < 0.15:
            workload.append(SimulatedDiscordRequest(
                guild_id=guild, route_key=f'POST /interactions/{random.randint(1000, 9999)}/callback', priority=Priority.CRITICAL, is_interaction=True, mock_network_delay=random.uniform(0.005, 0.015)))
        elif roll < 0.4:
            workload.append(SimulatedDiscordRequest(
                guild_id=guild, route_key=f'DELETE /channels/{channel}/messages', priority=Priority.HIGH, is_interaction=False, mock_network_delay=random.uniform(0.02, 0.05)))
        elif roll < 0.85:
            workload.append(SimulatedDiscordRequest(
                guild_id=guild, route_key=f'POST /channels/{channel}/messages', priority=Priority.NORMAL, is_interaction=False, mock_network_delay=random.uniform(0.02, 0.06)))
        else:
            workload.append(SimulatedDiscordRequest(
                guild_id=guild, route_key=f'GET /guilds/{guild}/members', priority=Priority.LOW, is_interaction=False, mock_network_delay=random.uniform(0.03, 0.1)))
    return workload
