import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest
import discord
from discord.http import Route
from requestflow.flow import RequestFlow
from requestflow.integrations.discord_py import DiscordHTTPAdapter, attach_to_bot, extract_resource_id
from requestflow.priorities import Priority


class MockHTTPClient:
    def __init__(self):
        self.request = AsyncMock()


class MockBot:
    def __init__(self):
        self.http = MockHTTPClient()


def test_compatibility_check():
    DiscordHTTPAdapter.verify_compatibility()


def test_resource_id_extraction():
    route_guild = Route('GET', '/guilds/{guild_id}/members', guild_id=123)
    assert extract_resource_id(route_guild) == 123

    route_channel = Route('POST', '/channels/{channel_id}/messages', channel_id=456)
    assert extract_resource_id(route_channel) == 456

    route_webhook = Route('POST', '/webhooks/{webhook_id}/{webhook_token}', webhook_id=789, webhook_token='tok')
    assert extract_resource_id(route_webhook) == 789

    mock_channel = MagicMock()
    mock_channel.guild.id = 999
    mock_bot = MagicMock()
    mock_bot.get_channel.return_value = mock_channel
    assert extract_resource_id(route_channel, mock_bot) == 999


@pytest.mark.asyncio
async def test_transparent_adapter_mode_b():
    async with RequestFlow() as flow:
        bot = MockBot()
        original_request = bot.http.request
        original_request.return_value = {'id': '12345', 'content': 'hello'}
        flow.attach(bot)
        assert bot.http.request != original_request
        route = Route(
            'POST', '/channels/{channel_id}/messages', channel_id=98765)
        result = await bot.http.request(route, json={'content': 'hello'})
        assert result == {'id': '12345', 'content': 'hello'}
        original_request.assert_called_once()
        assert flow.stats.submitted == 1
        assert flow.stats.completed == 1
        flow.detach(bot)
        assert bot.http.request == original_request


@pytest.mark.asyncio
async def test_automatic_cross_guild_fairness_partitioning():
    async with RequestFlow() as flow:
        bot = MockBot()
        bot.http.request.return_value = {'status': 'ok'}
        flow.attach(bot)
        route_a = Route('POST', '/channels/{channel_id}/messages', channel_id=1001)
        route_b = Route('POST', '/channels/{channel_id}/messages', channel_id=2002)
        t1 = asyncio.create_task(bot.http.request(route_a))
        t2 = asyncio.create_task(bot.http.request(route_b))
        await asyncio.gather(t1, t2)
        assert flow.stats.submitted == 2
        assert flow.stats.completed == 2


@pytest.mark.asyncio
async def test_interaction_endpoint_critical_priority():
    async with RequestFlow() as flow:
        bot = MockBot()
        bot.http.request.return_value = {'type': 4}
        flow.attach(bot)
        route = Route(
            'POST', '/interactions/{interaction_id}/{interaction_token}/callback', interaction_id=111, interaction_token='xyz')
        res = await bot.http.request(route, json={'type': 4})
        assert res == {'type': 4}
        assert flow.stats.submitted == 1
        assert flow.stats.completed == 1


@pytest.mark.asyncio
async def test_managed_mode_a_with_discord_call():
    async with RequestFlow() as flow:
        mock_send = AsyncMock(
            return_value={'id': 1, 'content': 'managed message'})
        res = await flow.submit(mock_send, content='managed message', priority=Priority.HIGH, guild_id=123456)
        assert res == {'id': 1, 'content': 'managed message'}
        mock_send.assert_called_once_with(content='managed message')
        assert flow.stats.completed == 1
