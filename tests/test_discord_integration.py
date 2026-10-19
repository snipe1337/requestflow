import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest
import discord
from discord.http import Route
from requestflow.flow import RequestFlow
from requestflow.integrations.discord_py import DiscordHTTPAdapter, attach_to_bot
from requestflow.priorities import Priority


class MockHTTPClient:
    def __init__(self):
        self.request = AsyncMock()


class MockBot:
    def __init__(self):
        self.http = MockHTTPClient()


def test_compatibility_check():
    DiscordHTTPAdapter.verify_compatibility()


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
