import functools
import inspect
import re
from typing import Any, Callable, Optional, Sequence, Union
try:
    import discord
    import discord.http
    HAS_DISCORD = True
except ImportError:
    HAS_DISCORD = False
from requestflow.exceptions import IntegrationError, UnsupportedDiscordVersionError
from requestflow.priorities import Priority
from requestflow.scheduler import RequestScheduler


def extract_resource_id(route: Any, client: Optional[Any] = None) -> Optional[Union[int, str]]:
    guild_id = getattr(route, 'guild_id', None)
    if guild_id is None and hasattr(route, 'parameters') and isinstance(route.parameters, dict):
        guild_id = route.parameters.get('guild_id')
    if guild_id is not None:
        try:
            return int(guild_id)
        except (ValueError, TypeError):
            return guild_id
    channel_id = getattr(route, 'channel_id', None)
    if channel_id is None and hasattr(route, 'parameters') and isinstance(route.parameters, dict):
        channel_id = route.parameters.get('channel_id')
    if channel_id is not None:
        if client is not None:
            get_channel = getattr(client, 'get_channel', None)
            if callable(get_channel):
                try:
                    ch = get_channel(int(channel_id))
                    if ch is not None:
                        g = getattr(ch, 'guild', None)
                        if g is not None and getattr(g, 'id', None) is not None:
                            return int(g.id)
                except Exception:
                    pass
            state = getattr(client, '_connection', None) or getattr(client, '_state', None)
            if state is not None:
                state_get_channel = getattr(state, 'get_channel', None)
                if callable(state_get_channel):
                    try:
                        ch = state_get_channel(int(channel_id))
                        if ch is not None:
                            g = getattr(ch, 'guild', None)
                            if g is not None and getattr(g, 'id', None) is not None:
                                return int(g.id)
                    except Exception:
                        pass
        try:
            return int(channel_id)
        except (ValueError, TypeError):
            return str(channel_id)
    webhook_id = getattr(route, 'webhook_id', None)
    if webhook_id is None and hasattr(route, 'parameters') and isinstance(route.parameters, dict):
        webhook_id = route.parameters.get('webhook_id')
    if webhook_id is not None:
        try:
            return int(webhook_id)
        except (ValueError, TypeError):
            return str(webhook_id)
    path = getattr(route, 'path', '') or ''
    if '/guilds/' in path:
        m = re.search(r'/guilds/(\d+)', path)
        if m:
            return int(m.group(1))
    if '/channels/' in path:
        m = re.search(r'/channels/(\d+)', path)
        if m:
            cid = int(m.group(1))
            if client is not None:
                get_channel = getattr(client, 'get_channel', None)
                if callable(get_channel):
                    try:
                        ch = get_channel(cid)
                        if ch is not None:
                            g = getattr(ch, 'guild', None)
                            if g is not None and getattr(g, 'id', None) is not None:
                                return int(g.id)
                    except Exception:
                        pass
                state = getattr(client, '_connection', None) or getattr(client, '_state', None)
                if state is not None:
                    state_get_channel = getattr(state, 'get_channel', None)
                    if callable(state_get_channel):
                        try:
                            ch = state_get_channel(cid)
                            if ch is not None:
                                g = getattr(ch, 'guild', None)
                                if g is not None and getattr(g, 'id', None) is not None:
                                    return int(g.id)
                        except Exception:
                            pass
            return cid
    if '/webhooks/' in path:
        m = re.search(r'/webhooks/(\d+)', path)
        if m:
            return int(m.group(1))
    return None


class DiscordHTTPAdapter:
    MIN_SUPPORTED_DISCORD_VERSION = (2, 0, 0)

    def __init__(self, scheduler: RequestScheduler) -> None:
        self.scheduler: RequestScheduler = scheduler
        self._attached_clients: set = set()
        self._original_requests: dict = {}
        self._bot_map: dict = {}

    @classmethod
    def verify_compatibility(cls) -> None:
        if not HAS_DISCORD:
            raise IntegrationError(
                'discord.py is not installed in the current environment.')
        version_str = getattr(discord, '__version__', 'unknown')
        try:
            parts = version_str.split('.')
            major = int(parts[0])
            minor = int(parts[1].rstrip('abcdefghijklmnopqrstuvwxyz'))
            if major < 2:
                raise UnsupportedDiscordVersionError(
                    f'requestflow requires discord.py >= 2.0.0, but found {version_str}.', detected_version=version_str)
        except (ValueError, IndexError):
            if not hasattr(discord.http, 'httpclient') or not hasattr(discord.http, 'route'):
                raise UnsupportedDiscordVersionError(
                    f'unsupported discord.py layout or version: {version_str}.', detected_version=version_str)

    def attach(self, client: Any) -> None:
        self.verify_compatibility()
        http_client = getattr(client, 'http', None)
        if http_client is None:
            if isinstance(client, getattr(discord.http, 'httpclient', object)):
                http_client = client
            else:
                raise IntegrationError(
                    f"target object {type(client).__name__} does not have an 'http' attribute.")
        if not hasattr(http_client, 'request') or not callable(http_client.request):
            raise IntegrationError(
                f"http client {type(http_client).__name__} has no callable 'request' method.")
        if http_client in self._attached_clients:
            return
        self._bot_map[http_client] = client
        original_request = http_client.request
        self._original_requests[http_client] = original_request

        @functools.wraps(original_request)
        async def scheduled_request(route: Any, *args: Any, **kwargs: Any) -> Any:
            bot_client = self._bot_map.get(http_client)
            guild_id = extract_resource_id(route, bot_client)
            route_path = getattr(route, 'path', '')
            route_key = getattr(
                route, 'key', f"{getattr(route, 'method', 'GET')} {route_path}")
            is_interaction = bool(
                route_path and '/interactions/' in route_path)
            if is_interaction:
                priority = Priority.CRITICAL
            elif getattr(route, 'method', '') == 'DELETE':
                priority = Priority.HIGH
            else:
                priority = Priority.NORMAL

            async def run_http() -> Any:
                try:
                    return await original_request(route, *args, **kwargs)
                except getattr(discord, 'ratelimited', Exception) as exc:
                    retry_after = getattr(exc, 'retry_after', 0.0)
                    if retry_after > 0:
                        self.scheduler.rate_coordinator.handle_429(
                            retry_after, is_global=False, route_key=route_key)
                        self.scheduler.metrics.record_429(is_global=False)
                    raise
                except getattr(discord, 'httpexception', Exception) as exc:
                    if getattr(exc, 'status', None) == 429:
                        retry_after = 1.0
                        if hasattr(exc, 'response') and hasattr(exc.response, 'headers'):
                            retry_after = float(
                                exc.response.headers.get('Retry-After', 1.0))
                        self.scheduler.rate_coordinator.handle_429(
                            retry_after, is_global=False, route_key=route_key)
                        self.scheduler.metrics.record_429(is_global=False)
                    raise
            return await self.scheduler.submit(run_http, priority=priority, guild_id=guild_id, route_key=route_key, is_interaction=is_interaction)
        http_client.request = scheduled_request
        self._attached_clients.add(http_client)

    def detach(self, client: Any) -> None:
        http_client = getattr(client, 'http', client)
        if http_client in self._attached_clients:
            self._bot_map.pop(http_client, None)
            original = self._original_requests.pop(http_client, None)
            if original:
                http_client.request = original
            self._attached_clients.remove(http_client)

    def detach_all(self) -> None:
        for http_client, original in list(self._original_requests.items()):
            http_client.request = original
        self._attached_clients.clear()
        self._original_requests.clear()
        self._bot_map.clear()


def attach_to_bot(scheduler: RequestScheduler, bot: Any) -> DiscordHTTPAdapter:
    adapter = DiscordHTTPAdapter(scheduler)
    adapter.attach(bot)
    return adapter
