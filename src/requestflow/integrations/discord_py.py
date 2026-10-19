import functools
import inspect
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


class DiscordHTTPAdapter:
    MIN_SUPPORTED_DISCORD_VERSION = (2, 0, 0)

    def __init__(self, scheduler: RequestScheduler) -> None:
        self.scheduler: RequestScheduler = scheduler
        self._attached_clients: set = set()
        self._original_requests: dict = {}

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
        original_request = http_client.request
        self._original_requests[http_client] = original_request

        @functools.wraps(original_request)
        async def scheduled_request(route: Any, *args: Any, **kwargs: Any) -> Any:
            guild_id = getattr(route, 'guild_id', None)
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
            original = self._original_requests.pop(http_client, None)
            if original:
                http_client.request = original
            self._attached_clients.remove(http_client)

    def detach_all(self) -> None:
        for http_client, original in list(self._original_requests.items()):
            http_client.request = original
        self._attached_clients.clear()
        self._original_requests.clear()


def attach_to_bot(scheduler: RequestScheduler, bot: Any) -> DiscordHTTPAdapter:
    adapter = DiscordHTTPAdapter(scheduler)
    adapter.attach(bot)
    return adapter
