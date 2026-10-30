import asyncio
from typing import Any, Callable, Coroutine, Optional, TypeVar, Union
from requestflow.deduplication import InFlightDeduplicator, ResultCache
from requestflow.exceptions import RequestFlowError
from requestflow.integrations.discord_py import DiscordHTTPAdapter
from requestflow.models import MetricsSnapshot
from requestflow.policies import OverloadPolicy, RetryPolicy
from requestflow.priorities import Priority
from requestflow.scheduler import RequestScheduler
T = TypeVar('T')


class RequestFlow:
    def __init__(self, bot: Optional[Any] = None, *, max_pending: int = 5000, max_concurrency: int = 16, global_rate_limit: float = 40.0, rate_limit_ceiling: float = 50.0, burst_allowance: float = 5.0, allow_custom_ceiling: bool = False, overload_policy: Union[OverloadPolicy, str] = OverloadPolicy.REJECT, aging_threshold_seconds: float = 2.0, guild_quantum: int = 3, default_timeout: Optional[float] = None, default_retry_policy: Optional[RetryPolicy] = None, auto_attach: bool = True) -> None:
        if isinstance(overload_policy, str):
            overload_policy = OverloadPolicy(overload_policy.lower())
        self.scheduler = RequestScheduler(max_pending=max_pending, max_concurrency=max_concurrency, global_rate_limit=global_rate_limit, rate_limit_ceiling=rate_limit_ceiling, burst_allowance=burst_allowance,
                                          allow_custom_ceiling=allow_custom_ceiling, overload_policy=overload_policy, aging_threshold_seconds=aging_threshold_seconds, guild_quantum=guild_quantum, default_timeout=default_timeout, default_retry_policy=default_retry_policy)
        self._bot: Optional[Any] = bot
        self._auto_attach: bool = auto_attach
        self._adapter: DiscordHTTPAdapter = DiscordHTTPAdapter(self.scheduler)
        self.deduplicator: InFlightDeduplicator = InFlightDeduplicator()
        self.cache: ResultCache = ResultCache()

    @property
    def metrics(self):
        return self.scheduler.metrics

    @property
    def stats(self) -> MetricsSnapshot:
        return self.scheduler.metrics.snapshot()

    @property
    def is_started(self) -> bool:
        return self.scheduler.is_started

    @property
    def is_closed(self) -> bool:
        return self.scheduler.is_closed

    async def start(self) -> 'RequestFlow':
        await self.scheduler.start()
        if self._bot is not None and self._auto_attach:
            self.attach(self._bot)
        return self

    async def close(self, drain: bool = True, timeout: float = 5.0) -> None:
        self._adapter.detach_all()
        await self.scheduler.close(drain=drain, timeout=timeout)

    async def __aenter__(self) -> 'RequestFlow':
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close(drain=True)

    def attach(self, bot: Any) -> None:
        self._adapter.attach(bot)

    def detach(self, bot: Any) -> None:
        self._adapter.detach(bot)

    async def update_rate(self, new_rate: float) -> None:
        await self.scheduler.rate_coordinator.global_limiter.update_rate(new_rate)

    async def submit(self, operation: Callable[..., Coroutine[Any, Any, T]], *args: Any, priority: Union[Priority, str, int] = Priority.NORMAL, guild_id: Optional[Union[int, str]] = None, route_key: Optional[str] = None, is_interaction: bool = False, dedup_key: Optional[str] = None, timeout: Optional[float] = None, enqueue_timeout: Optional[float] = None, **kwargs: Any) -> T:
        parsed_priority = Priority.from_value(priority)
        if dedup_key is not None:
            async def run_single() -> T:
                return await self.scheduler.submit(operation, *args, priority=parsed_priority, guild_id=guild_id, route_key=route_key, is_interaction=is_interaction, dedup_key=dedup_key, timeout=timeout, enqueue_timeout=enqueue_timeout, **kwargs)
            return await self.deduplicator.execute_or_join(dedup_key, run_single)
        return await self.scheduler.submit(operation, *args, priority=parsed_priority, guild_id=guild_id, route_key=route_key, is_interaction=is_interaction, dedup_key=dedup_key, timeout=timeout, enqueue_timeout=enqueue_timeout, **kwargs)
