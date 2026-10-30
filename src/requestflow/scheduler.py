import asyncio
import time
from typing import Any, Callable, Coroutine, List, Optional, Set, TypeVar, Union
from requestflow.exceptions import OperationCancelledError, QueueFullError, SchedulerClosedError, SchedulerNotStartedError
from requestflow.executor import RequestExecutor
from requestflow.metrics import RequestMetrics
from requestflow.models import RequestItem
from requestflow.policies import OverloadPolicy, RetryPolicy
from requestflow.priorities import Priority
from requestflow.queue import RequestQueue
from requestflow.ratelimit import RateLimitCoordinator
T = TypeVar('t')


class RequestScheduler:
    def __init__(self, max_pending: int = 5000, max_concurrency: int = 16, global_rate_limit: float = 40.0, rate_limit_ceiling: float = 50.0, burst_allowance: float = 5.0, allow_custom_ceiling: bool = False, overload_policy: OverloadPolicy = OverloadPolicy.REJECT, aging_threshold_seconds: float = 2.0, guild_quantum: int = 3, default_timeout: Optional[float] = None, default_retry_policy: Optional[RetryPolicy] = None) -> None:
        self.queue = RequestQueue(max_pending=max_pending, overload_policy=overload_policy,
                                  aging_threshold_seconds=aging_threshold_seconds, guild_quantum=guild_quantum)
        self.executor = RequestExecutor(
            max_concurrency=max_concurrency, default_timeout=default_timeout, default_retry_policy=default_retry_policy)
        self.rate_coordinator = RateLimitCoordinator(global_rate_limit=global_rate_limit, rate_limit_ceiling=rate_limit_ceiling,
                                                     burst_allowance=burst_allowance, allow_custom_ceiling=allow_custom_ceiling)
        self.metrics = RequestMetrics()
        self.metrics.register_providers(get_currently_queued=lambda: self.queue.size, get_currently_executing=lambda: self.executor.currently_executing,
                                        get_high_water_mark=lambda: self.queue.high_water_mark, get_per_priority=self.queue.get_priority_counts, get_active_guilds=self.queue.get_active_guild_count)
        self._is_started: bool = False
        self._is_closed: bool = False
        self._accepting_requests: bool = False
        self._dispatch_tasks: List[asyncio.Task] = []
        self._in_flight_tasks: Set[asyncio.Task] = set()
        self._drain_event: asyncio.Event = asyncio.Event()

    @property
    def is_started(self) -> bool:
        return self._is_started

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    async def start(self) -> None:
        if self._is_closed:
            raise SchedulerClosedError('cannot start a closed scheduler.')
        if self._is_started:
            return
        self._is_started = True
        self._accepting_requests = True
        self._dispatch_tasks = [asyncio.create_task(
            self._dispatch_loop(), name='requestflow_dispatcher')]

    async def close(self, drain: bool = True, timeout: float = 5.0) -> None:
        if self._is_closed:
            return
        self._accepting_requests = False
        if drain and self._is_started and (self.queue.size > 0 or len(self._in_flight_tasks) > 0):
            start_drain = time.monotonic()
            while time.monotonic() - start_drain < timeout:
                if self.queue.size == 0 and len(self._in_flight_tasks) == 0:
                    break
                await asyncio.sleep(0.01)
        self._is_closed = True
        for t in self._dispatch_tasks:
            t.cancel()
        for t in self._dispatch_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._dispatch_tasks.clear()
        for t in list(self._in_flight_tasks):
            t.cancel()
        self._in_flight_tasks.clear()
        while not self.queue.is_empty():
            try:
                item = await self.queue.get()
                if item.future and (not item.future.done()):
                    item.future.set_exception(SchedulerClosedError(
                        'scheduler closed before item could be executed.'))
            except Exception:
                break

    async def submit(self, operation: Callable[..., Coroutine[Any, Any, T]], *args: Any, priority: Priority = Priority.NORMAL, guild_id: Optional[Union[int, str]] = None, route_key: Optional[str] = None, is_interaction: bool = False, dedup_key: Optional[str] = None, timeout: Optional[float] = None, enqueue_timeout: Optional[float] = None, **kwargs: Any) -> T:
        if not self._is_started:
            raise SchedulerNotStartedError(
                "scheduler has not been started. call 'await flow.start()'.")
        if not self._accepting_requests or self._is_closed:
            raise SchedulerClosedError(
                'scheduler is closed. cannot accept new submissions.')
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        item = RequestItem(sequence=0, operation=operation, args=args, kwargs=kwargs, priority=priority, effective_priority=priority, guild_id=guild_id,
                           route_key=route_key, is_interaction=is_interaction, dedup_key=dedup_key, timeout=timeout, future=future, enqueue_time=time.monotonic())
        self.metrics.record_submit()
        try:
            await self.queue.put(item, timeout=enqueue_timeout)
        except QueueFullError:
            self.metrics.record_reject()
            raise
        try:
            return await future
        except asyncio.CancelledError:
            item.cancelled = True
            self.metrics.record_cancel()
            self.queue.remove_item(item)
            raise

    async def _dispatch_loop(self) -> None:
        while self._is_started and (not self._is_closed):
            try:
                item = await self.queue.get(is_route_limited=self.rate_coordinator.route_coordinator.is_route_cooling_down)
                if item.cancelled or (item.future and item.future.cancelled()):
                    continue
                wait_time = await self.rate_coordinator.acquire(item.route_key, is_interaction=item.is_interaction)
                self.metrics.record_rate_limit_wait(wait_time)
                task = asyncio.create_task(self._execute_item_task(
                    item), name=f'requestflow_exec_{item.sequence}')
                self._in_flight_tasks.add(task)
                task.add_done_callback(self._in_flight_tasks.discard)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await asyncio.sleep(0.01)

    async def _execute_item_task(self, item: RequestItem) -> None:
        def on_complete(it: RequestItem, err: Optional[BaseException]) -> None:
            self.metrics.record_completion(
                queue_wait_seconds=it.queue_wait_time, exec_duration_seconds=it.execution_duration, error=err)
        try:
            await self.executor.execute(item, on_complete=on_complete)
        except asyncio.CancelledError:
            pass
