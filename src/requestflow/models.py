import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, Generic, Optional, TypeVar
from requestflow.priorities import Priority
T = TypeVar('t')


@dataclass
class RequestItem(Generic[T]):
    sequence: int
    operation: Callable[..., Coroutine[Any, Any, T]]
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    effective_priority: Priority = Priority.NORMAL
    guild_id: Optional[int] = None
    route_key: Optional[str] = None
    is_interaction: bool = False
    dedup_key: Optional[str] = None
    timeout: Optional[float] = None
    future: Optional[asyncio.Future] = None
    enqueue_time: float = 0.0

    def __post_init__(self) -> None:
        if self.future is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
            self.future = loop.create_future()
        if self.enqueue_time == 0.0:
            self.enqueue_time = time.monotonic()
    execution_start_time: Optional[float] = None
    completion_time: Optional[float] = None
    retries_left: int = 0
    cancelled: bool = False

    @property
    def queue_wait_time(self) -> float:
        if self.execution_start_time is None:
            return time.monotonic() - self.enqueue_time
        return self.execution_start_time - self.enqueue_time

    @property
    def execution_duration(self) -> float:
        if self.execution_start_time is None or self.completion_time is None:
            return 0.0
        return self.completion_time - self.execution_start_time


@dataclass(frozen=True)
class MetricsSnapshot:
    timestamp: float
    submitted: int
    completed: int
    failed: int
    cancelled: int
    rejected: int
    currently_queued: int
    currently_executing: int
    queue_high_water_mark: int
    p50_queue_wait_ms: float
    p95_queue_wait_ms: float
    p99_queue_wait_ms: float
    p50_exec_latency_ms: float
    p95_exec_latency_ms: float
    p99_exec_latency_ms: float
    throughput_window_rps: float
    rate_limit_429_count: int
    cooldown_count_global: int
    cooldown_count_route: int
    retries_attempted: int
    active_guilds: int
    per_priority_queued: Dict[str, int] = field(default_factory=dict)
