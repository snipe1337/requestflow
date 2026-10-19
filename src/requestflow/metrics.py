import collections
import time
from typing import Callable, Deque, Dict, List, Optional
from requestflow.models import MetricsSnapshot


class RequestMetrics:
    def __init__(self, sample_buffer_size: int = 1000, throughput_window_seconds: float = 10.0) -> None:
        self._buffer_size: int = sample_buffer_size
        self._window_seconds: float = throughput_window_seconds
        self.submitted: int = 0
        self.completed: int = 0
        self.failed: int = 0
        self.cancelled: int = 0
        self.rejected: int = 0
        self.rate_limit_429_count: int = 0
        self.cooldown_count_global: int = 0
        self.cooldown_count_route: int = 0
        self.retries_attempted: int = 0
        self.total_rate_limit_wait_time: float = 0.0
        self._queue_wait_samples: Deque[float] = collections.deque(
            maxlen=sample_buffer_size)
        self._exec_duration_samples: Deque[float] = collections.deque(
            maxlen=sample_buffer_size)
        self._completion_timestamps: Deque[float] = collections.deque()
        self._get_currently_queued: Callable[[], int] = lambda: 0
        self._get_currently_executing: Callable[[], int] = lambda: 0
        self._get_high_water_mark: Callable[[], int] = lambda: 0
        self._get_per_priority: Callable[[], Dict[str, int]] = lambda: {}
        self._get_active_guilds: Callable[[], int] = lambda: 0

    def register_providers(self, get_currently_queued: Callable[[], int], get_currently_executing: Callable[[], int], get_high_water_mark: Callable[[], int], get_per_priority: Callable[[], Dict[str, int]], get_active_guilds: Callable[[], int]) -> None:
        self._get_currently_queued = get_currently_queued
        self._get_currently_executing = get_currently_executing
        self._get_high_water_mark = get_high_water_mark
        self._get_per_priority = get_per_priority
        self._get_active_guilds = get_active_guilds

    def record_submit(self) -> None:
        self.submitted += 1

    def record_reject(self) -> None:
        self.rejected += 1

    def record_cancel(self) -> None:
        self.cancelled += 1

    def record_retry(self) -> None:
        self.retries_attempted += 1

    def record_rate_limit_wait(self, duration_seconds: float) -> None:
        if duration_seconds > 0:
            self.total_rate_limit_wait_time += duration_seconds

    def record_429(self, *, is_global: bool = False) -> None:
        self.rate_limit_429_count += 1
        if is_global:
            self.cooldown_count_global += 1
        else:
            self.cooldown_count_route += 1

    def record_completion(self, queue_wait_seconds: float, exec_duration_seconds: float, error: Optional[BaseException] = None) -> None:
        now = time.monotonic()
        if error is not None:
            self.failed += 1
        else:
            self.completed += 1
        self._queue_wait_samples.append(queue_wait_seconds * 1000.0)
        self._exec_duration_samples.append(exec_duration_seconds * 1000.0)
        self._completion_timestamps.append(now)
        self._prune_throughput_window(now)

    def _prune_throughput_window(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._completion_timestamps and self._completion_timestamps[0] < cutoff:
            self._completion_timestamps.popleft()

    def get_throughput_rps(self) -> float:
        now = time.monotonic()
        self._prune_throughput_window(now)
        count = len(self._completion_timestamps)
        if count == 0:
            return 0.0
        return count / self._window_seconds

    @staticmethod
    def _calculate_percentile(samples: List[float], percentile: float) -> float:
        if not samples:
            return 0.0
        idx = int(len(samples) * (percentile / 100.0))
        idx = min(idx, len(samples) - 1)
        return samples[idx]

    def snapshot(self) -> MetricsSnapshot:
        now = time.monotonic()
        sorted_wait = sorted(self._queue_wait_samples)
        sorted_exec = sorted(self._exec_duration_samples)
        p50_wait = self._calculate_percentile(sorted_wait, 50.0)
        p95_wait = self._calculate_percentile(sorted_wait, 95.0)
        p99_wait = self._calculate_percentile(sorted_wait, 99.0)
        p50_exec = self._calculate_percentile(sorted_exec, 50.0)
        p95_exec = self._calculate_percentile(sorted_exec, 95.0)
        p99_exec = self._calculate_percentile(sorted_exec, 99.0)
        return MetricsSnapshot(timestamp=now, submitted=self.submitted, completed=self.completed, failed=self.failed, cancelled=self.cancelled, rejected=self.rejected, currently_queued=self._get_currently_queued(), currently_executing=self._get_currently_executing(), queue_high_water_mark=self._get_high_water_mark(), p50_queue_wait_ms=round(p50_wait, 2), p95_queue_wait_ms=round(p95_wait, 2), p99_queue_wait_ms=round(p99_wait, 2), p50_exec_latency_ms=round(p50_exec, 2), p95_exec_latency_ms=round(p95_exec, 2), p99_exec_latency_ms=round(p99_exec, 2), throughput_window_rps=round(self.get_throughput_rps(), 2), rate_limit_429_count=self.rate_limit_429_count, cooldown_count_global=self.cooldown_count_global, cooldown_count_route=self.cooldown_count_route, retries_attempted=self.retries_attempted, active_guilds=self._get_active_guilds(), per_priority_queued=self._get_per_priority())
