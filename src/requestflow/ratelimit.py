import asyncio
import time
from typing import Dict, Optional, Tuple


class GlobalRateLimiter:
    def __init__(self, rate: float = 40.0, ceiling: float = 50.0, burst_allowance: float = 5.0, allow_custom_ceiling: bool = False, safety_buffer_seconds: float = 0.05) -> None:
        if rate <= 0:
            raise ValueError(f'global rate limit must be positive, got {rate}')
        if ceiling <= 0:
            raise ValueError(
                f'rate limit ceiling must be positive, got {ceiling}')
        if not allow_custom_ceiling and rate > ceiling:
            raise ValueError(
                f'configured rate ({rate}) exceeds standard discord limit ceiling ({ceiling}). discord standard limit is 50 req/s. if you have an officially approved higher limit, set allow_custom_ceiling=true.')
        self._rate: float = float(rate)
        self._ceiling: float = float(ceiling)
        self._safety_buffer: float = max(0.0, float(safety_buffer_seconds))
        if burst_allowance == 5.0 and self._rate <= 1.0:
            self._burst_tokens = 0.0
        else:
            self._burst_tokens = float(burst_allowance)
        self._allow_custom_ceiling: bool = allow_custom_ceiling
        self._emission_interval: float = 1.0 / self._rate
        self._tau: float = self._burst_tokens * self._emission_interval
        self._tat: float = 0.0
        self._lock: asyncio.Lock = asyncio.Lock()
        self._global_cooldown_until: float = 0.0
        self._global_cooldown_event: asyncio.Event = asyncio.Event()
        self._global_cooldown_event.set()
        self._total_acquisitions: int = 0
        self._total_wait_time: float = 0.0
        self._cooldown_429_count: int = 0

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def ceiling(self) -> float:
        return self._ceiling

    @property
    def burst_allowance(self) -> float:
        return self._burst_tokens

    @property
    def safety_buffer(self) -> float:
        return self._safety_buffer

    @property
    def is_cooling_down(self) -> bool:
        return time.monotonic() < self._global_cooldown_until

    @property
    def cooldown_remaining(self) -> float:
        now = time.monotonic()
        return max(0.0, self._global_cooldown_until - now)

    async def update_rate(self, new_rate: float) -> None:
        if new_rate <= 0:
            raise ValueError(
                f'global rate limit must be positive, got {new_rate}')
        if not self._allow_custom_ceiling and new_rate > self._ceiling:
            raise ValueError(
                f'new rate ({new_rate}) exceeds ceiling ({self._ceiling}).')
        async with self._lock:
            self._rate = float(new_rate)
            self._emission_interval = 1.0 / self._rate
            self._tau = self._burst_tokens * self._emission_interval
            self._tat = max(self._tat, time.monotonic())

    async def acquire(self, *, is_interaction: bool = False) -> float:
        if is_interaction:
            return 0.0
        wait_seconds: float = 0.0
        while True:
            while not self._global_cooldown_event.is_set():
                try:
                    await asyncio.wait_for(self._global_cooldown_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    if time.monotonic() >= self._global_cooldown_until:
                        self._global_cooldown_event.set()
                        break
            now = time.monotonic()
            if now < self._global_cooldown_until:
                sleep_duration = self._global_cooldown_until - now
                if sleep_duration > 0:
                    await asyncio.sleep(sleep_duration)
                    continue
            async with self._lock:
                now = time.monotonic()
                tat = max(now, self._tat)
                if tat - now <= self._tau + 1e-9:
                    delay = 0.0
                    self._tat = tat + self._emission_interval
                else:
                    delay = max(0.0, (tat - self._tau) - now)
                    self._tat = tat + self._emission_interval
            if delay > 0:
                try:
                    await asyncio.sleep(delay)
                    wait_seconds += delay
                except asyncio.CancelledError:
                    async with self._lock:
                        now_cancel = time.monotonic()
                        if self._tat > now_cancel:
                            self._tat = max(
                                now_cancel, self._tat - self._emission_interval)
                    raise
                if self.is_cooling_down or not self._global_cooldown_event.is_set():
                    continue
            self._total_acquisitions += 1
            self._total_wait_time += wait_seconds
            return wait_seconds

    def handle_global_429(self, retry_after: float) -> None:
        now = time.monotonic()
        effective_delay = max(0.05, retry_after) + self._safety_buffer
        cooldown_end = now + effective_delay
        if cooldown_end > self._global_cooldown_until:
            self._global_cooldown_until = cooldown_end
            self._cooldown_429_count += 1
            self._global_cooldown_event.clear()
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
            loop.call_later(effective_delay,
                            self._clear_cooldown, cooldown_end)

    def _clear_cooldown(self, target_end: float) -> None:
        if time.monotonic() + 0.001 >= self._global_cooldown_until or target_end >= self._global_cooldown_until:
            self._global_cooldown_event.set()


class RouteBucketCoordinator:
    def __init__(self, safety_buffer_seconds: float = 0.05) -> None:
        self._safety_buffer: float = max(0.0, float(safety_buffer_seconds))
        self._cooldowns: Dict[str, float] = {}
        self._remaining: Dict[str, int] = {}
        self._custom_checker: Optional[any] = None
        self._custom_delay_getter: Optional[any] = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._route_429_count: int = 0

    @property
    def safety_buffer(self) -> float:
        return self._safety_buffer

    def register_custom_checker(self, checker: any, delay_getter: Optional[any] = None) -> None:
        self._custom_checker = checker
        self._custom_delay_getter = delay_getter

    def is_route_cooling_down(self, route_key: Optional[str]) -> bool:
        if not route_key:
            return False
        expiry = self._cooldowns.get(route_key)
        if expiry is not None:
            if time.monotonic() < expiry:
                return True
            self._cooldowns.pop(route_key, None)
        if self._custom_checker:
            try:
                if self._custom_checker(route_key):
                    return True
            except Exception:
                pass
        return False

    def get_route_delay(self, route_key: Optional[str]) -> float:
        if not route_key:
            return 0.0
        expiry = self._cooldowns.get(route_key)
        if expiry is not None:
            remaining = expiry - time.monotonic()
            if remaining > 0:
                return remaining
            self._cooldowns.pop(route_key, None)
        if self._custom_delay_getter:
            try:
                delay = self._custom_delay_getter(route_key)
                if delay > 0:
                    return delay
            except Exception:
                pass
        return 0.0

    def update_route_cooldown(self, route_key: str, retry_after: float) -> None:
        if not route_key or retry_after <= 0:
            return
        cooldown_until = time.monotonic() + retry_after + self._safety_buffer
        existing = self._cooldowns.get(route_key, 0.0)
        if cooldown_until > existing:
            self._cooldowns[route_key] = cooldown_until
            self._route_429_count += 1

    def update_from_headers(self, route_key: str, headers: Dict[str, str]) -> None:
        if not route_key:
            return
        lower_headers = {k.lower(): v for k, v in headers.items()}
        remaining = lower_headers.get('x-ratelimit-remaining')
        reset_after = lower_headers.get('x-ratelimit-reset-after')
        if remaining is not None:
            try:
                rem_val = int(remaining)
                self._remaining[route_key] = rem_val
                if rem_val <= 0 and reset_after is not None:
                    try:
                        reset_val = float(reset_after)
                        self.update_route_cooldown(route_key, reset_val)
                    except ValueError:
                        pass
                elif rem_val > 0:
                    self._cooldowns.pop(route_key, None)
            except ValueError:
                pass


class RateLimitCoordinator:
    def __init__(self, global_rate_limit: float = 40.0, rate_limit_ceiling: float = 50.0, burst_allowance: float = 5.0, allow_custom_ceiling: bool = False, safety_buffer_seconds: float = 0.05) -> None:
        self.global_limiter = GlobalRateLimiter(
            rate=global_rate_limit, ceiling=rate_limit_ceiling, burst_allowance=burst_allowance, allow_custom_ceiling=allow_custom_ceiling, safety_buffer_seconds=safety_buffer_seconds)
        self.route_coordinator = RouteBucketCoordinator(safety_buffer_seconds=safety_buffer_seconds)

    async def acquire_route(self, route_key: Optional[str], *, is_interaction: bool = False) -> float:
        total_delay = 0.0
        if not is_interaction and route_key:
            while self.route_coordinator.is_route_cooling_down(route_key):
                delay = self.route_coordinator.get_route_delay(route_key)
                if delay > 0:
                    await asyncio.sleep(delay)
                    total_delay += delay
                else:
                    break
        return total_delay

    async def acquire_global(self, *, is_interaction: bool = False) -> float:
        return await self.global_limiter.acquire(is_interaction=is_interaction)

    async def acquire(self, route_key: Optional[str] = None, *, is_interaction: bool = False) -> float:
        route_delay = await self.acquire_route(route_key, is_interaction=is_interaction)
        global_delay = await self.acquire_global(is_interaction=is_interaction)
        return route_delay + global_delay

    def handle_429(self, retry_after: float, *, is_global: bool = False, route_key: Optional[str] = None) -> None:
        if is_global:
            self.global_limiter.handle_global_429(retry_after)
        elif route_key:
            self.route_coordinator.update_route_cooldown(
                route_key, retry_after)
