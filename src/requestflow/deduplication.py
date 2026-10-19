import asyncio
import collections
import time
from typing import Any, Callable, Coroutine, Dict, Generic, List, Optional, Tuple, TypeVar
T = TypeVar('t')


class InFlightDeduplicator:
    def __init__(self) -> None:
        self._waiters: Dict[str, List[asyncio.Future]] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def execute_or_join(self, dedup_key: str, runner: Callable[[], Coroutine[Any, Any, T]]) -> T:
        loop = asyncio.get_running_loop()
        caller_future: asyncio.Future = loop.create_future()
        async with self._lock:
            if dedup_key in self._waiters:
                self._waiters[dedup_key].append(caller_future)
                is_leader = False
            else:
                self._waiters[dedup_key] = [caller_future]
                is_leader = True
                task = asyncio.create_task(self._run_leader(dedup_key, runner))
                self._tasks[dedup_key] = task
        try:
            return await caller_future
        except asyncio.CancelledError:
            async with self._lock:
                if dedup_key in self._waiters:
                    waiters = self._waiters[dedup_key]
                    if caller_future in waiters:
                        waiters.remove(caller_future)
                    if len(waiters) == 0 and dedup_key in self._tasks:
                        task = self._tasks.pop(dedup_key, None)
                        if task and (not task.done()):
                            task.cancel()
                        self._waiters.pop(dedup_key, None)
            raise

    async def _run_leader(self, dedup_key: str, runner: Callable[[], Coroutine[Any, Any, T]]) -> None:
        result: Any = None
        error: Optional[BaseException] = None
        try:
            result = await runner()
        except asyncio.CancelledError as exc:
            error = exc
        except BaseException as exc:
            error = exc
        async with self._lock:
            waiters = self._waiters.pop(dedup_key, [])
            self._tasks.pop(dedup_key, None)
        for fut in waiters:
            if not fut.done():
                if error is not None:
                    if isinstance(error, asyncio.CancelledError):
                        fut.cancel()
                    else:
                        fut.set_exception(error)
                else:
                    fut.set_result(result)


class ResultCache(Generic[T]):
    def __init__(self, max_capacity: int = 1000, default_ttl_seconds: float = 60.0) -> None:
        if max_capacity <= 0:
            raise ValueError('max_capacity must be > 0')
        self._max_capacity: int = max_capacity
        self._default_ttl: float = default_ttl_seconds
        self._entries: collections.OrderedDict[str,
                                               Tuple[T, float]] = collections.OrderedDict()
        self._lock: asyncio.Lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[T]:
        async with self._lock:
            if key not in self._entries:
                return None
            val, expiry = self._entries[key]
            if time.monotonic() > expiry:
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
            return val

    async def set(self, key: str, value: T, ttl_seconds: Optional[float] = None) -> None:
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        expiry = time.monotonic() + ttl
        async with self._lock:
            if key in self._entries:
                del self._entries[key]
            elif len(self._entries) >= self._max_capacity:
                self._entries.popitem(last=False)
            self._entries[key] = (value, expiry)

    async def invalidate(self, key: str) -> bool:
        async with self._lock:
            if key in self._entries:
                del self._entries[key]
                return True
            return False

    async def clear(self) -> None:
        async with self._lock:
            self._entries.clear()
