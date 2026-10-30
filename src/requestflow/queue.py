import asyncio
import collections
import time
from typing import Callable, Deque, Dict, List, Optional, Set, Union
from requestflow.exceptions import QueueFullError
from requestflow.models import RequestItem
from requestflow.policies import OverloadPolicy
from requestflow.priorities import Priority


class RequestQueue:
    def __init__(self, max_pending: int = 5000, overload_policy: OverloadPolicy = OverloadPolicy.REJECT, aging_threshold_seconds: float = 2.0, guild_quantum: int = 3) -> None:
        if max_pending <= 0:
            raise ValueError(f'max_pending must be > 0, got {max_pending}')
        self._max_pending: int = max_pending
        self._overload_policy: OverloadPolicy = overload_policy
        self._aging_threshold: float = aging_threshold_seconds
        self._guild_quantum: int = guild_quantum
        self._size: int = 0
        self._sequence_counter: int = 0
        self._high_water_mark: int = 0
        self._queues: Dict[Priority, Dict[Optional[Union[int, str]], Deque[RequestItem]]] = {
            p: collections.defaultdict(collections.deque) for p in Priority}
        self._active_guild_keys: Dict[Priority, List[Optional[Union[int, str]]]] = {
            p: [] for p in Priority}
        self._guild_indices: Dict[Priority, int] = {p: 0 for p in Priority}
        self._guild_deficits: Dict[Optional[Union[int, str]],
                                   int] = collections.defaultdict(int)
        self._not_empty: asyncio.Event = asyncio.Event()
        self._not_full: asyncio.Event = asyncio.Event()
        self._not_full.set()
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return self._size

    @property
    def max_pending(self) -> int:
        return self._max_pending

    @property
    def high_water_mark(self) -> int:
        return self._high_water_mark

    @property
    def overload_policy(self) -> OverloadPolicy:
        return self._overload_policy

    def is_empty(self) -> bool:
        return self._size == 0

    def is_full(self) -> bool:
        return self._size >= self._max_pending

    def get_priority_counts(self) -> Dict[str, int]:
        counts = {}
        for p in Priority:
            total = sum((len(q) for q in self._queues[p].values()))
            counts[p.name.lower()] = total
        return counts

    def get_active_guild_count(self) -> int:
        seen: Set[Optional[Union[int, str]]] = set()
        for p in Priority:
            for guild_id, q in self._queues[p].items():
                if len(q) > 0:
                    seen.add(guild_id)
        return len(seen)

    async def put(self, item: RequestItem, timeout: Optional[float] = None) -> None:
        async with self._lock:
            if self._size >= self._max_pending:
                if self._overload_policy == OverloadPolicy.REJECT:
                    raise QueueFullError(
                        f'queue is full ({self._size}/{self._max_pending}). workload rejected.')
                elif self._overload_policy == OverloadPolicy.DROP_OLDEST:
                    self._drop_oldest_locked()
                elif self._overload_policy == OverloadPolicy.WAIT:
                    self._not_full.clear()
        if self._overload_policy == OverloadPolicy.WAIT and self._size >= self._max_pending:
            start_wait = time.monotonic()
            while True:
                remaining_timeout = None
                if timeout is not None:
                    elapsed = time.monotonic() - start_wait
                    remaining_timeout = timeout - elapsed
                    if remaining_timeout <= 0:
                        raise QueueFullError(
                            'timed out waiting for queue capacity.')
                try:
                    await asyncio.wait_for(self._not_full.wait(), timeout=remaining_timeout)
                except asyncio.TimeoutError:
                    raise QueueFullError(
                        'timed out waiting for queue capacity.')
                async with self._lock:
                    if self._size < self._max_pending:
                        break
        async with self._lock:
            self._enqueue_item_locked(item)

    def _enqueue_item_locked(self, item: RequestItem) -> None:
        self._sequence_counter += 1
        item.sequence = self._sequence_counter
        p = item.priority
        g = item.guild_id
        guild_dict = self._queues[p]
        if g not in guild_dict or len(guild_dict[g]) == 0:
            if g not in self._active_guild_keys[p]:
                self._active_guild_keys[p].append(g)
        guild_dict[g].append(item)
        self._size += 1
        if self._size > self._high_water_mark:
            self._high_water_mark = self._size
        self._not_empty.set()
        if self._size >= self._max_pending:
            self._not_full.clear()

    def _drop_oldest_locked(self) -> None:
        for p in reversed(Priority):
            guild_dict = self._queues[p]
            oldest_item: Optional[RequestItem] = None
            target_guild: Optional[Union[int, str]] = None
            for g, q in guild_dict.items():
                if q:
                    head = q[0]
                    if oldest_item is None or head.sequence < oldest_item.sequence:
                        oldest_item = head
                        target_guild = g
            if oldest_item is not None and target_guild is not None:
                dropped = guild_dict[target_guild].popleft()
                if len(guild_dict[target_guild]) == 0:
                    if target_guild in self._active_guild_keys[p]:
                        self._active_guild_keys[p].remove(target_guild)
                self._size -= 1
                dropped.cancelled = True
                if dropped.future and (not dropped.future.done()):
                    dropped.future.set_exception(QueueFullError(
                        'dropped due to queue overload under drop_oldest policy'))
                return

    def remove_item(self, item: RequestItem) -> bool:
        p = item.effective_priority
        g = item.guild_id
        guild_dict = self._queues[p]
        q = guild_dict.get(g)
        if q:
            try:
                q.remove(item)
                self._size -= 1
                if len(q) == 0 and g in self._active_guild_keys[p]:
                    self._active_guild_keys[p].remove(g)
                if self._size < self._max_pending:
                    self._not_full.set()
                if self._size == 0:
                    self._not_empty.clear()
                return True
            except ValueError:
                pass
        return False

    async def get(self, is_route_limited: Optional[Callable[[Optional[str]], bool]] = None) -> RequestItem:
        while True:
            await self._not_empty.wait()
            async with self._lock:
                item = self._extract_next_item_locked(is_route_limited)
                if item is not None:
                    self._size -= 1
                    if self._size < self._max_pending:
                        self._not_full.set()
                    if self._size == 0:
                        self._not_empty.clear()
                    return item
                self._not_empty.clear()
            await asyncio.sleep(0.01)

    def _extract_next_item_locked(self, is_route_limited: Optional[Callable[[Optional[str]], bool]] = None) -> Optional[RequestItem]:
        now = time.monotonic()
        self._apply_aging_locked(now)
        for p in Priority:
            active_guilds = self._active_guild_keys[p]
            if not active_guilds:
                continue
            num_guilds = len(active_guilds)
            start_index = self._guild_indices[p] % num_guilds
            for i in range(num_guilds):
                idx = (start_index + i) % num_guilds
                g = active_guilds[idx]
                q = self._queues[p][g]
                if not q:
                    continue
                head = q[0]
                if head.cancelled or (head.future and head.future.cancelled()):
                    q.popleft()
                    self._size -= 1
                    continue
                if is_route_limited and is_route_limited(head.route_key):
                    continue
                if self._guild_deficits[g] <= 0:
                    self._guild_deficits[g] += self._guild_quantum
                self._guild_deficits[g] -= 1
                item = q.popleft()
                if len(q) == 0:
                    self._guild_deficits[g] = 0
                    active_guilds.remove(g)
                    if active_guilds:
                        self._guild_indices[p] = idx % len(active_guilds)
                    else:
                        self._guild_indices[p] = 0
                elif self._guild_deficits[g] <= 0:
                    self._guild_deficits[g] = 0
                    self._guild_indices[p] = (idx + 1) % num_guilds
                else:
                    self._guild_indices[p] = idx
                return item
        return None

    def _apply_aging_locked(self, now: float) -> None:
        if self._aging_threshold <= 0:
            return
        for p in [Priority.LOW, Priority.NORMAL, Priority.HIGH]:
            guild_dict = self._queues[p]
            higher_priority = Priority(p.value - 1)
            for g, q in list(guild_dict.items()):
                stale_items = []
                for item in list(q):
                    if now - item.enqueue_time >= self._aging_threshold:
                        stale_items.append(item)
                for item in stale_items:
                    q.remove(item)
                    item.effective_priority = higher_priority
                    self._queues[higher_priority][g].append(item)
                    if g not in self._active_guild_keys[higher_priority]:
                        self._active_guild_keys[higher_priority].append(g)
                if len(q) == 0 and g in self._active_guild_keys[p]:
                    self._active_guild_keys[p].remove(g)
