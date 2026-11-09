import asyncio
import contextvars
import inspect
import time
from typing import Any, Callable, Coroutine, Optional, Tuple
from requestflow.exceptions import OperationCancelledError, RequestTimeoutError
from requestflow.models import RequestItem
from requestflow.policies import RetryPolicy

IN_REQUESTFLOW_EXECUTION: contextvars.ContextVar[bool] = contextvars.ContextVar('IN_REQUESTFLOW_EXECUTION', default=False)


class RequestExecutor:
    def __init__(self, max_concurrency: int = 16, default_timeout: Optional[float] = None, default_retry_policy: Optional[RetryPolicy] = None, on_429: Optional[Callable[[float, bool, Optional[str]], None]] = None, max_429_retries: int = 5, safety_buffer: float = 0.05) -> None:
        if max_concurrency <= 0:
            raise ValueError(
                f'max_concurrency must be > 0, got {max_concurrency}')
        self._max_concurrency: int = max_concurrency
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrency)
        self._default_timeout: Optional[float] = default_timeout
        self._default_retry_policy: Optional[RetryPolicy] = default_retry_policy
        self._on_429: Optional[Callable[[float, bool, Optional[str]], None]] = on_429
        self._max_429_retries: int = max_429_retries
        self._safety_buffer: float = max(0.0, float(safety_buffer))
        self._currently_executing: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    @staticmethod
    def _extract_429_info(exc: BaseException) -> Tuple[bool, float, bool]:
        if exc is None:
            return False, 0.0, False
        is_429 = False
        retry_after = 0.0
        is_global = False

        if exc.__class__.__name__ == 'RateLimited' or hasattr(exc, 'retry_after'):
            is_429 = True
            retry_after = float(getattr(exc, 'retry_after', 0.0) or 0.0)
            is_global = bool(getattr(exc, 'is_global', False))

        if getattr(exc, 'status', None) == 429:
            is_429 = True
            if retry_after <= 0.0:
                retry_after = 1.0
            resp = getattr(exc, 'response', None)
            headers = getattr(resp, 'headers', None)
            if isinstance(headers, dict) or hasattr(headers, 'get'):
                ra = headers.get('Retry-After')
                if ra is not None:
                    try:
                        retry_after = float(ra)
                    except (ValueError, TypeError):
                        pass
                glob = headers.get('X-RateLimit-Global') or headers.get('x-ratelimit-global')
                if str(glob).lower() == 'true':
                    is_global = True
            data = getattr(exc, 'data', None)
            if isinstance(data, dict):
                if 'retry_after' in data:
                    try:
                        retry_after = float(data['retry_after'])
                    except (ValueError, TypeError):
                        pass
                if 'global' in data:
                    is_global = bool(data['global'])

        return is_429, retry_after, is_global

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @property
    def currently_executing(self) -> int:
        return self._currently_executing

    async def execute(self, item: RequestItem, on_complete: Optional[Callable[[RequestItem, Optional[BaseException]], None]] = None) -> None:
        if item.cancelled or (item.future and item.future.cancelled()):
            if on_complete:
                on_complete(item, OperationCancelledError(
                    'operation cancelled before execution'))
            return
        item.execution_start_time = time.monotonic()
        error: Optional[BaseException] = None
        retry_policy = self._default_retry_policy
        timeout = item.timeout if item.timeout is not None else self._default_timeout
        attempts = 0
        token = IN_REQUESTFLOW_EXECUTION.set(True)
        try:
            while True:
                attempts += 1
                async with self._semaphore:
                    async with self._lock:
                        self._currently_executing += 1
                    try:
                        if inspect.isawaitable(item.operation):
                            res = item.operation
                        elif callable(item.operation):
                            res = item.operation(*item.args, **item.kwargs)
                        else:
                            res = item.operation
                        if inspect.isawaitable(res):
                            if timeout is not None and timeout > 0:
                                result = await asyncio.wait_for(res, timeout=timeout)
                            else:
                                result = await res
                        else:
                            result = res
                        item.completion_time = time.monotonic()
                        if item.future and (not item.future.done()):
                            item.future.set_result(result)
                        error = None
                        break
                    except asyncio.TimeoutError as exc:
                        timeout_err = RequestTimeoutError(
                            f'operation timed out after {timeout:.2f} seconds', timeout_seconds=float(timeout or 0.0))
                        error = timeout_err
                        item.completion_time = time.monotonic()
                        if item.future and (not item.future.done()):
                            item.future.set_exception(timeout_err)
                        break
                    except asyncio.CancelledError as exc:
                        item.cancelled = True
                        error = exc
                        item.completion_time = time.monotonic()
                        if item.future and (not item.future.done()):
                            item.future.cancel()
                        raise
                    except BaseException as exc:
                        error = exc
                        is_429, retry_after, is_global = self._extract_429_info(exc)
                        if is_429:
                            if self._on_429:
                                self._on_429(retry_after, is_global, item.route_key)
                            if attempts <= self._max_429_retries:
                                pass
                            else:
                                item.completion_time = time.monotonic()
                                if item.future and (not item.future.done()):
                                    item.future.set_exception(exc)
                                break
                        elif not (retry_policy and retry_policy.is_retryable(exc) and (attempts <= retry_policy.max_retries)):
                            item.completion_time = time.monotonic()
                            if item.future and (not item.future.done()):
                                item.future.set_exception(exc)
                            break
                    finally:
                        async with self._lock:
                            self._currently_executing -= 1

                if error is None or (item.future and item.future.done()):
                    break

                is_429, retry_after, is_global = self._extract_429_info(error)
                if is_429:
                    delay = max(0.05, retry_after) + self._safety_buffer
                elif retry_policy:
                    ra = getattr(error, 'retry_after', None)
                    delay = retry_policy.compute_delay(attempts - 1, retry_after=ra)
                else:
                    delay = 0.1
                await asyncio.sleep(delay)
        finally:
            IN_REQUESTFLOW_EXECUTION.reset(token)
            if on_complete:
                on_complete(item, error)
