import asyncio
import inspect
import time
from typing import Any, Callable, Coroutine, Optional
from requestflow.exceptions import OperationCancelledError, RequestTimeoutError
from requestflow.models import RequestItem
from requestflow.policies import RetryPolicy


class RequestExecutor:
    def __init__(self, max_concurrency: int = 16, default_timeout: Optional[float] = None, default_retry_policy: Optional[RetryPolicy] = None) -> None:
        if max_concurrency <= 0:
            raise ValueError(
                f'max_concurrency must be > 0, got {max_concurrency}')
        self._max_concurrency: int = max_concurrency
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrency)
        self._default_timeout: Optional[float] = default_timeout
        self._default_retry_policy: Optional[RetryPolicy] = default_retry_policy
        self._currently_executing: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

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
        try:
            while True:
                attempts += 1
                async with self._semaphore:
                    async with self._lock:
                        self._currently_executing += 1
                    try:
                        res = item.operation(*item.args, **item.kwargs)
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
                        if not (retry_policy and retry_policy.is_retryable(exc) and (attempts <= retry_policy.max_retries)):
                            item.completion_time = time.monotonic()
                            if item.future and (not item.future.done()):
                                item.future.set_exception(exc)
                            break
                    finally:
                        async with self._lock:
                            self._currently_executing -= 1

                if error is None or (item.future and item.future.done()):
                    break

                retry_after = getattr(error, 'retry_after', None)
                delay = retry_policy.compute_delay(attempts - 1, retry_after=retry_after)
                await asyncio.sleep(delay)
        finally:
            if on_complete:
                on_complete(item, error)
