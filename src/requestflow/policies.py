import random
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence, Tuple, Type


class OverloadPolicy(str, Enum):
    REJECT = 'reject'
    WAIT = 'wait'
    DROP_OLDEST = 'drop_oldest'


class PacingStrategy(str, Enum):
    GCRA = 'gcra'
    LEAKY_BUCKET = 'leaky_bucket'


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 0
    base_delay: float = 0.5
    max_delay: float = 10.0
    jitter: bool = True
    retryable_exceptions: Tuple[Type[BaseException], ...] = ()

    def is_retryable(self, exception: BaseException) -> bool:
        if not self.retryable_exceptions:
            return False
        return isinstance(exception, self.retryable_exceptions)

    def compute_delay(self, attempt: int, retry_after: Optional[float] = None) -> float:
        if retry_after is not None and retry_after > 0:
            return retry_after
        delay = self.base_delay * 2 ** attempt
        delay = min(delay, self.max_delay)
        if self.jitter:
            factor = 0.5 + random.random()
            delay = delay * factor
        return min(delay, self.max_delay)
