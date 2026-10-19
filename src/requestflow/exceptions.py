from typing import Optional


class RequestFlowError(Exception):
    pass


class QueueFullError(RequestFlowError):
    pass


class SchedulerClosedError(RequestFlowError):
    pass


class SchedulerNotStartedError(RequestFlowError):
    pass


class RequestTimeoutError(RequestFlowError):
    def __init__(self, message: str, *, timeout_seconds: float) -> None:
        super().__init__(message)
        self.timeout_seconds: float = timeout_seconds


class RateLimitExceededError(RequestFlowError):
    def __init__(self, message: str, *, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after: Optional[float] = retry_after


class IntegrationError(RequestFlowError):
    pass


class UnsupportedDiscordVersionError(IntegrationError):
    def __init__(self, message: str, *, detected_version: Optional[str] = None) -> None:
        super().__init__(message)
        self.detected_version: Optional[str] = detected_version


class OperationCancelledError(RequestFlowError):
    pass
