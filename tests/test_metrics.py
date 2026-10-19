import time
import pytest
from requestflow.metrics import RequestMetrics
from requestflow.models import MetricsSnapshot


def test_metrics_counters_and_snapshot():
    m = RequestMetrics(sample_buffer_size=10, throughput_window_seconds=2.0)
    m.record_submit()
    m.record_submit()
    m.record_reject()
    m.record_cancel()
    m.record_429(is_global=True)
    m.record_429(is_global=False)
    m.record_retry()
    m.record_completion(queue_wait_seconds=0.01,
                        exec_duration_seconds=0.05, error=None)
    m.record_completion(queue_wait_seconds=0.02,
                        exec_duration_seconds=0.06, error=ValueError('bad'))
    snap = m.snapshot()
    assert isinstance(snap, MetricsSnapshot)
    assert snap.submitted == 2
    assert snap.completed == 1
    assert snap.failed == 1
    assert snap.rejected == 1
    assert snap.cancelled == 1
    assert snap.rate_limit_429_count == 2
    assert snap.cooldown_count_global == 1
    assert snap.cooldown_count_route == 1
    assert snap.retries_attempted == 1
    assert snap.p50_queue_wait_ms > 0
    assert snap.p50_exec_latency_ms > 0


def test_bounded_memory_sample_buffer():
    m = RequestMetrics(sample_buffer_size=5)
    for i in range(100):
        m.record_completion(queue_wait_seconds=i * 0.001,
                            exec_duration_seconds=i * 0.002)
    assert len(m._queue_wait_samples) == 5
    assert len(m._exec_duration_samples) == 5


def test_throughput_calculation():
    m = RequestMetrics(throughput_window_seconds=1.0)
    for _ in range(10):
        m.record_completion(queue_wait_seconds=0.001,
                            exec_duration_seconds=0.002)
    rps = m.get_throughput_rps()
    assert rps == 10.0
