import asyncio
import statistics
import time
from typing import Any, List, Tuple
from benchmarks.workloads import SimulatedDiscordRequest, generate_mixed_discord_workload
from requestflow.flow import RequestFlow
from requestflow.priorities import Priority


async def run_baseline_mock(workload: List[SimulatedDiscordRequest]) -> float:
    start = time.monotonic()

    async def mock_call(req: SimulatedDiscordRequest):
        await asyncio.sleep(req.mock_network_delay)
    await asyncio.gather(*(mock_call(r) for r in workload))
    return time.monotonic() - start


async def run_requestflow_mock(workload: List[SimulatedDiscordRequest]) -> Tuple[float, Any]:
    async with RequestFlow(global_rate_limit=100.0, rate_limit_ceiling=100.0, allow_custom_ceiling=True, max_concurrency=20, max_pending=len(workload) * 2) as flow:
        start = time.monotonic()

        async def mock_call(req: SimulatedDiscordRequest):
            await asyncio.sleep(req.mock_network_delay)
        tasks = [flow.submit(mock_call, req, priority=req.priority, guild_id=req.guild_id,
                             route_key=req.route_key, is_interaction=req.is_interaction) for req in workload]
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start
        stats = flow.stats
        return (elapsed, stats)


async def run_scheduler_microbenchmark(count: int = 5000) -> float:
    async with RequestFlow(global_rate_limit=5000.0, rate_limit_ceiling=5000.0, allow_custom_ceiling=True, max_concurrency=100, max_pending=count * 2) as flow:
        start = time.monotonic()

        async def noop():
            return 1
        tasks = [flow.submit(noop, priority=Priority.NORMAL)
                 for _ in range(count)]
        await asyncio.gather(*tasks)
        return time.monotonic() - start


async def main():
    print('=' * 65)
    print(' requestflow performance & scheduling benchmark suite')
    print('=' * 65)
    print('\n[phase 1] pure scheduler overhead (5,000 no-op async tasks):')
    t_sched = await run_scheduler_microbenchmark(5000)
    sched_rps = 5000 / t_sched
    overhead_per_req_us = t_sched / 5000 * 1000000
    print(f'  - total duration:     {t_sched:.4f} s')
    print(f'  - scheduling throughput: {sched_rps:,.1f} operations/sec')
    print(f'  - scheduling overhead:   {overhead_per_req_us:.2f} us/request')
    workload = generate_mixed_discord_workload(count=300)
    print(
        f'\n[phase 2] realistic multi-guild mocked discord workload (300 requests):')
    print(f'  - active guilds: 10, mixed priorities (critical, high, normal, low)')
    print('  running unmanaged baseline (asyncio.gather without concurrency/rate limits)...')
    baseline_time = await run_baseline_mock(workload)
    print(
        f'  - unmanaged duration: {baseline_time:.3f} s (peak unconstrained concurrency: 300)')
    print('\n  running requestflow (paced global limit: 100 req/s, max concurrency: 20)...')
    rf_time, rf_stats = await run_requestflow_mock(workload)
    print(f'  - requestflow duration:   {rf_time:.3f} s')
    print(
        f'  - effective throughput:   {rf_stats.completed / rf_time:.1f} req/s')
    print(f'  - queue wait time (p50):  {rf_stats.p50_queue_wait_ms:.2f} ms')
    print(f'  - queue wait time (p95):  {rf_stats.p95_queue_wait_ms:.2f} ms')
    print(f'  - queue wait time (p99):  {rf_stats.p99_queue_wait_ms:.2f} ms')
    print(f'  - exec latency (p50):     {rf_stats.p50_exec_latency_ms:.2f} ms')
    print(f'  - exec latency (p95):     {rf_stats.p95_exec_latency_ms:.2f} ms')
    print(
        f'  - queue high water mark:  {rf_stats.queue_high_water_mark} items')
    print('\n[analysis & transparency disclosure]:')
    print('  notice: queueing and rate-limit pacing deliberately control traffic dispatch.')
    print('  they do not reduce raw discord network rtt. instead, requestflow prevents 429')
    print('  burst collisions, enforces cross-guild fairness, and prioritizes interaction')
    print('  deadlines within bounded memory.')
    print('=' * 65)
if __name__ == '__main__':
    asyncio.run(main())
