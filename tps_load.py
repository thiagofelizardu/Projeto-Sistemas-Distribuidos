#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tps_load.py — fire POST requests for X seconds and plot TPS over time.

Usage (examples):
  python tps_load.py --url http://localhost:8080/payments --duration 120 --rps 200 --concurrency 200
  python tps_load.py --url http://localhost:8080/payments --duration 60 --rps 50 --concurrency 64 --outfile tps_run1

It will produce:
  - <outfile>.csv   (per-second metrics)
  - <outfile>.png   (TPS chart)
  - JSON body is embedded but can be overridden with --body-file path.json
"""
import argparse
import asyncio
import json
import math
import signal
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, Optional

import aiohttp
import matplotlib.pyplot as plt  # DO NOT set styles or colors per instructions

DEFAULT_BODY = {
    "merchantId": "MERCH-001",
    "customerId": "CUST-123456",
    "terminalId": "TERM-0001",
    "amount": 1050,
    "currency": "BRL",
    "method": "CREDIT",
    "entryMode": "CHIP",
    "cardHash": "3f6d9a0c1e2b4d5f6a7b8c9d0e1f2345"
}

@dataclass
class Counters:
    completed: int = 0
    ok: int = 0
    errors: int = 0
    bytes: int = 0

@dataclass
class PerSecond:
    second: int
    sent: int
    completed: int
    ok: int
    errors: int
    avg_latency_ms: float

class GracefulExit(SystemExit):
    pass

def _install_signal_handlers(loop):
    def _handler():
        raise GracefulExit()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _handler)
        except NotImplementedError:
            # Windows may not support add_signal_handler for SIGTERM
            signal.signal(s, lambda *_: (_ for _ in ()).throw(GracefulExit()))

async def worker(name: int,
                 session: aiohttp.ClientSession,
                 url: str,
                 body: dict,
                 timeout: aiohttp.ClientTimeout,
                 queue: "asyncio.Queue[float]",
                 per_sec: Dict[int, Counters],
                 lat_sum_ms: Dict[int, float],
                 ok_status_min: int,
                 ok_status_max: int):
    while True:
        try:
            scheduled_at = await queue.get()
        except asyncio.CancelledError:
            return
        now = time.perf_counter()
        # Busy wait / sleep until scheduled_at
        if scheduled_at > now:
            await asyncio.sleep(scheduled_at - now)
        start = time.perf_counter()
        sec_bucket = int(start - queue.start_time)  # type: ignore[attr-defined]
        try:
            async with session.post(url, json=body, timeout=timeout) as resp:
                content = await resp.read()
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                c = per_sec.setdefault(sec_bucket, Counters())
                c.completed += 1
                c.bytes += len(content)
                if ok_status_min <= resp.status <= ok_status_max:
                    c.ok += 1
                else:
                    c.errors += 1
                lat_sum_ms[sec_bucket] = lat_sum_ms.get(sec_bucket, 0.0) + elapsed_ms
        except Exception:
            c = per_sec.setdefault(sec_bucket, Counters())
            c.completed += 1
            c.errors += 1
        finally:
            queue.task_done()

async def scheduler(duration: float,
                    rps: float,
                    queue: "asyncio.Queue[float]"):
    """
    Put timestamps (perf_counter) onto the queue at the target RPS for 'duration' seconds.
    """
    start = time.perf_counter()
    queue.start_time = start  # type: ignore[attr-defined]
    period = 1.0 / rps if rps > 0 else 0.0
    n = 0
    next_ts = start
    end_time = start + duration
    while True:
        now = time.perf_counter()
        if now >= end_time:
            break
        if now >= next_ts:
            await queue.put(next_ts)
            n += 1
            next_ts = start + n * period
        else:
            await asyncio.sleep(min(0.001, next_ts - now))
    return n

def save_csv(outbase: str, series: Dict[int, PerSecond]):
    import csv
    path = f"{outbase}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["second", "sent", "completed", "ok", "errors", "avg_latency_ms"])
        for s in sorted(series.keys()):
            row = series[s]
            w.writerow([row.second, row.sent, row.completed, row.ok, row.errors, f"{row.avg_latency_ms:.2f}"])
    return path

def plot_tps(outbase: str, series: Dict[int, PerSecond]):
    xs = sorted(series.keys())
    ys = [series[s].completed for s in xs]  # TPS per second (completed responses)
    plt.figure()  # single plot, no styles or colors set
    plt.plot(xs, ys, marker="o")
    plt.title("TPS (completed responses per second)")
    plt.xlabel("Second")
    plt.ylabel("TPS")
    plt.grid(True)
    path = f"{outbase}.png"
    plt.savefig(path, bbox_inches="tight", dpi=120)
    plt.close()
    return path

async def main():
    parser = argparse.ArgumentParser(description="Simple TPS load generator (POST) with per-second TPS chart.")
    parser.add_argument("--url", default="http://localhost:8080/payments", help="Target URL")
    parser.add_argument("--duration", type=float, default=60.0, help="Test duration in seconds (e.g., 60, 120)")
    parser.add_argument("--rps", type=float, default=50.0, help="Target requests per second to schedule")
    parser.add_argument("--concurrency", type=int, default=100, help="Max in-flight requests (connection limit)")
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout seconds")
    parser.add_argument("--ok-min", type=int, default=200, help="Min HTTP code considered success")
    parser.add_argument("--ok-max", type=int, default=299, help="Max HTTP code considered success")
    parser.add_argument("--body-file", default=None, help="Path to JSON body file (overrides default body)")
    parser.add_argument("--outfile", default="tps_results", help="Output file base name (no extension)")
    args = parser.parse_args()

    if args.rps <= 0 or args.duration <= 0:
        print("rps and duration must be > 0", file=sys.stderr)
        sys.exit(2)

    body = DEFAULT_BODY
    if args.body_file:
        with open(args.body_file, "r", encoding="utf-8") as f:
            body = json.load(f)

    timeout = aiohttp.ClientTimeout(total=args.timeout)
    connector = aiohttp.TCPConnector(limit=args.concurrency)

    queue: "asyncio.Queue[float]" = asyncio.Queue(maxsize=max(1, int(args.rps * 2)))
    per_sec: Dict[int, Counters] = {}
    lat_sum_ms: Dict[int, float] = {}

    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop)

    total_scheduled = 0
    started_at = time.perf_counter()
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            sched_task = asyncio.create_task(scheduler(args.duration, args.rps, queue))
            workers = [
                asyncio.create_task(worker(i, session, args.url, body, timeout, queue, per_sec, lat_sum_ms, args.ok_min, args.ok_max))
                for i in range(min(args.concurrency, max(1, int(args.rps * 2))))
            ]
            try:
                total_scheduled = await sched_task
                # Once scheduling ends, wait for queue to drain
                await queue.join()
            finally:
                for w in workers:
                    w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
    except GracefulExit:
        print("Interrupted; finalizing...")

    duration_real = time.perf_counter() - started_at
    # Prepare per-second series including seconds with no traffic (0..ceil(duration))
    horizon = int(math.ceil(args.duration)) + 1
    series: Dict[int, PerSecond] = {}
    for s in range(horizon):
        c = per_sec.get(s, Counters())
        lat_total = lat_sum_ms.get(s, 0.0)
        avg_lat = (lat_total / c.completed) if c.completed > 0 else 0.0
        # Sent is approximated by scheduled timestamps falling into that second
        # Better: compute from queue schedule; as a close approximation we use completed as sent for the charting window
        series[s] = PerSecond(second=s, sent=c.completed, completed=c.completed, ok=c.ok, errors=c.errors, avg_latency_ms=avg_lat)

    total_completed = sum(v.completed for v in per_sec.values())
    total_ok = sum(v.ok for v in per_sec.values())
    total_errors = sum(v.errors for v in per_sec.values())
    total_bytes = sum(v.bytes for v in per_sec.values())
    avg_tps = total_completed / duration_real if duration_real > 0 else 0.0

    print(f"Scheduled: {total_scheduled}")
    print(f"Completed: {total_completed}  OK: {total_ok}  Errors: {total_errors}")
    print(f"Avg TPS: {avg_tps:.2f} over {duration_real:.2f}s  (~target {args.rps} rps for {args.duration:.2f}s)")
    print(f"Total bytes: {total_bytes}")
    # Save CSV and chart
    csv_path = save_csv(args.outfile, series)
    png_path = plot_tps(args.outfile, series)
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {png_path}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
