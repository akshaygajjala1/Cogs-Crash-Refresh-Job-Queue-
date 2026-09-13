"""The worker loop: claim, heartbeat, run, ack/fail.

This module exists because the heartbeat is the part everyone gets wrong by
hand. Forget it and any job that outlives the lease gets silently re-run by
another worker while the original is still executing it. See PROTOCOL.md.
"""

from __future__ import annotations

import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from .client import ApiError, Client

log = logging.getLogger("cogs.worker")


class LeaseLostError(Exception):
    """Raised inside a handler (via cancellation, not directly) when the
    lease expired mid-run. Handlers don't need to catch this themselves;
    Worker checks a cancellation flag it can pass to cooperative handlers."""


class JobContext:
    """Passed to handlers so long-running handlers can poll for lease loss and
    stop early instead of doing pointless work after another worker has
    already picked the job back up."""

    def __init__(self):
        self._lost = threading.Event()

    def lease_lost(self) -> bool:
        return self._lost.is_set()

    def raise_if_lost(self):
        if self._lost.is_set():
            raise LeaseLostError()


Handler = Callable[[dict, JobContext], None]


class Worker:
    def __init__(self, client: Client, worker_id: str):
        self.client = client
        self.worker_id = worker_id
        self.handlers: dict[str, Handler] = {}
        self.batch = 1
        self.wait_ms = 5000
        self.concurrency = 4
        self._stop = threading.Event()

    def handle(self, queue: str, handler: Handler):
        self.handlers[queue] = handler

    def run(self):
        """Blocks until stop() is called or SIGINT/SIGTERM is received. Jobs
        still in flight at shutdown are not lost: their leases simply expire
        and the reaper re-queues them, the same path a hard crash takes."""
        if not self.handlers:
            raise RuntimeError("no handlers registered; call worker.handle(queue, fn) first")

        def _signal_handler(signum, frame):
            log.info("received signal %s, draining in-flight jobs", signum)
            self._stop.set()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        queues = list(self.handlers.keys())
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            futures = []
            while not self._stop.is_set():
                try:
                    res = self.client.claim(queues, self.batch, self.wait_ms, self.worker_id)
                except ApiError as e:
                    log.error("claim failed: %s", e)
                    self._stop.wait(0.25)
                    continue

                jobs = res.get("jobs") or []
                if not jobs:
                    continue

                heartbeat_ms = res.get("heartbeat_every_ms") or 1000
                for job in jobs:
                    futures.append(pool.submit(self._process, job, heartbeat_ms / 1000))

            for f in futures:
                f.result()

    def stop(self):
        self._stop.set()

    def _process(self, job: dict, heartbeat_seconds: float):
        ctx = JobContext()
        stop_heartbeat = threading.Event()

        def heartbeat_loop():
            while not stop_heartbeat.wait(heartbeat_seconds):
                try:
                    self.client.renew(job["queue"], job["id"])
                except ApiError as e:
                    if e.status == 409:
                        log.warning("lease lost for job %s; abandoning", job["id"])
                        ctx._lost.set()
                        return
                    log.warning("renew failed for job %s: %s", job["id"], e)

        hb = threading.Thread(target=heartbeat_loop, daemon=True)
        hb.start()

        handler = self.handlers.get(job["queue"])
        error: Optional[str] = None
        try:
            if handler is None:
                raise RuntimeError(f"no handler registered for queue {job['queue']!r}")
            handler(job, ctx)
        except LeaseLostError:
            pass
        except Exception as e:  # noqa: BLE001 — any handler exception is a job failure, by design
            error = str(e)
        finally:
            stop_heartbeat.set()
            hb.join(timeout=heartbeat_seconds + 1)

        if ctx.lease_lost():
            # Neither ack nor fail: another worker owns this job now, and
            # reporting against it would corrupt its attempt count.
            return

        try:
            if error is None:
                self.client.ack(job["queue"], [job["id"]])
            else:
                self.client.fail(job["queue"], job["id"], error, job.get("attempts", 0))
        except ApiError as e:
            log.error("failed to report result for job %s: %s", job["id"], e)
