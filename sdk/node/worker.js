// The worker loop: claim, heartbeat, run, ack/fail.
//
// This module exists because the heartbeat is the part everyone gets wrong by
// hand. Forget it and any job that outlives the lease gets silently re-run by
// another worker while the original is still executing it. See PROTOCOL.md.

import { ApiError } from "./client.js";

// Signalled to a running handler when its lease has been lost. Handlers that
// accept an AbortSignal and check it cooperatively can stop early instead of
// doing pointless work after another worker already has the job.
export class LeaseLostError extends Error {
  constructor() {
    super("cogs: lease lost");
  }
}

export class Worker {
  #client;
  #workerId;
  #handlers = new Map();
  #stopping = false;
  #inFlight = new Set();

  batch = 1;
  waitMs = 5000;
  concurrency = 4;

  constructor(client, workerId) {
    this.#client = client;
    this.#workerId = workerId;
  }

  handle(queue, handler) {
    this.#handlers.set(queue, handler);
  }

  stop() {
    this.#stopping = true;
  }

  // Resolves once stop() (or SIGINT/SIGTERM) has been requested and all
  // in-flight jobs have finished. Jobs still running at a hard kill are not
  // lost: their leases expire and the reaper re-queues them -- the same path
  // a crash takes. Graceful drain here is a latency optimization, not a
  // correctness requirement.
  async run() {
    if (this.#handlers.size === 0) {
      throw new Error("no handlers registered; call worker.handle(queue, fn) first");
    }
    const onSignal = () => {
      console.error("cogs: received shutdown signal, draining in-flight jobs");
      this.stop();
    };
    process.once("SIGINT", onSignal);
    process.once("SIGTERM", onSignal);

    const queues = [...this.#handlers.keys()];
    const active = new Set();

    while (!this.#stopping) {
      while (active.size >= this.concurrency) {
        await Promise.race(active);
      }

      let res;
      try {
        res = await this.#client.claim(queues, this.batch, this.waitMs, this.#workerId);
      } catch (err) {
        console.error("cogs: claim failed:", err.message);
        await sleep(250);
        continue;
      }

      const jobs = res.jobs || [];
      if (jobs.length === 0) continue;

      const heartbeatMs = res.heartbeat_every_ms || 1000;
      for (const job of jobs) {
        const p = this.#process(job, heartbeatMs).finally(() => active.delete(p));
        active.add(p);
      }
    }

    await Promise.all(active);
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
  }

  async #process(job, heartbeatMs) {
    let leaseLost = false;
    const heartbeat = setInterval(async () => {
      try {
        await this.#client.renew(job.queue, job.id);
      } catch (err) {
        if (err instanceof ApiError && err.status === 409) {
          console.warn(`cogs: lease lost for job ${job.id}; abandoning`);
          leaseLost = true;
          clearInterval(heartbeat);
        } else {
          console.warn(`cogs: renew failed for job ${job.id}:`, err.message);
        }
      }
    }, heartbeatMs);

    let error = null;
    try {
      const handler = this.#handlers.get(job.queue);
      if (!handler) throw new Error(`no handler registered for queue "${job.queue}"`);
      await handler(job);
    } catch (err) {
      if (!(err instanceof LeaseLostError)) {
        error = err.message || String(err);
      }
    } finally {
      clearInterval(heartbeat);
    }

    if (leaseLost) {
      // Neither ack nor fail: another worker owns this job now, and
      // reporting against it would corrupt its attempt count.
      return;
    }

    try {
      if (error === null) {
        await this.#client.ack(job.queue, [job.id]);
      } else {
        await this.#client.fail(job.queue, job.id, error, job.attempts || 0);
      }
    } catch (err) {
      console.error(`cogs: failed to report result for job ${job.id}:`, err.message);
    }
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
