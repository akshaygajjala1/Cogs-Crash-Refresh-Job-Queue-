# Cogs wire protocol

This is the contract every SDK implements. It's written so a worker can be
built in any language from this document alone, without reading the Go SDK
source. See [docs/API.md](docs/API.md) for the full endpoint reference,
including status codes not relevant to workers (schedules, stats, dead-letter).

## Auth

Every request except `/healthz`, `/readyz`, and `/metrics` carries:

```
Authorization: Bearer <token>
```

If the server was started with no `AUTH_TOKEN`, auth is disabled (local dev
only — never do this in production).

## The worker loop

This is the entire client-side contract. Every SDK implements exactly this
loop; the only per-language difference is what a timer/ticker looks like.

```
loop:
    response = POST /v1/jobs/claim
               {"queues": [...], "batch": N, "wait_ms": W, "worker_id": ID}

    if response.jobs is empty:
        continue   # the call already long-polled up to wait_ms; don't sleep again

    for job in response.jobs (process concurrently, up to your concurrency limit):
        start a timer that fires every (response.heartbeat_every_ms) ms:
            on fire: POST /v1/jobs/{id}/renew {"queue": job.queue}
                     if response status == 409:
                         stop the timer
                         cancel/abort the handler for this job
                         do NOT ack or fail — you no longer own this job

        run your handler(job.payload)

        stop the timer

        if the lease was lost (409 seen above):
            do nothing further for this job
        elif handler succeeded:
            POST /v1/jobs/ack {"queue": job.queue, "ids": [job.id]}
        else:
            POST /v1/jobs/{id}/fail
                {"queue": job.queue, "error": "<message>", "attempt": job.attempts}

on SIGTERM/SIGINT:
    stop claiming new batches
    let in-flight jobs finish (their heartbeats keep them alive)
    exit
```

### Why the heartbeat exists

The server hands out a lease with a deadline (`response.lease_ttl_ms` from now).
If nothing renews it and the deadline passes, the reaper assumes the worker
died and re-queues the job for someone else. A job takes longer than
`lease_ttl_ms` to run in almost every real handler, so the heartbeat is not
optional — a worker without one drops every job whose lease expires mid-run,
and it will be silently re-run by another worker.

Heartbeat every `heartbeat_every_ms` (server-provided, currently
`lease_ttl_ms / 3`). This is a timing recommendation, not a guarantee: request
latency, process pauses, and Redis outages can still let a lease expire. A
worker must handle 409 and abandon the job.

### Why a 409 on renew means "stop," not "retry"

If renew returns 409, the reaper already decided this worker was dead and gave
the job to somebody else. Continuing to work on it and then acking would mean
two workers believe they finished the same job — a correctness bug, not a
transient error. Cancel the handler's context/cancellation token and drop the
job; do not ack, do not fail, do not retry the renew. The current membership
check does not provide a fencing generation: a stale caller that ignores this
rule may still mutate by job ID, so cancellation and idempotent handlers remain
the worker's responsibility.

### Why claim is a POST

`/v1/jobs/claim` mutates state (it removes jobs from pending and creates
leases), so it is not safe to retry blindly or let an intermediary cache or
prefetch it, which is what GET semantics would invite.

### Batching

`batch` controls how many jobs one claim call returns (max `MAX_CLAIM_BATCH`,
default 64). Ack is batched the same way: collect the IDs you completed and
send them in one `POST /v1/jobs/ack` call. Larger batches reduce HTTP overhead;
the repository makes no hardware-independent throughput claim.

### Idempotency

Pass `idempotency_key` on enqueue if your caller might retry the enqueue call
itself (e.g. after a network timeout). A repeated key within the configured
window returns the original job's ID with HTTP 200 instead of creating a
second job; a fresh key returns 201.

## Minimal example (raw HTTP, pseudocode)

```
POST /v1/jobs
{"queue": "emails", "payload": {"to": "a@b.com"}}
-> 201 {"id": "abc123", "queue": "emails", "created": true}

POST /v1/jobs/claim
{"queues": ["emails"], "batch": 8, "wait_ms": 5000, "worker_id": "w1"}
-> 200 {"jobs": [{"id": "abc123", "queue": "emails", "payload": {...}, "attempts": 0}],
        "lease_ttl_ms": 3000, "heartbeat_every_ms": 1000}

# ... every 1000ms while working ...
POST /v1/jobs/abc123/renew
{"queue": "emails"}
-> 200 {"id": "abc123", "lease_deadline_ms": 1234567890123}

# on success
POST /v1/jobs/ack
{"queue": "emails", "ids": ["abc123"]}
-> 200 {"acked": 1}

# on failure instead
POST /v1/jobs/abc123/fail
{"queue": "emails", "error": "smtp timeout", "attempt": 0}
-> 200 {"id": "abc123", "state": "retry"}
```
