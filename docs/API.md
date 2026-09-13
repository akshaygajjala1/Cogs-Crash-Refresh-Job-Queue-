# Cogs API reference

Base URL: `http://<host>:8080`. All request/response bodies are JSON. See
[PROTOCOL.md](../PROTOCOL.md) for the worker loop these endpoints compose
into.

## Auth

Every endpoint except `/healthz`, `/readyz`, and `/metrics` requires:

```
Authorization: Bearer <AUTH_TOKEN>
```

If the server starts with no `AUTH_TOKEN` set, auth is disabled — local
development only. The comparison is constant-time
(`crypto/subtle.ConstantTimeCompare`) so the check cannot be turned into a
byte-at-a-time timing oracle.

## Error envelope

Every non-2xx response uses the same shape, so clients branch on a
machine-readable `code` instead of parsing prose:

```json
{ "error": { "code": "invalid_queue", "message": "queue is required" } }
```

| HTTP status | When |
|---|---|
| 400 | Malformed JSON |
| 401 | Missing/invalid bearer token |
| 404 | No such job |
| 409 | Lease already gone (see `renew`/`ack`/`fail` below) |
| 422 | Well-formed JSON, invalid semantics (bad queue name, missing required field, bad cron expression) |
| 500 | Internal error (logged server-side; body never contains the underlying error) |

422 vs 400: 400 means "I couldn't parse this as JSON at all"; 422 means "I
parsed it fine, but these values are not acceptable" (e.g. a queue name
containing `:`). Splitting them lets clients distinguish a client-side bug
from a validation failure.

## Jobs

### `POST /v1/jobs` — enqueue

```json
{ "queue": "emails", "payload": {"to": "a@b.com"}, "idempotency_key": "" }
```

- `queue` — required. Non-empty, ≤128 chars, no `:` or whitespace (queue names
  become Redis key segments; `:` could be crafted to collide with another
  queue's keys).
- `payload` — arbitrary JSON. The server never parses it; it's passed through
  as `json.RawMessage` end to end.
- `idempotency_key` — optional. If a job with this key was enqueued within the
  configured window (`IDEMPOTENCY_WINDOW_SECONDS`, default 3600s), the
  **original** job's ID is returned instead of creating a new job.

**201 Created** (new job):
```json
{ "id": "abc123", "queue": "emails", "created": true }
```

**200 OK** (idempotency key hit — this call created nothing):
```json
{ "id": "abc123", "queue": "emails", "created": false }
```

This distinction matters for exactly the reason idempotency keys exist: a
caller that retried an enqueue after a timeout can tell whether its first
attempt actually landed.

### `POST /v1/jobs/claim` — claim a batch of jobs

```json
{ "queues": ["emails", "sms"], "batch": 32, "wait_ms": 5000, "worker_id": "w1" }
```

- `queues` — required, checked in order (list your highest-priority queue
  first — a worker fills its batch from queue[0] before touching queue[1]).
- `batch` — jobs to claim in one call, capped at `MAX_CLAIM_BATCH` (default
  64). Larger batches reduce HTTP overhead. Capacity depends on the deployment
  and is not promised by this documentation.
- `wait_ms` — long-poll: if nothing is available, the server holds the
  connection (polling internally every 25ms) up to this many milliseconds
  before returning an empty result. Capped at `CLAIM_WAIT_MS` (default 5000).

This is a **POST despite reading like a GET**, because it mutates state: it
removes jobs from `pending` and creates leases in `leased`. A GET would
invite proxies and clients to retry, cache, or prefetch it — and every such
retry would silently claim additional jobs meant for someone else.

**200 OK**, always — including when nothing was available (`jobs: []`). There
is deliberately no 204 here: workers poll this endpoint constantly, and one
response shape means the client never branches on status code before parsing.

```json
{
  "jobs": [
    { "id": "abc123", "queue": "emails", "payload": {"to": "a@b.com"}, "attempts": 0 }
  ],
  "lease_ttl_ms": 3000,
  "heartbeat_every_ms": 1000
}
```

`heartbeat_every_ms` is `lease_ttl_ms / HEARTBEAT_DIVISOR` (default divisor 3).
It is a recommendation from the server, not a guarantee that a lease survives
latency, process pauses, or a Redis outage.

### `POST /v1/jobs/{id}/renew` — extend a lease

```json
{ "queue": "emails" }
```

**200 OK**: `{ "id": "abc123", "lease_deadline_ms": 1234567890123 }`

**409 Conflict** — `lease_gone`:
```json
{ "error": { "code": "lease_gone", "message": "lease expired and the job was re-queued; stop working on it" } }
```

This 409 is the single most important status code in the API. If it fires,
the reaper already decided this worker was dead (its lease expired) and
handed the job to somebody else. A worker that ignores this and keeps working
— then acks anyway — creates a real correctness bug: two workers each
believing they exclusively completed the same job. The 409 turns a silent
failure into an explicit signal a well-behaved worker acts on by abandoning
its work immediately. See `PROTOCOL.md` for the required client behavior.

### `POST /v1/jobs/ack` — complete jobs (batch)

```json
{ "queue": "emails", "ids": ["abc123", "def456"] }
```

**200 OK**: `{ "acked": 2 }` — the count may be less than `len(ids)` if some
leases had already expired; those jobs were re-queued and this worker no
longer owns them.

Batched because one request can complete several jobs and reduce HTTP
overhead. Capacity depends on deployment and workload; no throughput target is
implied by this API reference.

### `POST /v1/jobs/{id}/ack` — complete one job

```json
{ "queue": "emails" }
```

**200 OK**: `{ "acked": 1 }`
**409 Conflict** (`lease_gone`) if the lease was already gone — the same
signal as `renew`, telling the caller its result may be a duplicate.

### `POST /v1/jobs/{id}/fail` — report a failed attempt

```json
{ "queue": "emails", "error": "smtp timeout", "attempt": 0 }
```

**200 OK**: `{ "id": "abc123", "state": "retry" }` or `{ ..., "state": "dead" }`
once `MAX_RETRIES` is exceeded.
**409 Conflict** (`lease_gone`) — same meaning as above.

### `GET /v1/jobs/{id}` — inspect a job

**200 OK**: the job record. **404** if it doesn't exist (already acked jobs are
deleted, so this returns 404 for them too — Cogs is a queue, not an audit
log).

## Schedules

### `POST /v1/schedules` — register a recurring job

```json
{ "queue": "cleanup", "cron": "0 3 * * *", "payload": {} }
```

Standard 5-field cron (`minute hour dom month dow`) or a descriptor
(`@hourly`, `@daily`, ...). **Seconds fields are not supported** — accepting a
6-field form would let a schedule fire once per second, which is a job
firehose wearing a cron expression's clothes.

**201 Created**: the `Schedule` record including `next_run_ms`.
**422** if the cron expression doesn't parse.

If the server was down when a schedule was due, it fires exactly once on
restart/next tick and jumps to the next scheduled time — it does not replay
every missed occurrence. Firing 400 catch-up jobs after a seven-hour outage is
almost never what a nightly cleanup job wants.

### `GET /v1/schedules` — list

**200 OK**: `{ "schedules": [...] }`

### `DELETE /v1/schedules/{id}` — remove

**204 No Content**.

## Queues

### `GET /v1/queues/{name}/stats`

**200 OK**:
```json
{ "queue": "emails", "pending": 12, "leased": 3, "retry": 1, "dead": 0 }
```

### `GET /v1/dead?queue={name}&limit={n}`

**200 OK**: `{ "queue": "emails", "ids": ["...", "..."] }` — up to `limit`
(default 100, max 1000) dead-lettered job IDs, most recent first.

## Operational endpoints

### `GET /healthz` — liveness

Always returns `200 {"status":"ok"}` if the process is running. **Never
touches Redis.** If it did, a Redis blip would make every replica look dead
simultaneously, and an orchestrator restarting the entire fleet is the worst
possible response to the one incident where restarts help least.

### `GET /readyz` — readiness

Pings Redis. **503** if unreachable — a replica that can't reach Redis should
be pulled from the load balancer's rotation, not killed. Also reports
`leader: bool`, whether this replica currently runs the background loops.

### `GET /metrics` — Prometheus

Key series:

| Metric | Meaning |
|---|---|
| `cogs_request_duration_seconds{endpoint,status}` | Server-side handler latency, excluding client/network time. Use it with a recorded load command and named hardware when measuring latency. |
| `cogs_claim_batch_size` | Jobs returned per claim call — throughput is `claims/sec * batch size`, so a latency number means little without this alongside it. |
| `cogs_jobs_requeued_total{queue}` | Reaper recoveries. Non-zero rate means workers are dying or overrunning their leases — the most useful single fleet-health signal. |
| `cogs_lease_renew_rejected_total` | Renewals refused because the lease membership was already gone; this is not a fencing generation. |
| `cogs_is_leader` | 1 on the replica currently running the background loops. |

## Design notes referenced above

See [DESIGN.md](DESIGN.md) for: why leases instead of delete-on-claim, the
atomic Lua claim script, why exactly-once is impossible, the failure-mode
table, and the batching arithmetic behind the throughput numbers.
