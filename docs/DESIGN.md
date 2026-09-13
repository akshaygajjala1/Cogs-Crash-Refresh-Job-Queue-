# Cogs design

Cogs chooses a small, inspectable failure model: a stateless Go HTTP server, one Redis instance, and workers outside the server. Redis holds job state so a server restart does not discard an in memory queue.

## Key configuration

| Setting | Default | Purpose |
|---|---:|---|
| `REDIS_ADDR` | `localhost:6379` | Redis connection |
| `DEFAULT_LEASE_SECONDS` | `3` | Worker lease duration |
| `REAPER_INTERVAL_MS` | `500` | Expired lease scan interval |
| `MAX_CLAIM_BATCH` | `64` | Maximum jobs per claim |
| `CLAIM_WAIT_MS` | `5000` | Maximum empty-queue long poll |
| `MAX_RETRIES` | `5` | Retries after the initial attempt |
| `DRAIN_LIMIT` | `500` | Due entries handled per background pass |

## State model

```text
pending -> leased -> acked
                  └-> retry -> pending
                  └-> dead
```

`job:{id}` stores payload, queue, attempts, and timestamps. `queue:{q}:pending` is a FIFO list of IDs; `queue:{q}:leased` and `queue:{q}:retry` are sorted sets scored by deadlines; `queue:{q}:dead` is a list. Schedules use a `schedules` sorted set and `schedule:{id}` hashes. Optional enqueue idempotency keys use short lived Redis string markers.

## Lease protocol

Claim atomically removes IDs from pending and writes their lease deadlines. A worker renews its lease about every third of the lease TTL and acknowledges or fails it when the handler returns. The reaper drains expired lease entries back to pending and counts lease expiry toward the retry limit. A failed handler is delayed with exponential backoff and jitter; poison jobs eventually enter the dead letter list.

Atomic scripts matter because a crash between a pop and a lease write would otherwise leave a job in neither structure. Renew checks membership and returns 409 after the lease is gone. SDKs stop processing on that signal. Acknowledgement and failure are also atomic with lease removal.

## Scheduling and replicas

The scheduler validates standard five field cron expressions and creates at most one job for each due schedule, skipping missed occurrences after downtime. Reaper, retry, and scheduler all use the shared `drainDue` ticker shape. A Redis `SET NX PX` lock with renewal elects one leader for those loops; replicas still handle HTTP requests.

The lock avoids duplicate scheduling during ordinary operation, but it is not consensus. Redis remains the single source of truth. Lua scripts are atomic on one Redis instance and assume the keys for each operation are colocated; the current key construction is not Redis Cluster safe.

## Delivery and failure behavior

Delivery is designed to be at least once within the retry and durability limits below. If a handler completes and the worker dies before ack, the job can run again. Enqueue idempotency protects retries of the enqueue request; it does not deduplicate handler side effects. Users should make handlers idempotent or maintain their own deduplication record.

Redis durability is an operational dependency. The included Compose setup enables AOF with `appendfsync everysec`, which limits expected restart loss to the durability behavior of that Redis configuration. Replication and failover can still lose writes that were not replicated. The service cannot guarantee jobs survive arbitrary Redis data loss.

There is currently no fencing token or lease generation in job mutations. A worker that ignores renew 409, or a worker whose cancellation is delayed, may still call ack or fail using the job ID after another worker has reclaimed it. The SDKs follow the protocol and abandon on 409, but Cogs cannot undo an external side effect already performed by a stale handler. Fencing generations are the next reliability milestone.

| Failure | Behavior | Boundary |
|---|---|---|
| Worker crash | Lease expires and the reaper requeues it | At least once; duplicate side effects are possible |
| Server crash | Redis state remains; in flight requests fail | Depends on Redis retaining writes |
| Redis restart/failover | Requests fail until Redis returns | Recent or unreplicated writes may be lost |
| Stale worker | SDK abandons after renew 409 | No fencing token prevents a misbehaving stale caller from mutating by ID |
| Leader loss | Another replica can acquire the Redis lock | Lock is not consensus under partitions |

## Observability and evidence

`/healthz` checks only that the process is alive; `/readyz` checks Redis and reports leadership; `/metrics` exposes request duration, batch size, requeues, rejected renewals, and leadership. These metrics support measurement but do not constitute benchmark results.

Run integration and chaos checks against a disposable real Redis:

```powershell
docker run --name cogs-test-redis --rm -d -p 127.0.0.1:16379:6379 redis:7-alpine
$env:REDIS_ADDR = '127.0.0.1:16379'
$env:COGS_TEST_REDIS = '1'
go test -p 1 ./...
docker stop cogs-test-redis
```

The tests flush the selected database. The chaos test kills worker processes and asserts no missing jobs after recovery. `loadtest/` exercises enqueue, batch claim, and ack while reporting client side observations; server side claim latency is read from `/metrics`. Any public performance statement must include a reproducible command and named hardware. This repository currently publishes no throughput or p99 result.

## Scaling boundary

Batch claim and ack reduce HTTP overhead and are the main reason the design can be efficient, but no capacity number is promised here. One Redis instance, one keyspace, and single instance Lua scripts are the deliberate first boundary. Sharding queues or adopting Redis Cluster would require hash tagged keys and corresponding changes to leader, scheduler, reaper, and retry coordination.

## Demo boundary

`demo/index.html` is an interactive, deterministic explanation of lease expiry and requeue. It is a static simulation suitable for an iframe embed. It does not measure the server, connect to Redis, or replace an operational dashboard.
