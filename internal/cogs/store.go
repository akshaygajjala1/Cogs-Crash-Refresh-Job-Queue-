package cogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrLeaseGone means the job is no longer in the leased set: the reaper
	// already took it back, or it was acked. A worker seeing this must stop
	// working on the job, because someone else may already have it.
	ErrLeaseGone = errors.New("lease no longer held")
	ErrNotFound  = errors.New("job not found")
)

type Store struct {
	rdb *redis.Client
	cfg Config
}

func NewStore(cfg Config) *Store {
	return &Store{
		rdb: redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			// The hot path is one round trip per request, so the pool needs to
			// be wide enough that concurrent handlers never queue for a conn.
			PoolSize:     256,
			MinIdleConns: 32,
		}),
		cfg: cfg,
	}
}

func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }
func (s *Store) Close() error                   { return s.rdb.Close() }
func (s *Store) Redis() *redis.Client           { return s.rdb }

func nowMS() int64 { return time.Now().UnixMilli() }

// ---------------------------------------------------------------------------
// Lua scripts
//
// Every operation that moves a job between states is one script, so it is one
// atomic step from Redis's point of view. This is the core correctness
// property of the whole system: there is no instant at which a job exists in
// neither the pending list nor the leased set. Splitting any of these into two
// client calls would open a window where a crash in between loses the job
// permanently -- and that window is small enough that you would never hit it
// in testing, only in production.
//
// Note these scripts compute key names ('job:'..id) internally. That is not
// Redis Cluster safe, and it is a deliberate choice: this is a single-instance
// design (see docs/DESIGN.md, "scaling limits").
// ---------------------------------------------------------------------------

// enqueueScript writes the job hash and pushes its ID, honouring an optional
// idempotency key. The SET NX is what makes a duplicate enqueue return the
// original job ID instead of creating a second job.
var enqueueScript = redis.NewScript(`
local jobKey, pendingKey, idemK = KEYS[1], KEYS[2], KEYS[3]
local id, payload, queue, created = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local idem, idemTTL = ARGV[5], tonumber(ARGV[6])

if idem ~= '' then
  local ok = redis.call('SET', idemK, id, 'NX', 'EX', idemTTL)
  if not ok then
    return {0, redis.call('GET', idemK)}
  end
end

redis.call('HSET', jobKey,
  'payload', payload, 'queue', queue, 'attempts', 0,
  'idempotency_key', idem, 'created_at', created)
redis.call('LPUSH', pendingKey, id)
return {1, id}
`)

// claimScript is the hot path and the single most important script here.
//
// It pops up to N IDs off the pending list and writes each one into the leased
// sorted set with its deadline, as ONE atomic operation. Two separate calls
// would be a real bug, not a theoretical one: pop the ID, then crash before
// the ZADD, and the job is now in no structure at all. The reaper cannot find
// it because the reaper only scans the leased set. The job is silently gone
// forever, and nothing in the system reports an error.
//
// Batching is also what makes 10k jobs/sec reachable. One HTTP request and one
// Redis round trip per *batch* rather than per *job* is a 10-50x difference in
// achievable throughput; the arithmetic is in docs/DESIGN.md.
var claimScript = redis.NewScript(`
local pendingKey, leasedKey = KEYS[1], KEYS[2]
local n, deadline = tonumber(ARGV[1]), tonumber(ARGV[2])
local out = {}

for i = 1, n do
  local id = redis.call('RPOP', pendingKey)
  if not id then break end
  redis.call('ZADD', leasedKey, deadline, id)
  local f = redis.call('HMGET', 'job:'..id, 'payload', 'attempts')
  out[#out+1] = id
  out[#out+1] = f[1] or ''
  out[#out+1] = f[2] or '0'
end
return out
`)

// renewScript extends a lease only if the ID is still in the leased set.
//
// The membership check rejects a worker that stalled past its deadline -- GC
// pause, a slow syscall, or a paused container -- after the reaper removed the
// lease. Membership alone is not ownership fencing: if the job has already
// been reclaimed and re-leased, an old worker can still match the ID. A lease
// generation/token is required to close that race; callers must treat a 409 as
// a signal to abandon work, but this ID-only protocol cannot provide fencing.
var renewScript = redis.NewScript(`
local leasedKey = KEYS[1]
local id, deadline = ARGV[1], tonumber(ARGV[2])
if redis.call('ZSCORE', leasedKey, id) == false then
  return 0
end
redis.call('ZADD', leasedKey, deadline, id)
return 1
`)

// ackScript finishes jobs. Removing the lease is what tells the reaper to stop
// caring; deleting the hash reclaims the memory. Batched, because at 10k
// jobs/sec one HTTP round trip per ack would dominate the request budget.
var ackScript = redis.NewScript(`
local leasedKey = KEYS[1]
local n = 0
for i = 1, #ARGV do
  -- Delete only after removing the ID from this queue's leased set. This
  -- protects unleased and wrong-queue jobs, but cannot distinguish generations
  -- after the same job is reclaimed by a different worker.
  if redis.call('ZREM', leasedKey, ARGV[i]) == 1 then
    n = n + 1
    redis.call('DEL', 'job:'..ARGV[i])
  end
end
return n
`)

// failScript moves a job to retry or to the dead-letter list.
// Returns -1 lease gone, 1 scheduled for retry, 2 dead-lettered.
var failScript = redis.NewScript(`
local leasedKey, retryKey, deadKey = KEYS[1], KEYS[2], KEYS[3]
local id, nextAt, maxRetries = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

if redis.call('ZREM', leasedKey, id) == 0 then
  return -1
end
local attempts = redis.call('HINCRBY', 'job:'..id, 'attempts', 1)
if attempts > maxRetries then
  redis.call('LPUSH', deadKey, id)
  return 2
end
redis.call('ZADD', retryKey, nextAt, id)
return 1
`)

// drainScript is the one loop body behind the reaper AND the retry drainer.
// Both are the same shape: a sorted set scored by a future timestamp, from
// which everything scored <= now moves back to pending. The only difference is
// whether a due entry counts as a failed attempt, which is the bumpAttempts
// flag. Writing this once means the crash-recovery path and the retry path
// share a single tested implementation.
//
// LIMIT bounds one pass. Redis is single-threaded, so an unbounded scan right
// after a mass worker death would block every other command and spike claim
// latency for every healthy worker in the fleet.
//
// Returns {requeuedIDs, deadIDs}.
var drainScript = redis.NewScript(`
local dueKey, pendingKey, deadKey = KEYS[1], KEYS[2], KEYS[3]
local now, limit = tonumber(ARGV[1]), tonumber(ARGV[2])
local bump, maxRetries = tonumber(ARGV[3]), tonumber(ARGV[4])

local due = redis.call('ZRANGEBYSCORE', dueKey, '-inf', now, 'LIMIT', 0, limit)
local requeued, dead = {}, {}

for _, id in ipairs(due) do
  if redis.call('ZREM', dueKey, id) == 1 then
    local attempts = 0
    if bump == 1 then
      attempts = redis.call('HINCRBY', 'job:'..id, 'attempts', 1)
    end
    if bump == 1 and attempts > maxRetries then
      redis.call('LPUSH', deadKey, id)
      dead[#dead+1] = id
    else
      redis.call('LPUSH', pendingKey, id)
      requeued[#requeued+1] = id
    end
  end
end
return {requeued, dead}
`)

// releaseLockScript makes lock release safe. Checking "is it mine?" and
// deleting must be atomic, or a leader whose lock already expired could delete
// the lock a different process has since acquired.
var releaseLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// renewLockScript extends the leader lock only while we still hold it.
var renewLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

// LoadScripts pushes every script into Redis at startup so the hot path uses
// EVALSHA (a 40-byte hash) rather than EVAL (the whole script body on every
// single call). go-redis falls back to EVAL automatically on NOSCRIPT, so this
// is a performance measure, not a correctness one.
func (s *Store) LoadScripts(ctx context.Context) error {
	for name, sc := range map[string]*redis.Script{
		"enqueue": enqueueScript, "claim": claimScript, "renew": renewScript,
		"ack": ackScript, "fail": failScript, "drain": drainScript,
		"releaseLock": releaseLockScript, "renewLock": renewLockScript,
		"schedule": scheduleDrainScript,
	} {
		if err := sc.Load(ctx, s.rdb).Err(); err != nil {
			return fmt.Errorf("load %s script: %w", name, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

// Enqueue adds a job. When an idempotency key is supplied and has been seen
// inside the window, it returns the original job's ID and created=false.
func (s *Store) Enqueue(ctx context.Context, queue string, payload json.RawMessage, idem string) (id string, created bool, err error) {
	newID, err := NewID()
	if err != nil {
		return "", false, err
	}
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}

	res, err := enqueueScript.Run(ctx, s.rdb,
		[]string{jobKey(newID), pendingKey(queue), idemKey(idem)},
		newID, string(payload), queue, nowMS(), idem,
		int(s.cfg.IdempotencyWindow.Seconds()),
	).Slice()
	if err != nil {
		return "", false, fmt.Errorf("enqueue: %w", err)
	}
	if len(res) != 2 {
		return "", false, fmt.Errorf("enqueue: unexpected reply %v", res)
	}
	createdFlag, _ := res[0].(int64)
	existingID, _ := res[1].(string)
	return existingID, createdFlag == 1, nil
}

// Claim takes up to batch jobs across the given queues, in the order given, so
// callers can express priority by listing queues most-important-first.
func (s *Store) Claim(ctx context.Context, queues []string, batch int, worker string) ([]Job, error) {
	if batch < 1 {
		batch = 1
	}
	if batch > s.cfg.MaxClaimBatch {
		batch = s.cfg.MaxClaimBatch
	}

	deadline := nowMS() + s.cfg.LeaseTTL.Milliseconds()
	jobs := make([]Job, 0, batch)

	for _, q := range queues {
		if len(jobs) >= batch {
			break
		}
		vals, err := claimScript.Run(ctx, s.rdb,
			[]string{pendingKey(q), leasedKey(q)},
			batch-len(jobs), deadline,
		).Slice()
		if err != nil {
			return nil, fmt.Errorf("claim %s: %w", q, err)
		}
		for i := 0; i+3 <= len(vals); i += 3 {
			id, _ := vals[i].(string)
			payload, _ := vals[i+1].(string)
			attemptsStr, _ := vals[i+2].(string)
			attempts, _ := strconv.Atoi(attemptsStr)
			jobs = append(jobs, Job{
				ID:            id,
				Queue:         q,
				Payload:       json.RawMessage(payload),
				Attempts:      attempts,
				LeaseDeadline: deadline,
			})
		}
	}
	return jobs, nil
}

// Renew extends a currently present lease. ErrLeaseGone means the ID is absent
// from the queue's leased set; a generation/token would be needed to tell an
// old worker apart after the same job is reclaimed and leased again.
func (s *Store) Renew(ctx context.Context, queue, id string) (int64, error) {
	deadline := nowMS() + s.cfg.LeaseTTL.Milliseconds()
	ok, err := renewScript.Run(ctx, s.rdb, []string{leasedKey(queue)}, id, deadline).Int()
	if err != nil {
		return 0, fmt.Errorf("renew: %w", err)
	}
	if ok == 0 {
		return 0, ErrLeaseGone
	}
	return deadline, nil
}

// Ack marks jobs done and removes them.
func (s *Store) Ack(ctx context.Context, queue string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	n, err := ackScript.Run(ctx, s.rdb, []string{leasedKey(queue)}, args...).Int()
	if err != nil {
		return 0, fmt.Errorf("ack: %w", err)
	}
	return n, nil
}

// Fail reports a failed attempt. The job is scheduled for a retry with
// exponential backoff, or dead-lettered once it has burned through MaxRetries.
func (s *Store) Fail(ctx context.Context, queue, id string, attempt int) (State, error) {
	nextAt := nowMS() + Backoff(s.cfg.BackoffBase, attempt).Milliseconds()
	r, err := failScript.Run(ctx, s.rdb,
		[]string{leasedKey(queue), retryKey(queue), deadKey(queue)},
		id, nextAt, s.cfg.MaxRetries,
	).Int()
	if err != nil {
		return "", fmt.Errorf("fail: %w", err)
	}
	switch r {
	case -1:
		return "", ErrLeaseGone
	case 2:
		return StateDead, nil
	default:
		return StateRetry, nil
	}
}

// drain runs one pass of the shared due-set loop. Used by both the reaper and
// the retry drainer; see drainScript.
func (s *Store) drain(ctx context.Context, dueKey, queue string, bumpAttempts bool) (requeued, dead []string, err error) {
	bump := 0
	if bumpAttempts {
		bump = 1
	}
	res, err := drainScript.Run(ctx, s.rdb,
		[]string{dueKey, pendingKey(queue), deadKey(queue)},
		nowMS(), s.cfg.DrainLimit, bump, s.cfg.MaxRetries,
	).Slice()
	if err != nil {
		return nil, nil, err
	}
	if len(res) != 2 {
		return nil, nil, fmt.Errorf("drain: unexpected reply %v", res)
	}
	return toStrings(res[0]), toStrings(res[1]), nil
}

// RunDrainForTest exposes the shared drain pass for integration tests, which
// need to force a reap/retry cycle deterministically rather than waiting on
// the background ticker's schedule. Not used by production code paths.
func (s *Store) RunDrainForTest(ctx context.Context, queue string, bumpAttempts bool) (requeued, dead []string, err error) {
	dueKey := leasedKey(queue)
	if !bumpAttempts {
		dueKey = retryKey(queue)
	}
	return s.drain(ctx, dueKey, queue, bumpAttempts)
}

// Job reads a single job. Used by the dashboard/stats paths, never on the hot
// path.
func (s *Store) Job(ctx context.Context, id string) (*Job, error) {
	m, err := s.rdb.HGetAll(ctx, jobKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, ErrNotFound
	}
	attempts, _ := strconv.Atoi(m["attempts"])
	created, _ := strconv.ParseInt(m["created_at"], 10, 64)
	return &Job{
		ID:             id,
		Queue:          m["queue"],
		Payload:        json.RawMessage(m["payload"]),
		Attempts:       attempts,
		IdempotencyKey: m["idempotency_key"],
		CreatedAt:      created,
	}, nil
}

type QueueStats struct {
	Queue   string `json:"queue"`
	Pending int64  `json:"pending"`
	Leased  int64  `json:"leased"`
	Retry   int64  `json:"retry"`
	Dead    int64  `json:"dead"`
}

// Stats reads all four depths in one round trip via a pipeline.
func (s *Store) Stats(ctx context.Context, queue string) (QueueStats, error) {
	pipe := s.rdb.Pipeline()
	pending := pipe.LLen(ctx, pendingKey(queue))
	leased := pipe.ZCard(ctx, leasedKey(queue))
	retry := pipe.ZCard(ctx, retryKey(queue))
	dead := pipe.LLen(ctx, deadKey(queue))
	if _, err := pipe.Exec(ctx); err != nil {
		return QueueStats{}, err
	}
	return QueueStats{
		Queue:   queue,
		Pending: pending.Val(),
		Leased:  leased.Val(),
		Retry:   retry.Val(),
		Dead:    dead.Val(),
	}, nil
}

// DeadJobs lists dead-lettered job IDs for inspection.
func (s *Store) DeadJobs(ctx context.Context, queue string, limit int64) ([]string, error) {
	return s.rdb.LRange(ctx, deadKey(queue), 0, limit-1).Result()
}

// KnownQueues discovers queues by scanning for pending lists. SCAN, never
// KEYS: KEYS blocks Redis for the length of the scan, which on a live server
// is an outage.
func (s *Store) KnownQueues(ctx context.Context) ([]string, error) {
	seen := map[string]bool{}
	for _, pattern := range []string{"queue:*:pending", "queue:*:leased", "queue:*:retry"} {
		var cursor uint64
		for {
			keys, next, err := s.rdb.Scan(ctx, cursor, pattern, 256).Result()
			if err != nil {
				return nil, err
			}
			for _, k := range keys {
				if q := queueFromKey(k); q != "" {
					seen[q] = true
				}
			}
			if next == 0 {
				break
			}
			cursor = next
		}
	}
	out := make([]string, 0, len(seen))
	for q := range seen {
		out = append(out, q)
	}
	return out, nil
}

// queueFromKey extracts "emails" from "queue:emails:pending".
func queueFromKey(k string) string {
	if len(k) < len("queue::") || k[:6] != "queue:" {
		return ""
	}
	rest := k[6:]
	for i := len(rest) - 1; i >= 0; i-- {
		if rest[i] == ':' {
			return rest[:i]
		}
	}
	return ""
}

func toStrings(v interface{}) []string {
	items, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
