package cogs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// State is where a job sits in its lifecycle. It is never stored as a field:
// a job's state is implied by which Redis structure holds its ID. Storing it
// separately would mean two writes to keep in sync, and a crash between them
// is exactly the bug leases exist to prevent.
type State string

const (
	StatePending State = "pending"
	StateLeased  State = "leased"
	StateRetry   State = "retry"
	StateDead    State = "dead"
)

// Job is the unit of work. Payload stays as json.RawMessage end to end: the
// server never parses it, so users can put anything in it and we pay no
// decode cost per job on the hot path.
type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Attempts       int             `json:"attempts"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      int64           `json:"created_at_ms"`

	// LeaseDeadline is only set on jobs handed back from a claim.
	LeaseDeadline int64 `json:"lease_deadline_ms,omitempty"`
}

// NewID returns a 128-bit random ID as hex.
//
// crypto/rand, not math/rand: job IDs are handed to clients and are the only
// thing standing between a caller and someone else's job on ack/renew/fail.
// math/rand is seeded deterministically and its output is predictable from a
// few samples, so IDs must be unguessable, not merely unique.
func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// Redis key layout. Lists and sorted sets hold IDs only; the payload lives in
// the job hash. That keeps the hot-path structures small and sorted-set score
// comparisons cheap regardless of how large a user's payload is.
//
//	job:{id}            HASH  payload, queue, attempts, idempotency_key, created_at
//	queue:{q}:pending   LIST  job IDs, LPUSH at head and RPOP from tail (FIFO)
//	queue:{q}:leased    ZSET  member=id, score=lease deadline   <- the reaper's index
//	queue:{q}:retry     ZSET  member=id, score=next attempt time
//	queue:{q}:dead      LIST  job IDs
//	idem:{key}          STRING short-TTL dedup marker -> job ID
func jobKey(id string) string        { return "job:" + id }
func pendingKey(queue string) string { return "queue:" + queue + ":pending" }
func leasedKey(queue string) string  { return "queue:" + queue + ":leased" }
func retryKey(queue string) string   { return "queue:" + queue + ":retry" }
func deadKey(queue string) string    { return "queue:" + queue + ":dead" }
func idemKey(key string) string      { return "idem:" + key }

// leaderKey guards the background loops. Only one server process runs the
// reaper, retry drainer, and scheduler at a time.
const leaderKey = "cogs:leader"

// ValidQueueName keeps queue names usable as key segments. A name containing
// ':' could be crafted to collide with another queue's keys.
func ValidQueueName(q string) bool {
	if q == "" || len(q) > 128 {
		return false
	}
	return !strings.ContainsAny(q, ": \t\r\n{}")
}
