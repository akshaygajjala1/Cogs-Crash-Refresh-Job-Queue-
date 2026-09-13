// Integration tests exercise Lua transitions against a disposable real Redis.
package cogs_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/akshaygajjala1/cogs/internal/cogs"
	"github.com/redis/go-redis/v9"
)

func testConfig() cogs.Config {
	cfg := cogs.LoadConfig()
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		cfg.RedisAddr = v
	}
	cfg.LeaseTTL = 500 * time.Millisecond
	cfg.ReaperInterval = 100 * time.Millisecond
	cfg.MaxRetries = 2
	cfg.BackoffBase = 50 * time.Millisecond
	return cfg
}

// newTestStore returns a store against a real Redis, flushed, with scripts
// loaded. These tests are opt-in because FlushDB destroys every key in the
// selected database; once opted in, an unavailable Redis is a test failure.
func newTestStore(t *testing.T) (*cogs.Store, cogs.Config) {
	t.Helper()
	if os.Getenv("COGS_TEST_REDIS") != "1" {
		t.Skip("Redis integration tests require COGS_TEST_REDIS=1 (they flush the selected database)")
	}
	cfg := testConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("COGS_TEST_REDIS=1 but Redis unavailable at %s: %v", cfg.RedisAddr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}

	store := cogs.NewStore(cfg)
	t.Cleanup(func() { store.Close() })
	if err := store.LoadScripts(ctx); err != nil {
		t.Fatalf("load scripts: %v", err)
	}
	return store, cfg
}

func TestEnqueueClaimAck(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	id, created, err := store.Enqueue(ctx, "q1", json.RawMessage(`{"n":1}`), "")
	if err != nil || !created || id == "" {
		t.Fatalf("enqueue: id=%q created=%v err=%v", id, created, err)
	}

	jobs, err := store.Claim(ctx, []string{"q1"}, 10, "w1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("expected exactly the enqueued job, got %+v", jobs)
	}

	// A second claim must find nothing: the job left pending atomically when
	// the first claim leased it.
	again, err := store.Claim(ctx, []string{"q1"}, 10, "w2")
	if err != nil || len(again) != 0 {
		t.Fatalf("expected empty second claim, got %+v err=%v", again, err)
	}

	n, err := store.Ack(ctx, "q1", []string{id})
	if err != nil || n != 1 {
		t.Fatalf("ack: n=%d err=%v", n, err)
	}
}

func TestAckOnlyDeletesJobsWithLiveLease(t *testing.T) {
	store, cfg := newTestStore(t)
	ctx := context.Background()

	// A stale ack against a pending job must leave its payload available.
	pendingID, _, err := store.Enqueue(ctx, "ack-pending", json.RawMessage(`{"kind":"pending"}`), "")
	if err != nil {
		t.Fatalf("enqueue pending: %v", err)
	}
	if n, err := store.Ack(ctx, "ack-pending", []string{pendingID}); err != nil || n != 0 {
		t.Fatalf("stale ack of pending job: n=%d err=%v", n, err)
	}
	if _, err := store.Job(ctx, pendingID); err != nil {
		t.Fatalf("stale ack deleted pending job: %v", err)
	}

	// A request naming the wrong queue must not delete a job leased in its real
	// queue. This also protects against stale workers using an incorrect queue.
	wrongQueueID, _, err := store.Enqueue(ctx, "ack-real", json.RawMessage(`{"kind":"wrong-queue"}`), "")
	if err != nil {
		t.Fatalf("enqueue wrong-queue case: %v", err)
	}
	if _, err := store.Claim(ctx, []string{"ack-real"}, 1, "worker"); err != nil {
		t.Fatalf("claim wrong-queue case: %v", err)
	}
	if n, err := store.Ack(ctx, "ack-other", []string{wrongQueueID}); err != nil || n != 0 {
		t.Fatalf("ack with wrong queue: n=%d err=%v", n, err)
	}
	if _, err := store.Job(ctx, wrongQueueID); err != nil {
		t.Fatalf("wrong-queue ack deleted leased job: %v", err)
	}
	if n, err := store.Ack(ctx, "ack-real", []string{wrongQueueID}); err != nil || n != 1 {
		t.Fatalf("ack with real queue: n=%d err=%v", n, err)
	}

	// After expiry and reaping, the old worker's ack must not delete the
	// requeued payload. A replacement worker must still be able to claim it.
	reapedID, _, err := store.Enqueue(ctx, "ack-reaped", json.RawMessage(`{"kind":"reaped"}`), "")
	if err != nil {
		t.Fatalf("enqueue reaped case: %v", err)
	}
	if _, err := store.Claim(ctx, []string{"ack-reaped"}, 1, "old-worker"); err != nil {
		t.Fatalf("claim reaped case: %v", err)
	}
	time.Sleep(cfg.LeaseTTL + 50*time.Millisecond)
	if _, _, err := store.RunDrainForTest(ctx, "ack-reaped", true); err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n, err := store.Ack(ctx, "ack-reaped", []string{reapedID}); err != nil || n != 0 {
		t.Fatalf("stale ack after reap: n=%d err=%v", n, err)
	}
	j, err := store.Job(ctx, reapedID)
	if err != nil || j == nil {
		t.Fatalf("stale ack after reap deleted payload: job=%v err=%v", j, err)
	}
	if jobs, err := store.Claim(ctx, []string{"ack-reaped"}, 1, "new-worker"); err != nil || len(jobs) != 1 || jobs[0].ID != reapedID {
		t.Fatalf("requeued job unavailable after stale ack: jobs=%+v err=%v", jobs, err)
	}

	// A duplicate ack is idempotent and must report zero on the second call.
	if n, err := store.Ack(ctx, "ack-reaped", []string{reapedID}); err != nil || n != 1 {
		t.Fatalf("first ack: n=%d err=%v", n, err)
	}
	if n, err := store.Ack(ctx, "ack-reaped", []string{reapedID}); err != nil || n != 0 {
		t.Fatalf("duplicate ack: n=%d err=%v", n, err)
	}
}

func TestScheduleCreateListDeleteConsistent(t *testing.T) {
	store, cfg := newTestStore(t)
	ctx := context.Background()
	scheduler := cogs.NewScheduler(store, cfg, slog.Default())

	created, err := scheduler.Create(ctx, "schedule-q", "@hourly", json.RawMessage(`{"kind":"scheduled"}`))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	score, err := store.Redis().ZScore(ctx, "schedules", created.ID).Result()
	if err != nil {
		t.Fatalf("schedule membership missing after create: %v", err)
	}
	if int64(score) != created.NextRun {
		t.Fatalf("schedule score=%v, response next run=%d", score, created.NextRun)
	}
	m, err := store.Redis().HGetAll(ctx, "schedule:"+created.ID).Result()
	if err != nil || len(m) == 0 {
		t.Fatalf("schedule definition missing after create: fields=%v err=%v", m, err)
	}
	listed, err := scheduler.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("created schedule not listed consistently: schedules=%+v err=%v", listed, err)
	}

	if err := scheduler.Delete(ctx, created.ID); err != nil {
		t.Fatalf("delete schedule: %v", err)
	}
	if _, err := store.Redis().ZScore(ctx, "schedules", created.ID).Result(); err != redis.Nil {
		t.Fatalf("schedule membership remains after delete: err=%v", err)
	}
	if fields, err := store.Redis().HGetAll(ctx, "schedule:"+created.ID).Result(); err != nil || len(fields) != 0 {
		t.Fatalf("schedule definition remains after delete: fields=%v err=%v", fields, err)
	}
	listed, err = scheduler.List(ctx)
	if err != nil || len(listed) != 0 {
		t.Fatalf("deleted schedule still listed: schedules=%+v err=%v", listed, err)
	}
}

// TestClaimIsAtomicAcrossWorkers is the direct test of the core correctness
// property: N workers claiming concurrently from a queue of N jobs must never
// see the same job twice and must never miss one. This is what the atomic Lua
// claim script exists to guarantee.
func TestClaimIsAtomicAcrossWorkers(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	const total = 200
	want := map[string]bool{}
	for i := 0; i < total; i++ {
		id, _, err := store.Enqueue(ctx, "race", json.RawMessage(`{}`), "")
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		want[id] = true
	}

	seen := make(chan string, total*4) // generous: a bug that over-claims must not deadlock the test on a full channel
	done := make(chan struct{})
	claimErrs := make(chan error, 64)
	var wg sync.WaitGroup
	const workers = 10
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				jobs, err := store.Claim(ctx, []string{"race"}, 3, "w")
				if err != nil {
					select {
					case claimErrs <- err:
					default:
					}
					return
				}
				for _, j := range jobs {
					seen <- j.ID
				}
				if len(jobs) == 0 {
					return
				}
			}
		}(w)
	}

	got := map[string]int{}
	timedOut := false
loop:
	for len(got) < total {
		select {
		case id := <-seen:
			got[id]++
		case <-time.After(5 * time.Second):
			timedOut = true
			break loop
		}
	}
	close(done)
	wg.Wait()
	close(claimErrs)
	for err := range claimErrs {
		t.Errorf("claim: %v", err)
	}
	if timedOut {
		t.Fatalf("timed out with only %d/%d jobs claimed", len(got), total)
	}

	for id := range want {
		if got[id] != 1 {
			t.Errorf("job %s claimed %d times, want exactly 1", id, got[id])
		}
	}
}

func TestRenewRejectsAfterLeaseGone(t *testing.T) {
	store, cfg := newTestStore(t)
	ctx := context.Background()

	id, _, _ := store.Enqueue(ctx, "q1", json.RawMessage(`{}`), "")
	jobs, err := store.Claim(ctx, []string{"q1"}, 1, "w1")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %+v err=%v", jobs, err)
	}

	// Renewing immediately, while the lease is live, must succeed.
	if _, err := store.Renew(ctx, "q1", id); err != nil {
		t.Fatalf("renew while lease live: %v", err)
	}

	// Let the (short, test-configured) lease expire, then simulate the reaper
	// taking it back by draining the leased set directly.
	time.Sleep(cfg.LeaseTTL + 50*time.Millisecond)
	if _, _, err := store.RunDrainForTest(ctx, "q1", true); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// The renew that matters: this must be refused, because extending it here
	// would let two workers believe they own the same job at once.
	if _, err := store.Renew(ctx, "q1", id); err != cogs.ErrLeaseGone {
		t.Fatalf("expected ErrLeaseGone after reap, got %v", err)
	}
}

func TestReaperRequeuesExpiredLease(t *testing.T) {
	store, cfg := newTestStore(t)
	ctx := context.Background()

	id, _, _ := store.Enqueue(ctx, "q1", json.RawMessage(`{}`), "")
	if _, err := store.Claim(ctx, []string{"q1"}, 1, "w1"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	time.Sleep(cfg.LeaseTTL + 50*time.Millisecond)
	requeued, dead, err := store.RunDrainForTest(ctx, "q1", true)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(dead) != 0 {
		t.Fatalf("expected no dead jobs on first reap, got %v", dead)
	}
	if len(requeued) != 1 || requeued[0] != id {
		t.Fatalf("expected job %s requeued, got %v", id, requeued)
	}

	// The job must be claimable again -- this is the actual crash-recovery
	// behavior, not just an internal bookkeeping change.
	jobs, err := store.Claim(ctx, []string{"q1"}, 1, "w2")
	if err != nil || len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("expected requeued job claimable, got %+v err=%v", jobs, err)
	}
}

func TestReaperDeadLettersAfterMaxRetries(t *testing.T) {
	store, cfg := newTestStore(t)
	ctx := context.Background()

	// A poison-pill job: whatever worker gets it, it never acks. Left alone
	// this would cycle through the fleet forever, killing one worker at a
	// time. Counting reap events as attempts is what breaks that cycle.
	id, _, _ := store.Enqueue(ctx, "poison", json.RawMessage(`{}`), "")

	for i := 0; i <= cfg.MaxRetries; i++ {
		if _, err := store.Claim(ctx, []string{"poison"}, 1, "w"); err != nil {
			t.Fatalf("claim round %d: %v", i, err)
		}
		time.Sleep(cfg.LeaseTTL + 50*time.Millisecond)
		_, dead, err := store.RunDrainForTest(ctx, "poison", true)
		if err != nil {
			t.Fatalf("drain round %d: %v", i, err)
		}
		if i == cfg.MaxRetries {
			if len(dead) != 1 || dead[0] != id {
				t.Fatalf("expected dead-letter on round %d, got dead=%v", i, dead)
			}
		} else if len(dead) != 0 {
			t.Fatalf("dead-lettered too early on round %d: %v", i, dead)
		}
	}

	ids, err := store.DeadJobs(ctx, "poison", 10)
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("expected job in dead-letter list, got %v err=%v", ids, err)
	}
}

func TestIdempotentEnqueueReturnsOriginal(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	id1, created1, err := store.Enqueue(ctx, "q1", json.RawMessage(`{"a":1}`), "key-1")
	if err != nil || !created1 {
		t.Fatalf("first enqueue: created=%v err=%v", created1, err)
	}
	id2, created2, err := store.Enqueue(ctx, "q1", json.RawMessage(`{"a":2}`), "key-1")
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if created2 {
		t.Fatal("duplicate enqueue with same idempotency key must not create a second job")
	}
	if id1 != id2 {
		t.Fatalf("expected same job id back, got %s vs %s", id1, id2)
	}

	st, err := store.Stats(ctx, "q1")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Pending != 1 {
		t.Fatalf("expected exactly one pending job, got %d", st.Pending)
	}
}

func TestFailSchedulesRetryWithBackoff(t *testing.T) {
	store, cfg := newTestStore(t)
	ctx := context.Background()

	id, _, _ := store.Enqueue(ctx, "q1", json.RawMessage(`{}`), "")
	if _, err := store.Claim(ctx, []string{"q1"}, 1, "w"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	state, err := store.Fail(ctx, "q1", id, 0)
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if state != cogs.StateRetry {
		t.Fatalf("expected retry state, got %s", state)
	}

	// Immediately: nothing claimable yet, the backoff hasn't elapsed.
	jobs, _ := store.Claim(ctx, []string{"q1"}, 1, "w")
	if len(jobs) != 0 {
		t.Fatalf("job claimable before backoff elapsed: %+v", jobs)
	}

	time.Sleep(cfg.BackoffBase + 100*time.Millisecond)
	requeued, _, err := store.RunDrainForTest(ctx, "q1", false)
	if err != nil {
		t.Fatalf("drain retry: %v", err)
	}
	if len(requeued) != 1 || requeued[0] != id {
		t.Fatalf("expected job requeued from retry set, got %v", requeued)
	}

	jobs, err = store.Claim(ctx, []string{"q1"}, 1, "w")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("expected job claimable after backoff, got %+v err=%v", jobs, err)
	}
	if jobs[0].Attempts != 1 {
		t.Fatalf("expected attempts=1 after one failure, got %d", jobs[0].Attempts)
	}
}

func TestBackoffIsMonotonicWithJitterSpread(t *testing.T) {
	base := 100 * time.Millisecond
	// Nominal delay doubles each attempt; jitter keeps every sample within
	// [50%, 100%] of nominal, so attempt N+1's minimum must still exceed
	// attempt N's minimum once N is large enough to separate the ranges.
	d0 := cogs.Backoff(base, 0) // range [50,100)ms
	d3 := cogs.Backoff(base, 3) // range [400,800)ms
	if d3 <= d0 {
		// Flaky in theory (both draws are random), vanishingly unlikely in
		// practice given the disjoint ranges; this is the honest way to test
		// a jittered function without mocking the RNG.
		t.Fatalf("expected attempt 3 backoff (%v) to typically exceed attempt 0 (%v)", d3, d0)
	}
	if d3 < 400*time.Millisecond || d3 >= 800*time.Millisecond {
		t.Fatalf("attempt 3 backoff %v outside expected [400ms,800ms) range", d3)
	}
}

func TestConcurrentFailNeverExceedsMaxRetriesJobs(t *testing.T) {
	// Guards against a regression where the reaper and an explicit /fail could
	// double-count an attempt for the same job, dead-lettering it early.
	store, cfg := newTestStore(t)
	ctx := context.Background()

	id, _, _ := store.Enqueue(ctx, "q1", json.RawMessage(`{}`), "")
	for i := 0; i < cfg.MaxRetries; i++ {
		if _, err := store.Claim(ctx, []string{"q1"}, 1, "w"); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		state, err := store.Fail(ctx, "q1", id, i)
		if err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
		if state != cogs.StateRetry {
			t.Fatalf("expected retry at attempt %d, got %s", i, state)
		}
		time.Sleep(cfg.BackoffBase*(1<<uint(i)) + 200*time.Millisecond)
		if _, _, err := store.RunDrainForTest(ctx, "q1", false); err != nil {
			t.Fatalf("drain: %v", err)
		}
	}

	if _, err := store.Claim(ctx, []string{"q1"}, 1, "w"); err != nil {
		t.Fatalf("final claim: %v", err)
	}
	state, err := store.Fail(ctx, "q1", id, cfg.MaxRetries)
	if err != nil {
		t.Fatalf("final fail: %v", err)
	}
	if state != cogs.StateDead {
		t.Fatalf("expected dead-letter after exhausting retries, got %s", state)
	}
}
