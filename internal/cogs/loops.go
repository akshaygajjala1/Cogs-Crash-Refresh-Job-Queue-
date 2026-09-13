package cogs

import (
	"context"
	"log/slog"
	"math/rand"
	"time"
)

// Backoff returns the delay before attempt n is retried, with jitter.
//
// The jitter is not decoration. Without it, ten thousand jobs that all failed
// against the same downed dependency retry at the same millisecond and
// stampede it the instant it comes back, knocking it over again. Spreading
// each delay across [50%, 100%] of its nominal value breaks the synchrony.
func Backoff(base time.Duration, attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 20 { // cap the shift; 2^20 * 1s is already ~12 days
		attempt = 20
	}
	nominal := base * (1 << uint(attempt))
	jitter := 0.5 + rand.Float64()*0.5
	return time.Duration(float64(nominal) * jitter)
}

// drainDue is the one loop body behind the reaper, the retry drainer, and the
// scheduler. Each is the same shape: a sorted set scored by a future
// timestamp, drained every tick of whatever is now due. Writing it once means
// crash recovery and retry scheduling share a single tested implementation
// rather than two that drift apart.
//
// It runs only while this process holds leadership. Every replica running its
// own reaper would be merely wasteful; every replica running its own scheduler
// would fire each cron job N times, which is a correctness bug.
func drainDue(ctx context.Context, tick time.Duration, isLeader func() bool, pass func(context.Context) error, log *slog.Logger, name string) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !isLeader() {
				continue
			}
			if err := pass(ctx); err != nil && ctx.Err() == nil {
				log.Error("drain pass failed", "loop", name, "err", err)
			}
		}
	}
}

// Runtime owns the background loops.
type Runtime struct {
	store  *Store
	cfg    Config
	log    *slog.Logger
	leader *Leader
	sched  *Scheduler

	// OnRequeue, when set, is called with the IDs the reaper just recovered.
	// The chaos test uses it to timestamp real recoveries rather than inferring
	// them by polling, which would blur the very latency being measured.
	// Nil in production; it must not block, as it runs on the reaper's tick.
	OnRequeue func(queue string, ids []string, at time.Time)
}

func NewRuntime(store *Store, cfg Config, log *slog.Logger) *Runtime {
	return &Runtime{
		store:  store,
		cfg:    cfg,
		log:    log,
		leader: NewLeader(store, cfg, log),
		sched:  NewScheduler(store, cfg, log),
	}
}

// Start launches leadership election and the three background loops. They all
// stop when ctx is cancelled.
func (r *Runtime) Start(ctx context.Context) {
	go r.leader.Run(ctx)
	go drainDue(ctx, r.cfg.ReaperInterval, r.leader.IsLeader, r.reapPass, r.log, "reaper")
	go drainDue(ctx, r.cfg.RetryInterval, r.leader.IsLeader, r.retryPass, r.log, "retry")
	go drainDue(ctx, r.cfg.SchedulerInterval, r.leader.IsLeader, r.sched.Pass, r.log, "scheduler")
}

func (r *Runtime) IsLeader() bool { return r.leader.IsLeader() }

// reapPass re-queues jobs whose lease expired -- that is, jobs whose worker
// died or hung. This is the mechanism behind "no job is lost to a crash".
//
// A reaped job counts as a failed attempt, which matters for the poison-pill
// case: a job that reliably kills whichever worker picks it up would otherwise
// cycle forever, killing the entire fleet one worker at a time. Counting the
// attempt sends it to the dead-letter list after MaxRetries instead.
func (r *Runtime) reapPass(ctx context.Context) error {
	queues, err := r.store.KnownQueues(ctx)
	if err != nil {
		return err
	}
	for _, q := range queues {
		requeued, dead, err := r.store.drain(ctx, leasedKey(q), q, true)
		if err != nil {
			return err
		}
		if len(requeued) > 0 {
			if r.OnRequeue != nil {
				r.OnRequeue(q, requeued, time.Now())
			}
			JobsRequeued.WithLabelValues(q).Add(float64(len(requeued)))
			r.log.Info("reaped expired leases", "queue", q, "requeued", len(requeued), "ids", truncate(requeued))
		}
		if len(dead) > 0 {
			JobsDead.WithLabelValues(q, "max_attempts").Add(float64(len(dead)))
			r.log.Warn("dead-lettered after repeated lease expiry", "queue", q, "count", len(dead))
		}
	}
	return nil
}

// retryPass moves jobs whose backoff has elapsed back onto pending. Attempts
// were already counted when the worker reported the failure, so this pass does
// not count them again.
func (r *Runtime) retryPass(ctx context.Context) error {
	queues, err := r.store.KnownQueues(ctx)
	if err != nil {
		return err
	}
	for _, q := range queues {
		requeued, _, err := r.store.drain(ctx, retryKey(q), q, false)
		if err != nil {
			return err
		}
		if len(requeued) > 0 {
			JobsRetried.WithLabelValues(q).Add(float64(len(requeued)))
		}
	}
	return nil
}

func truncate(ids []string) []string {
	if len(ids) > 5 {
		return ids[:5]
	}
	return ids
}
