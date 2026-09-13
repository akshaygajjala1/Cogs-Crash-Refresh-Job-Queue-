package cogs

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Leader is a single-holder lock over Redis, used to decide which server
// process runs the background loops.
//
// The HTTP handlers are stateless and every replica serves them, which is what
// makes the service horizontally scalable. The background loops are the
// opposite: they must run in exactly one place. Two reapers is merely wasteful
// duplicate work, but two schedulers means every cron job fires twice, which
// users experience as duplicate emails. This ~60 lines is the entire
// difference between "we run multiple replicas" and "we run multiple replicas
// correctly".
//
// This is a lease, exactly like the job leases: acquire with SET NX PX, renew
// while alive, and let it expire if the process dies so another can take over
// within LeaderLockTTL. It is not a consensus algorithm and does not pretend
// to be one -- under a network partition two processes could briefly both
// believe they lead. That is tolerable here because every loop's actions are
// individually atomic and idempotent-ish: a doubly-reaped job is re-queued
// once, since the drain script's ZREM only succeeds for one caller.
type Leader struct {
	store *Store
	cfg   Config
	log   *slog.Logger
	id    string
	held  atomic.Bool
}

func NewLeader(store *Store, cfg Config, log *slog.Logger) *Leader {
	id, err := NewID()
	if err != nil {
		id = time.Now().Format(time.RFC3339Nano)
	}
	return &Leader{store: store, cfg: cfg, log: log, id: id}
}

func (l *Leader) IsLeader() bool { return l.held.Load() }
func (l *Leader) ID() string     { return l.id }

// Run campaigns for leadership until ctx is cancelled, then releases the lock
// so a peer can take over immediately rather than waiting out the TTL.
func (l *Leader) Run(ctx context.Context) {
	// Renew at a third of the TTL so two consecutive failed renewals still
	// leave time to recover before the lock expires under us.
	tick := l.cfg.LeaderLockTTL / 3
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	l.tryAcquire(ctx)
	for {
		select {
		case <-ctx.Done():
			l.release()
			return
		case <-t.C:
			l.tryAcquire(ctx)
		}
	}
}

func (l *Leader) tryAcquire(ctx context.Context) {
	ttl := l.cfg.LeaderLockTTL

	if l.held.Load() {
		// Renew only if the value is still ours. A process that stalled past
		// its TTL must not extend a lock a peer has since taken.
		ok, err := renewLockScript.Run(ctx, l.store.rdb,
			[]string{leaderKey}, l.id, ttl.Milliseconds()).Int()
		if err != nil {
			l.log.Error("leader renew failed", "err", err)
			return
		}
		if ok == 0 {
			l.held.Store(false)
			l.log.Warn("lost leadership", "id", l.id)
		}
		return
	}

	acquired, err := l.store.rdb.SetArgs(ctx, leaderKey, l.id, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Result()
	if err != nil && err != redis.Nil {
		l.log.Error("leader acquire failed", "err", err)
		return
	}
	if acquired == "OK" {
		l.held.Store(true)
		l.log.Info("became leader", "id", l.id)
	}
}

func (l *Leader) release() {
	if !l.held.Load() {
		return
	}
	// Fresh context: ctx is already cancelled by the time we get here, and the
	// point of releasing is to hand over promptly instead of leaving peers to
	// wait out the full TTL.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := releaseLockScript.Run(ctx, l.store.rdb, []string{leaderKey}, l.id).Err(); err != nil {
		// A closed client here just means the process is already tearing down
		// (e.g. in tests where the Store is closed before shutdown finishes);
		// the lock's TTL still reclaims it, so this is not a leak.
		if !errors.Is(err, redis.ErrClosed) {
			l.log.Error("leader release failed", "err", err)
		}
	}
	l.held.Store(false)
}
