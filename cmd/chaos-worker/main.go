// Command chaos-worker is a worker process the chaos test spawns and kills.
//
// It has to be a real OS process, not a goroutine. A goroutine "killed" by
// cancelling its context unwinds cleanly, releases its lease, and proves
// nothing. SIGKILL to a process is the actual failure being defended against:
// no cleanup, no ack, no lease release, TCP connections left hanging.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/akshaygajjala1/cogs/sdk/go/cogsclient"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		serverURL = flag.String("server", "http://127.0.0.1:8080", "cogs server URL")
		redisAddr = flag.String("redis", "127.0.0.1:6379", "redis address for bookkeeping")
		queue     = flag.String("queue", "chaos", "queue to consume")
		workerID  = flag.String("id", "", "worker id")
		batch     = flag.Int("batch", 4, "claim batch size")
		conc      = flag.Int("concurrency", 4, "concurrent jobs")
		workMin   = flag.Duration("work-min", 50*time.Millisecond, "minimum simulated work")
		workMax   = flag.Duration("work-max", 400*time.Millisecond, "maximum simulated work")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := cogsclient.New(strings.TrimSuffix(*serverURL, "/"), "")
	w := cogsclient.NewWorker(client, *workerID)
	w.Batch = *batch
	w.Concurrent = *conc
	w.WaitMS = 500
	w.Log = log

	inflightKey := "chaos:inflight:" + *workerID

	// Record what this worker holds, so when the test kills it, the test can
	// read back exactly which jobs were in flight at the moment of death and
	// time their recovery.
	w.OnClaim = func(job cogsclient.Job) {
		rdb.SAdd(context.Background(), inflightKey, job.ID)
		rdb.HSet(context.Background(), "chaos:claimed_at", job.ID, time.Now().UnixMilli())
	}
	w.OnAck = func(job cogsclient.Job) {
		rdb.SRem(context.Background(), inflightKey, job.ID)
	}

	w.Handle(*queue, func(ctx context.Context, job cogsclient.Job) error {
		// Count executions BEFORE the work, not after. Counting after would
		// miss exactly the case worth measuring: a worker killed mid-job. The
		// gap between this counter and the ack count is the real, honest
		// at-least-once duplicate rate.
		rdb.HIncrBy(context.Background(), "chaos:runs", job.ID, 1)

		spread := *workMax - *workMin
		d := *workMin
		if spread > 0 {
			d += time.Duration(rand.Int63n(int64(spread)))
		}
		select {
		case <-ctx.Done():
			// Lease lost: another worker owns this job now. Stop immediately.
			return ctx.Err()
		case <-time.After(d):
		}

		var payload struct {
			N int `json:"n"`
		}
		_ = json.Unmarshal(job.Payload, &payload)
		return nil
	})

	if err := w.Run(ctx); err != nil {
		log.Error("worker exited", "err", err)
		os.Exit(1)
	}
}
