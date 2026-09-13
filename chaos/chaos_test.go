// Package chaos holds the crash-recovery proof for Cogs.
//
// The claim being defended is: a job whose worker dies is re-queued in under
// five seconds, and no job is ever lost. This test is the evidence. It enqueues
// thousands of jobs, runs real worker processes, SIGKILLs a third of them
// mid-job, and then asserts that every single job still ran -- while recording
// the distribution of death-to-requeue latency so the guarantee is a measured
// number rather than an assertion.
//
// "Characterize failure" is the operative phrase: a pass/fail bit would prove
// far less than a histogram. The percentiles printed here are the number to
// quote, and the test fails if p99 exceeds the five-second budget.
package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/akshaygajjala1/cogs/internal/cogs"
	"github.com/akshaygajjala1/cogs/sdk/go/cogsclient"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	testQueue  = "chaos"
	numJobs    = 2000
	numWorkers = 8
	killFrac   = 0.34 // roughly a third of the fleet dies
	// The guarantee under test. Worst case is lease TTL (3s) + reaper interval
	// (500ms) + scan time, so a 5s budget leaves real headroom without being
	// so loose that a regression would slip through.
	requeueBudget = 5 * time.Second
)

func redisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}

func TestChaosNoJobLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test spawns processes and takes ~60s; skipped under -short")
	}
	if os.Getenv("COGS_TEST_REDIS") != "1" {
		t.Skip("Redis chaos test requires COGS_TEST_REDIS=1 (it flushes the selected database)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr()})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("COGS_TEST_REDIS=1 but Redis unavailable at %s: %v", redisAddr(), err)
	}
	// A dedicated database, flushed, so a previous run cannot make this one
	// pass or fail for the wrong reason.
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	cfg := cogs.LoadConfig()
	cfg.RedisAddr = redisAddr()
	cfg.LeaseTTL = 3 * time.Second
	cfg.ReaperInterval = 500 * time.Millisecond

	store := cogs.NewStore(cfg)
	defer store.Close()
	if err := store.LoadScripts(ctx); err != nil {
		t.Fatalf("load scripts: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	rt := cogs.NewRuntime(store, cfg, log)

	// Timestamp every real recovery as the reaper performs it. Polling Redis to
	// notice re-queues would add the poll interval to every measurement and
	// blur the very number being reported.
	var recMu sync.Mutex
	requeuedAt := map[string]time.Time{}
	rt.OnRequeue = func(_ string, ids []string, at time.Time) {
		recMu.Lock()
		defer recMu.Unlock()
		for _, id := range ids {
			if _, seen := requeuedAt[id]; !seen {
				requeuedAt[id] = at
			}
		}
	}
	rt.Start(ctx)

	reg := cogs.NewRegistry()
	api := cogs.NewAPI(store, cfg, log, rt)
	srv := httptest.NewServer(api.Routes(promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))
	defer srv.Close()

	// ---- enqueue -----------------------------------------------------------
	client := cogsclient.New(srv.URL, "")
	enqueued := make(map[string]bool, numJobs)
	for i := 0; i < numJobs; i++ {
		res, err := client.Enqueue(ctx, testQueue, map[string]int{"n": i}, "")
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		enqueued[res.ID] = true
	}
	t.Logf("enqueued %d jobs", len(enqueued))

	workerBin := buildWorker(t)

	// ---- run the fleet, killing a third of it ------------------------------
	type killRecord struct {
		at   time.Time
		jobs []string
	}
	var killMu sync.Mutex
	var kills []killRecord

	procs := make([]*exec.Cmd, numWorkers)
	ids := make([]string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		ids[i] = fmt.Sprintf("w%d", i)
		procs[i] = startWorker(t, ctx, workerBin, srv.URL, ids[i])
	}

	nw := numWorkers
	victims := int(float64(nw) * killFrac)
	if victims < 1 {
		victims = 1
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Stagger the kills across the run so recoveries happen while the rest
		// of the fleet is under load, not against an idle system.
		for round := 0; round < 3; round++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(2+rand.Intn(2)) * time.Second):
			}
			for v := 0; v < victims; v++ {
				idx := rand.Intn(numWorkers)
				if procs[idx] == nil || procs[idx].Process == nil {
					continue
				}

				// Read what this worker holds BEFORE killing it. After SIGKILL
				// the process cannot tell us anything.
				inflight, err := rdb.SMembers(ctx, "chaos:inflight:"+ids[idx]).Result()
				if err != nil {
					continue
				}
				killAt := time.Now()
				_ = procs[idx].Process.Kill()

				if len(inflight) > 0 {
					killMu.Lock()
					kills = append(kills, killRecord{at: killAt, jobs: inflight})
					killMu.Unlock()
				}
				t.Logf("killed %s holding %d job(s)", ids[idx], len(inflight))

				// A dead worker is replaced, as an autoscaler or orchestrator
				// would: the interesting question is whether work recovers, not
				// whether a shrinking fleet eventually drains.
				rdb.Del(ctx, "chaos:inflight:"+ids[idx])
				ids[idx] = fmt.Sprintf("%s-r%d", ids[idx], round)
				procs[idx] = startWorker(t, ctx, workerBin, srv.URL, ids[idx])
			}
		}
	}()

	// ---- wait for the queue to drain ---------------------------------------
	deadline := time.Now().Add(2 * time.Minute)
	var final cogs.QueueStats
	for time.Now().Before(deadline) {
		st, err := store.Stats(ctx, testQueue)
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		final = st
		if st.Pending == 0 && st.Leased == 0 && st.Retry == 0 {
			// Hold briefly: a job could be between structures for a moment.
			time.Sleep(1500 * time.Millisecond)
			st, _ = store.Stats(ctx, testQueue)
			final = st
			if st.Pending == 0 && st.Leased == 0 && st.Retry == 0 {
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	wg.Wait()
	for _, p := range procs {
		if p != nil && p.Process != nil {
			_ = p.Process.Kill()
		}
	}

	// ---- assertions --------------------------------------------------------
	runs, err := rdb.HGetAll(ctx, "chaos:runs").Result()
	if err != nil {
		t.Fatalf("read run counts: %v", err)
	}

	var missing []string
	duplicates, totalRuns := 0, 0
	for id := range enqueued {
		c, ok := runs[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		n, _ := strconv.Atoi(c)
		totalRuns += n
		if n > 1 {
			duplicates += n - 1
		}
	}

	// THE assertion. Everything else in this file is measurement; this is the
	// guarantee. A single lost job means crash recovery does not work.
	if len(missing) > 0 {
		t.Errorf("JOB LOSS: %d of %d jobs never ran (e.g. %v)",
			len(missing), len(enqueued), missing[:min(5, len(missing))])
	}
	if final.Pending != 0 || final.Leased != 0 || final.Retry != 0 {
		t.Errorf("queue did not drain: pending=%d leased=%d retry=%d",
			final.Pending, final.Leased, final.Retry)
	}

	// ---- characterize ------------------------------------------------------
	recMu.Lock()
	killMu.Lock()
	var latencies []time.Duration
	unrecovered := 0
	for _, k := range kills {
		for _, jobID := range k.jobs {
			at, ok := requeuedAt[jobID]
			if !ok {
				// The worker finished and acked between our SMEMBERS read and
				// the kill landing. Nothing was lost; there is just no recovery
				// to time.
				unrecovered++
				continue
			}
			if d := at.Sub(k.at); d > 0 {
				latencies = append(latencies, d)
			}
		}
	}
	killMu.Unlock()
	recMu.Unlock()

	report(t, latencies, len(enqueued), len(missing), totalRuns, duplicates, unrecovered)

	if len(latencies) == 0 {
		t.Fatal("no recoveries observed: the test killed workers but measured nothing, " +
			"so it proves nothing -- investigate before trusting a pass")
	}
	p99 := percentile(latencies, 0.99)
	if p99 > requeueBudget {
		t.Errorf("p99 death-to-requeue was %v, over the %v budget", p99.Round(time.Millisecond), requeueBudget)
	}
}

func report(t *testing.T, lat []time.Duration, jobs, lost, runs, dupes, unrecovered int) {
	t.Helper()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	dupRate := 0.0
	if jobs > 0 {
		dupRate = float64(dupes) / float64(jobs) * 100
	}

	t.Log("")
	t.Log("=== CHAOS RESULTS ==========================================")
	t.Logf("  jobs enqueued        %d", jobs)
	t.Logf("  jobs lost            %d   <- the guarantee", lost)
	t.Logf("  total executions     %d", runs)
	t.Logf("  duplicate executions %d (%.2f%%)  <- at-least-once, measured", dupes, dupRate)
	t.Logf("  recoveries timed     %d (%d kills raced an ack)", len(lat), unrecovered)
	if len(lat) > 0 {
		t.Log("  death -> requeue latency:")
		t.Logf("    p50  %v", percentile(lat, 0.50).Round(time.Millisecond))
		t.Logf("    p95  %v", percentile(lat, 0.95).Round(time.Millisecond))
		t.Logf("    p99  %v   <- budget %v", percentile(lat, 0.99).Round(time.Millisecond), requeueBudget)
		t.Logf("    max  %v", lat[len(lat)-1].Round(time.Millisecond))
	}
	t.Log("============================================================")
	t.Log("")
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// buildWorker compiles the worker once. `go run` per worker would add seconds
// of compile time to every spawn and pollute the kill timing.
func buildWorker(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "chaos-worker")
	if runtime.GOOS == "windows" {
		out += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", out, "github.com/akshaygajjala1/cogs/cmd/chaos-worker")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build chaos-worker: %v", err)
	}
	return out
}

func startWorker(t *testing.T, ctx context.Context, bin, serverURL, id string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, bin,
		"-server", serverURL,
		"-redis", redisAddr(),
		"-queue", testQueue,
		"-id", id,
	)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker %s: %v", id, err)
	}
	return cmd
}
