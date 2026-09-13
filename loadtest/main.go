// Command loadtest measures Cogs throughput and claim latency.
//
// This is deliberately a hand-written Go tool rather than k6: k6 can express
// stateless request-per-second load well, but it cannot express "claim
// batch=32, then ack exactly those IDs, in a loop" without contorting its
// scripting model. That claim-then-ack coupling is the whole design being
// measured, so the harness needs to speak it natively.
//
// It reports two views of the same request, and reports them separately on
// purpose:
//   - server-side: a Prometheus histogram scraped from /metrics, unaffected
//     by anything client-side.
//   - client-side: measured here, in the caller. The gap between the two is
//     the pure HTTP/network overhead, and knowing that gap is itself the
//     answer to "how much of your latency is the network."
//
// A long-poll claim against an empty queue blocks for up to wait_ms by
// design; that is not claim latency, it is idle time, and mixing the two
// would understate the real number. This tool keeps the queue non-empty by
// running producers and consumers concurrently, and the -sweep mode reports
// per batch size so the batching argument in docs/DESIGN.md is a measured
// curve rather than an assertion.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/akshaygajjala1/cogs/sdk/go/cogsclient"
)

func main() {
	var (
		serverURL   = flag.String("server", "http://127.0.0.1:8080", "cogs server URL")
		queue       = flag.String("queue", "loadtest", "queue to use")
		duration    = flag.Duration("dur", 30*time.Second, "duration per run")
		batch       = flag.Int("batch", 32, "claim batch size (ignored with -sweep)")
		producers   = flag.Int("producers", 8, "concurrent producer goroutines")
		consumers   = flag.Int("consumers", 8, "concurrent consumer goroutines")
		enqueueOnly = flag.Bool("prefill", false, "just enqueue N jobs and exit, don't consume")
		prefillN    = flag.Int("prefill-n", 50000, "jobs to prefill with -prefill")
		sweep       = flag.Bool("sweep", false, "run the batch curve {1,8,32,64} instead of a single run")
		warmup      = flag.Duration("warmup", 10*time.Second, "warmup discarded from results")
	)
	flag.Parse()

	client := cogsclient.New(strings.TrimSuffix(*serverURL, "/"), "")

	if *enqueueOnly {
		prefill(client, *queue, *prefillN)
		return
	}

	if *sweep {
		// A quick reachability check up front: a dead server makes every
		// producer/consumer request fail, which without this check reports as
		// a quiet "0 jobs/sec" indistinguishable from a real (bad) result.
		if err := ping(*serverURL); err != nil {
			fmt.Fprintf(os.Stderr, "cogs server unreachable at %s: %v\n", *serverURL, err)
			fmt.Fprintln(os.Stderr, "start it first, e.g.: go run ./cmd/cogs")
			os.Exit(1)
		}

		fmt.Println("batch |   jobs/sec | client p50 | client p95 | client p99 | client max | errors")
		fmt.Println("------+------------+------------+------------+------------+------------+-------")
		for _, b := range []int{1, 8, 32, 64} {
			r := run(client, *queue, b, *producers, *consumers, *warmup, *duration)
			fmt.Printf("%5d | %10.0f | %10s | %10s | %10s | %10s | %6d\n",
				b, r.jobsPerSec, fmtMS(r.claimP50), fmtMS(r.claimP95), fmtMS(r.claimP99), fmtMS(r.claimMax), r.errors)
			if r.errors > 0 && r.acked == 0 {
				fmt.Fprintln(os.Stderr, "  ^ all requests failed at this batch size — is the server still running?")
			}
		}
		fmt.Println()
		fmt.Println("Server-side numbers (queue non-empty, at the achieved rate) are in /metrics:")
		fmt.Println("  cogs_request_duration_seconds{endpoint=\"claim\"} — the number the p99 claim is defined against")
		return
	}

	if err := ping(*serverURL); err != nil {
		fmt.Fprintf(os.Stderr, "cogs server unreachable at %s: %v\n", *serverURL, err)
		fmt.Fprintln(os.Stderr, "start it first, e.g.: go run ./cmd/cogs")
		os.Exit(1)
	}

	r := run(client, *queue, *batch, *producers, *consumers, *warmup, *duration)
	fmt.Printf("batch=%d  achieved=%.0f jobs/sec  acked=%d  errors=%d\n", *batch, r.jobsPerSec, r.acked, r.errors)
	fmt.Println("client-side claim latency:")
	fmt.Printf("  p50 %s   p95 %s   p99 %s   max %s\n", fmtMS(r.claimP50), fmtMS(r.claimP95), fmtMS(r.claimP99), fmtMS(r.claimMax))
	fmt.Println()
	fmt.Println("This is the CLIENT view (includes network). The claim being made is server-side;")
	fmt.Println("scrape /metrics and read cogs_request_duration_seconds{endpoint=\"claim\"} for that number,")
	fmt.Println("and see docs/DESIGN.md for why the two are reported separately.")
}

// ping does a quick, short-timeout health check so a dead server is reported
// as a clear error up front instead of a silent "0 jobs/sec" that looks like
// a real (if bad) measurement.
func ping(serverURL string) error {
	req, err := http.NewRequest(http.MethodGet, strings.TrimSuffix(serverURL, "/")+"/healthz", nil)
	if err != nil {
		return err
	}
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func fmtMS(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
}

type result struct {
	jobsPerSec                             float64
	acked                                  int64
	errors                                 int64
	claimP50, claimP95, claimP99, claimMax time.Duration
}

// run drives producers enqueueing at full tilt and consumers doing
// claim-batch -> ack-batch, for the warmup+duration window, discarding the
// warmup from the reported numbers. Both loops run the whole time so the
// queue stays non-empty for the entire measured window -- an empty queue
// would make claim latency measure long-poll idle time instead of the claim
// path itself.
func run(client *cogsclient.Client, queue string, batch, producers, consumers int, warmup, duration time.Duration) result {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	total := warmup + duration
	runCtx, cancel := context.WithTimeout(ctx, total)
	defer cancel()

	var enqueued, acked, errs int64
	var latMu sync.Mutex
	var claimLatencies []time.Duration

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			n := 0
			for runCtx.Err() == nil {
				_, err := client.Enqueue(runCtx, queue, map[string]int{"p": p, "n": n}, "")
				n++
				if err != nil {
					if runCtx.Err() != nil {
						return
					}
					atomic.AddInt64(&errs, 1)
					continue
				}
				atomic.AddInt64(&enqueued, 1)
			}
		}(p)
	}

	warmupDeadline := time.Now().Add(warmup)
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			workerID := fmt.Sprintf("loadtest-%d", c)
			for runCtx.Err() == nil {
				start := time.Now()
				res, err := client.Claim(runCtx, []string{queue}, batch, 200, workerID)
				elapsed := time.Since(start)
				if err != nil {
					if runCtx.Err() != nil {
						return
					}
					atomic.AddInt64(&errs, 1)
					continue
				}
				if len(res.Jobs) == 0 {
					continue
				}
				if time.Now().After(warmupDeadline) {
					latMu.Lock()
					claimLatencies = append(claimLatencies, elapsed)
					latMu.Unlock()
				}

				ids := make([]string, len(res.Jobs))
				for i, j := range res.Jobs {
					ids[i] = j.ID
				}
				if err := client.Ack(runCtx, queue, ids); err != nil {
					if runCtx.Err() != nil {
						return
					}
					atomic.AddInt64(&errs, 1)
					continue
				}
				atomic.AddInt64(&acked, int64(len(ids)))
			}
		}(c)
	}

	wg.Wait()

	latMu.Lock()
	sort.Slice(claimLatencies, func(i, j int) bool { return claimLatencies[i] < claimLatencies[j] })
	lat := claimLatencies
	latMu.Unlock()

	r := result{
		jobsPerSec: float64(atomic.LoadInt64(&acked)) / duration.Seconds(),
		acked:      atomic.LoadInt64(&acked),
		errors:     atomic.LoadInt64(&errs),
	}
	if len(lat) > 0 {
		r.claimP50 = pct(lat, 0.50)
		r.claimP95 = pct(lat, 0.95)
		r.claimP99 = pct(lat, 0.99)
		r.claimMax = lat[len(lat)-1]
	}
	return r
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// prefill seeds the queue with N jobs before a measured run, so the very
// first consumer requests don't measure an artificially empty queue.
func prefill(client *cogsclient.Client, queue string, n int) {
	ctx := context.Background()
	const workers = 32
	var wg sync.WaitGroup
	var done int64
	chunk := n / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < chunk; i++ {
				if _, err := client.Enqueue(ctx, queue, map[string]int{"w": w, "n": i}, ""); err != nil {
					fmt.Fprintln(os.Stderr, "enqueue error:", err)
					continue
				}
				atomic.AddInt64(&done, 1)
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("prefilled %d jobs into %q\n", atomic.LoadInt64(&done), queue)
}

// scrapeMetric is a small helper for anyone scripting around this tool: pull a
// single counter value out of /metrics without a full Prometheus client.
func scrapeMetric(metricsURL, name string) (float64, error) {
	resp, err := http.Get(metricsURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name) {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				return strconv.ParseFloat(fields[1], 64)
			}
		}
	}
	return 0, fmt.Errorf("metric %s not found", name)
}
