// Package cogsclient is the Go worker SDK for Cogs.
//
// The HTTP protocol is deliberately simple enough to use by hand, but the
// heartbeat is the part everyone gets wrong: forget it and every job longer
// than the lease gets re-queued and run twice while the original worker is
// still happily working on it. This package's whole reason to exist is that
// nobody should have to remember to renew a lease.
package cogsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// ErrLeaseLost means the server refused a renewal because the lease had
// already expired and the job was re-queued elsewhere. A handler receiving a
// cancelled context for this reason should stop and discard its work: another
// worker already has the job.
var ErrLeaseLost = errors.New("cogs: lease lost")

type Job struct {
	ID            string          `json:"id"`
	Queue         string          `json:"queue"`
	Payload       json.RawMessage `json:"payload"`
	Attempts      int             `json:"attempts"`
	LeaseDeadline int64           `json:"lease_deadline_ms"`
}

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		http: &http.Client{
			Timeout: 60 * time.Second,
			// Without a raised MaxIdleConnsPerHost, Go's default of 2 forces a
			// fresh TCP handshake for most requests under concurrency, which
			// shows up as latency that looks like server slowness but isn't.
			Transport: &http.Transport{
				MaxIdleConns:        512,
				MaxIdleConnsPerHost: 512,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *Client) do(ctx context.Context, method, path string, in, out interface{}) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if out != nil && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
		return resp.StatusCode, nil
	}
	// Drain so the connection can be reused rather than closed and re-dialled.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

type EnqueueResult struct {
	ID      string `json:"id"`
	Queue   string `json:"queue"`
	Created bool   `json:"created"`
}

// Enqueue submits a job. An idempotency key, when supplied, makes a repeated
// submission return the original job instead of creating a second one --
// which is what makes it safe to retry an enqueue that timed out.
func (c *Client) Enqueue(ctx context.Context, queue string, payload interface{}, idempotencyKey string) (EnqueueResult, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return EnqueueResult{}, err
	}
	var out EnqueueResult
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs", map[string]interface{}{
		"queue": queue, "payload": json.RawMessage(raw), "idempotency_key": idempotencyKey,
	}, &out)
	if err != nil {
		return out, err
	}
	if status >= 300 {
		return out, fmt.Errorf("cogs: enqueue returned %d", status)
	}
	return out, nil
}

type ClaimResult struct {
	Jobs             []Job `json:"jobs"`
	LeaseTTLMS       int64 `json:"lease_ttl_ms"`
	HeartbeatEveryMS int64 `json:"heartbeat_every_ms"`
}

func (c *Client) Claim(ctx context.Context, queues []string, batch, waitMS int, workerID string) (ClaimResult, error) {
	var out ClaimResult
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs/claim", map[string]interface{}{
		"queues": queues, "batch": batch, "wait_ms": waitMS, "worker_id": workerID,
	}, &out)
	if err != nil {
		return out, err
	}
	if status >= 300 {
		return out, fmt.Errorf("cogs: claim returned %d", status)
	}
	return out, nil
}

// Renew extends a lease. A 409 becomes ErrLeaseLost, which callers must treat
// as "stop working", not as a transient error to retry.
func (c *Client) Renew(ctx context.Context, queue, id string) error {
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs/"+id+"/renew",
		map[string]string{"queue": queue}, nil)
	if err != nil {
		return err
	}
	if status == http.StatusConflict {
		return ErrLeaseLost
	}
	if status >= 300 {
		return fmt.Errorf("cogs: renew returned %d", status)
	}
	return nil
}

func (c *Client) Ack(ctx context.Context, queue string, ids []string) error {
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs/ack",
		map[string]interface{}{"queue": queue, "ids": ids}, nil)
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusConflict {
		return fmt.Errorf("cogs: ack returned %d", status)
	}
	return nil
}

func (c *Client) Fail(ctx context.Context, queue, id string, attempt int, reason string) error {
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs/"+id+"/fail",
		map[string]interface{}{"queue": queue, "error": reason, "attempt": attempt}, nil)
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusConflict {
		return fmt.Errorf("cogs: fail returned %d", status)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

// Handler processes one job. Returning an error reports a failure, which the
// server turns into a retry with backoff or a dead-letter.
//
// The context is cancelled if the lease is lost, so a well-behaved handler
// that respects it stops doing work the moment another worker has taken over.
type Handler func(ctx context.Context, job Job) error

// Worker runs the claim/heartbeat/ack loop.
type Worker struct {
	client   *Client
	id       string
	queues   []string
	handlers map[string]Handler

	Batch      int
	WaitMS     int
	Concurrent int
	Log        *slog.Logger

	// OnClaim and OnAck are optional observation hooks, used by the chaos test
	// to record what a worker held at the moment it was killed.
	OnClaim func(job Job)
	OnAck   func(job Job)
}

func NewWorker(client *Client, workerID string) *Worker {
	return &Worker{
		client:     client,
		id:         workerID,
		handlers:   map[string]Handler{},
		Batch:      1,
		WaitMS:     1000,
		Concurrent: 1,
		Log:        slog.Default(),
	}
}

func (w *Worker) Handle(queue string, h Handler) {
	w.handlers[queue] = h
	w.queues = append(w.queues, queue)
}

// Run claims and processes until ctx is cancelled, then returns once in-flight
// jobs finish. Jobs still running at a hard kill are not lost: their leases
// expire and the reaper re-queues them.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.queues) == 0 {
		return errors.New("cogs: worker has no handlers registered")
	}
	sem := make(chan struct{}, w.Concurrent)
	var wg sync.WaitGroup

	for {
		if ctx.Err() != nil {
			break
		}
		res, err := w.client.Claim(ctx, w.queues, w.Batch, w.WaitMS, w.id)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.Log.Error("claim failed", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}

		heartbeat := time.Duration(res.HeartbeatEveryMS) * time.Millisecond
		if heartbeat <= 0 {
			heartbeat = time.Second
		}
		for _, job := range res.Jobs {
			wg.Add(1)
			sem <- struct{}{}
			go func(job Job) {
				defer wg.Done()
				defer func() { <-sem }()
				w.process(context.WithoutCancel(ctx), job, heartbeat)
			}(job)
		}
	}
	wg.Wait()
	return nil
}

// process runs one job with a heartbeat alongside it.
//
// Renew every lease/3 to leave time for transient delays. Two missed renewals
// consume that margin; a third attempt at the deadline may already be too late.
func (w *Worker) process(parent context.Context, job Job, heartbeat time.Duration) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	if w.OnClaim != nil {
		w.OnClaim(job)
	}

	var leaseLost bool
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		t := time.NewTicker(heartbeat)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				err := w.client.Renew(ctx, job.Queue, job.ID)
				if errors.Is(err, ErrLeaseLost) {
					mu.Lock()
					leaseLost = true
					mu.Unlock()
					// Cancel the handler: the job now belongs to somebody
					// else, and continuing would duplicate its side effects.
					w.Log.Warn("lease lost; abandoning job", "job", job.ID)
					cancel()
					return
				} else if err != nil && ctx.Err() == nil {
					w.Log.Warn("renew failed", "job", job.ID, "err", err)
				}
			}
		}
	}()

	handler := w.handlers[job.Queue]
	var err error
	if handler == nil {
		err = fmt.Errorf("no handler registered for queue %q", job.Queue)
	} else {
		err = handler(ctx, job)
	}
	close(done)

	mu.Lock()
	lost := leaseLost
	mu.Unlock()
	if lost {
		// Neither ack nor fail: we no longer hold the lease, and reporting
		// against a job another worker owns would corrupt its attempt count.
		return
	}

	ackCtx, ackCancel := context.WithTimeout(parent, 10*time.Second)
	defer ackCancel()
	if err != nil {
		w.Log.Error("job failed", "job", job.ID, "err", err)
		if ferr := w.client.Fail(ackCtx, job.Queue, job.ID, job.Attempts, err.Error()); ferr != nil {
			w.Log.Error("reporting failure failed", "job", job.ID, "err", ferr)
		}
		return
	}
	if aerr := w.client.Ack(ackCtx, job.Queue, []string{job.ID}); aerr != nil {
		w.Log.Error("ack failed", "job", job.ID, "err", aerr)
		return
	}
	if w.OnAck != nil {
		w.OnAck(job)
	}
}
