package cogs

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// API holds the HTTP handlers. Every handler is stateless; all state lives in
// Redis, which is what lets any replica serve any request and what makes a
// server crash a non-event.
type API struct {
	store *Store
	cfg   Config
	log   *slog.Logger
	rt    *Runtime
}

func NewAPI(store *Store, cfg Config, log *slog.Logger, rt *Runtime) *API {
	return &API{store: store, cfg: cfg, log: log, rt: rt}
}

// errorResponse is the single error envelope used by every endpoint. One
// consistent shape with a machine-readable code means clients can branch on
// behaviour instead of parsing prose.
type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	var body errorResponse
	body.Error.Code = code
	body.Error.Message = msg
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Routes builds the mux. net/http's own method-and-path patterns are enough
// here; a third-party router would add a dependency to buy nothing.
func (a *API) Routes(metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("POST /v1/jobs", a.wrap("enqueue", a.handleEnqueue))
	mux.Handle("POST /v1/jobs/claim", a.wrap("claim", a.handleClaim))
	mux.Handle("POST /v1/jobs/ack", a.wrap("ack_batch", a.handleAckBatch))
	mux.Handle("POST /v1/jobs/{id}/renew", a.wrap("renew", a.handleRenew))
	mux.Handle("POST /v1/jobs/{id}/ack", a.wrap("ack", a.handleAck))
	mux.Handle("POST /v1/jobs/{id}/fail", a.wrap("fail", a.handleFail))
	mux.Handle("GET /v1/jobs/{id}", a.wrap("get_job", a.handleGetJob))

	mux.Handle("POST /v1/schedules", a.wrap("create_schedule", a.handleCreateSchedule))
	mux.Handle("GET /v1/schedules", a.wrap("list_schedules", a.handleListSchedules))
	mux.Handle("DELETE /v1/schedules/{id}", a.wrap("delete_schedule", a.handleDeleteSchedule))

	mux.Handle("GET /v1/queues/{name}/stats", a.wrap("stats", a.handleStats))
	mux.Handle("GET /v1/dead", a.wrap("dead", a.handleDead))

	// Liveness answers "is this process running" and must never touch Redis:
	// if it did, a Redis blip would make every replica look dead and the
	// orchestrator would restart the entire fleet during the one incident
	// where restarts help least.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	// Readiness answers "can this process serve traffic", so it does check
	// Redis -- a replica that cannot reach Redis should be pulled from the
	// load balancer without being killed.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := contextWithTimeout(r, 2*time.Second)
		defer cancel()
		if err := a.store.Ping(ctx); err != nil {
			writeErr(w, http.StatusServiceUnavailable, "redis_unavailable", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status": "ready", "leader": a.rt.IsLeader(),
		})
	})
	mux.Handle("GET /metrics", metricsHandler)

	return mux
}

type handlerFunc func(http.ResponseWriter, *http.Request) int

// wrap adds auth, body limits, and timing to every endpoint. The handler
// returns the status it wrote so the metric is labelled accurately without
// wrapping ResponseWriter.
func (a *API) wrap(endpoint string, h handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.authorized(r) {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			RequestDuration.WithLabelValues(endpoint, "401").Observe(0)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, a.cfg.MaxBodyBytes)

		start := time.Now()
		status := h(w, r)
		RequestDuration.WithLabelValues(endpoint, strconv.Itoa(status)).
			Observe(time.Since(start).Seconds())
	})
}

// authorized does a constant-time compare so the check cannot be turned into a
// byte-at-a-time oracle by timing it.
func (a *API) authorized(r *http.Request) bool {
	if a.cfg.AuthToken == "" {
		return true // auth disabled, for local development
	}
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(h[len(prefix):]), []byte(a.cfg.AuthToken)) == 1
}

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

type enqueueRequest struct {
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type enqueueResponse struct {
	ID      string `json:"id"`
	Queue   string `json:"queue"`
	Created bool   `json:"created"`
}

// handleEnqueue accepts a job.
//
// Returns 201 for a new job and 200 for an idempotency-key hit. The
// distinction is deliberate and worth keeping: a client that retried a request
// after a network timeout can tell whether its first attempt landed, which is
// the entire point of supplying the key.
func (a *API) handleEnqueue(w http.ResponseWriter, r *http.Request) int {
	var req enqueueRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return http.StatusBadRequest
	}
	if !ValidQueueName(req.Queue) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue",
			"queue must be non-empty, under 128 chars, and free of ':' and whitespace")
		return http.StatusUnprocessableEntity
	}

	id, created, err := a.store.Enqueue(r.Context(), req.Queue, req.Payload, req.IdempotencyKey)
	if err != nil {
		return a.serverError(w, "enqueue", err)
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
		JobsEnqueued.WithLabelValues(req.Queue).Inc()
	}
	writeJSON(w, status, enqueueResponse{ID: id, Queue: req.Queue, Created: created})
	return status
}

type claimRequest struct {
	Queues   []string `json:"queues"`
	Batch    int      `json:"batch"`
	WaitMS   int      `json:"wait_ms"`
	WorkerID string   `json:"worker_id"`
}

type claimResponse struct {
	Jobs             []Job `json:"jobs"`
	LeaseTTLMS       int64 `json:"lease_ttl_ms"`
	HeartbeatEveryMS int64 `json:"heartbeat_every_ms"`
}

// handleClaim hands jobs to a worker and takes a lease on each.
//
// This is a POST despite reading like a GET, because it mutates state: it
// removes jobs from pending and creates leases. Making it a GET would invite
// proxies and clients to retry or prefetch it, and every such retry would
// silently claim more jobs.
//
// Returns 200 with an empty array rather than 204 when nothing is available.
// Workers poll this endpoint constantly, and one response shape means the
// client never branches on status code before parsing -- the empty list is the
// normal, expected case, not an exception.
func (a *API) handleClaim(w http.ResponseWriter, r *http.Request) int {
	var req claimRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return http.StatusBadRequest
	}
	if len(req.Queues) == 0 {
		writeErr(w, http.StatusUnprocessableEntity, "no_queues", "at least one queue is required")
		return http.StatusUnprocessableEntity
	}
	for _, q := range req.Queues {
		if !ValidQueueName(q) {
			writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "bad queue name: "+q)
			return http.StatusUnprocessableEntity
		}
	}

	wait := time.Duration(req.WaitMS) * time.Millisecond
	if wait > a.cfg.MaxClaimWait {
		wait = a.cfg.MaxClaimWait
	}
	deadline := time.Now().Add(wait)

	for {
		jobs, err := a.store.Claim(r.Context(), req.Queues, req.Batch, req.WorkerID)
		if err != nil {
			return a.serverError(w, "claim", err)
		}
		if len(jobs) > 0 || !time.Now().Before(deadline) {
			for _, j := range jobs {
				JobsClaimed.WithLabelValues(j.Queue).Inc()
			}
			ClaimBatchSize.Observe(float64(len(jobs)))
			writeJSON(w, http.StatusOK, claimResponse{
				Jobs:             jobs,
				LeaseTTLMS:       a.cfg.LeaseTTL.Milliseconds(),
				HeartbeatEveryMS: a.cfg.LeaseTTL.Milliseconds() / int64(a.cfg.HeartbeatDivisor),
			})
			return http.StatusOK
		}
		// Long-poll by polling rather than blocking. BRPOP would block, but it
		// cannot be combined with the lease write in a single atomic step, and
		// atomicity matters more here than saving a few idle round trips. At
		// any real load the queue is non-empty and this loop never runs twice.
		select {
		case <-r.Context().Done():
			return a.clientGone(w)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

type renewResponse struct {
	ID              string `json:"id"`
	LeaseDeadlineMS int64  `json:"lease_deadline_ms"`
}

// handleRenew extends a lease, or refuses.
//
// The 409 is the important case. If this process stalled long enough for its
// lease to expire, the reaper has already handed the job to somebody else.
// Extending the lease then would leave two workers running the same job, each
// believing it holds it exclusively -- a silent correctness failure. Refusing
// turns that into an explicit signal the worker can act on by abandoning its
// work.
func (a *API) handleRenew(w http.ResponseWriter, r *http.Request) int {
	id := r.PathValue("id")
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		var body struct {
			Queue string `json:"queue"`
		}
		_ = decode(r, &body)
		queue = body.Queue
	}
	if !ValidQueueName(queue) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "queue is required")
		return http.StatusUnprocessableEntity
	}

	deadline, err := a.store.Renew(r.Context(), queue, id)
	if errors.Is(err, ErrLeaseGone) {
		LeaseRenewRejected.Inc()
		writeErr(w, http.StatusConflict, "lease_gone",
			"lease expired and the job was re-queued; stop working on it")
		return http.StatusConflict
	}
	if err != nil {
		return a.serverError(w, "renew", err)
	}
	writeJSON(w, http.StatusOK, renewResponse{ID: id, LeaseDeadlineMS: deadline})
	return http.StatusOK
}

type ackBatchRequest struct {
	Queue string   `json:"queue"`
	IDs   []string `json:"ids"`
}

// handleAckBatch completes many jobs in one request. At 10k jobs/sec a
// per-job ack would double the request rate for no benefit.
func (a *API) handleAckBatch(w http.ResponseWriter, r *http.Request) int {
	var req ackBatchRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return http.StatusBadRequest
	}
	if !ValidQueueName(req.Queue) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "queue is required")
		return http.StatusUnprocessableEntity
	}
	n, err := a.store.Ack(r.Context(), req.Queue, req.IDs)
	if err != nil {
		return a.serverError(w, "ack", err)
	}
	JobsAcked.WithLabelValues(req.Queue).Add(float64(n))
	writeJSON(w, http.StatusOK, map[string]int{"acked": n})
	return http.StatusOK
}

func (a *API) handleAck(w http.ResponseWriter, r *http.Request) int {
	id := r.PathValue("id")
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		var body struct {
			Queue string `json:"queue"`
		}
		_ = decode(r, &body)
		queue = body.Queue
	}
	if !ValidQueueName(queue) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "queue is required")
		return http.StatusUnprocessableEntity
	}
	n, err := a.store.Ack(r.Context(), queue, []string{id})
	if err != nil {
		return a.serverError(w, "ack", err)
	}
	JobsAcked.WithLabelValues(queue).Add(float64(n))
	// n == 0 means the lease was already gone -- almost always because this
	// worker overran its lease and the reaper reclaimed the job. Report it so
	// the worker knows its result may be a duplicate.
	if n == 0 {
		writeErr(w, http.StatusConflict, "lease_gone",
			"lease had already expired; this job may have been re-run elsewhere")
		return http.StatusConflict
	}
	writeJSON(w, http.StatusOK, map[string]int{"acked": n})
	return http.StatusOK
}

type failRequest struct {
	Queue   string `json:"queue"`
	Error   string `json:"error"`
	Attempt int    `json:"attempt"`
}

func (a *API) handleFail(w http.ResponseWriter, r *http.Request) int {
	id := r.PathValue("id")
	var req failRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return http.StatusBadRequest
	}
	if !ValidQueueName(req.Queue) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "queue is required")
		return http.StatusUnprocessableEntity
	}

	state, err := a.store.Fail(r.Context(), req.Queue, id, req.Attempt)
	if errors.Is(err, ErrLeaseGone) {
		LeaseRenewRejected.Inc()
		writeErr(w, http.StatusConflict, "lease_gone", "lease had already expired")
		return http.StatusConflict
	}
	if err != nil {
		return a.serverError(w, "fail", err)
	}
	JobsFailed.WithLabelValues(req.Queue).Inc()
	if state == StateDead {
		JobsDead.WithLabelValues(req.Queue, "max_attempts").Inc()
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "state": string(state)})
	return http.StatusOK
}

func (a *API) handleGetJob(w http.ResponseWriter, r *http.Request) int {
	job, err := a.store.Job(r.Context(), r.PathValue("id"))
	if errors.Is(err, ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "no such job")
		return http.StatusNotFound
	}
	if err != nil {
		return a.serverError(w, "get_job", err)
	}
	writeJSON(w, http.StatusOK, job)
	return http.StatusOK
}

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

func (a *API) handleStats(w http.ResponseWriter, r *http.Request) int {
	q := r.PathValue("name")
	if !ValidQueueName(q) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "bad queue name")
		return http.StatusUnprocessableEntity
	}
	stats, err := a.store.Stats(r.Context(), q)
	if err != nil {
		return a.serverError(w, "stats", err)
	}
	writeJSON(w, http.StatusOK, stats)
	return http.StatusOK
}

func (a *API) handleDead(w http.ResponseWriter, r *http.Request) int {
	q := r.URL.Query().Get("queue")
	if !ValidQueueName(q) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "queue query param is required")
		return http.StatusUnprocessableEntity
	}
	limit := int64(100)
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	ids, err := a.store.DeadJobs(r.Context(), q, limit)
	if err != nil {
		return a.serverError(w, "dead", err)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"queue": q, "ids": ids})
	return http.StatusOK
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// decode rejects unknown fields so a typo in a client's JSON is a loud 400
// rather than a silently ignored setting.
func decode(r *http.Request, v interface{}) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (a *API) serverError(w http.ResponseWriter, op string, err error) int {
	a.log.Error("request failed", "op", op, "err", err)
	writeErr(w, http.StatusInternalServerError, "internal", "internal error")
	return http.StatusInternalServerError
}

func (a *API) clientGone(w http.ResponseWriter) int {
	// 499 is nginx's "client closed request". Nothing was written; the metric
	// label is what matters.
	return 499
}
