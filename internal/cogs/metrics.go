package cogs

import "github.com/prometheus/client_golang/prometheus"

// claimBuckets are custom on purpose.
//
// prometheus.DefBuckets starts at 5ms and its next stop is 10ms, so with the
// defaults a "p99 claim latency under 5ms" claim would land inside the very
// first bucket and every reported quantile below it would be linear
// interpolation -- a guess, not a measurement.
//
// These buckets put an exact boundary at 0.005s. Prometheus histogram
// quantiles are only ever accurate to a bucket edge, so having the edge sit
// precisely at the number being claimed is what makes "p99 < 5ms" provable
// rather than inferred. The buckets below it give real resolution in the range
// where the answer actually lives.
var claimBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025,
	0.005, // <- the number we claim; exact boundary, deliberately
	0.01, 0.025, 0.05, 0.1, 0.25, 1,
}

var (
	// RequestDuration measures server-side handler latency. This is the
	// authoritative number for the latency claim: it excludes client and
	// network time, and the load generator records its own client-side view
	// separately so the difference can be reported honestly.
	RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cogs",
		Name:      "request_duration_seconds",
		Help:      "Server-side handler latency.",
		Buckets:   claimBuckets,
	}, []string{"endpoint", "status"})

	// ClaimBatchSize records how many jobs each claim actually returned.
	// Throughput is (requests/sec * batch size), so a claim latency figure
	// means nothing without the batch size beside it.
	ClaimBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cogs",
		Name:      "claim_batch_size",
		Help:      "Jobs returned per claim request.",
		Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64},
	})

	JobsEnqueued = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_enqueued_total",
		Help: "Jobs accepted.",
	}, []string{"queue"})

	JobsClaimed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_claimed_total",
		Help: "Jobs handed to workers.",
	}, []string{"queue"})

	JobsAcked = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_acked_total",
		Help: "Jobs completed successfully.",
	}, []string{"queue"})

	JobsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_failed_total",
		Help: "Failures reported by workers.",
	}, []string{"queue"})

	// JobsRequeued counts jobs recovered by the reaper. A non-zero rate here
	// means workers are dying or overrunning their leases; it is the single
	// most useful signal that something is wrong in the fleet.
	JobsRequeued = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_requeued_total",
		Help: "Jobs re-queued by the reaper after a lease expired.",
	}, []string{"queue"})

	JobsRetried = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_retried_total",
		Help: "Jobs returned to pending after backoff.",
	}, []string{"queue"})

	JobsDead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_dead_total",
		Help: "Jobs moved to the dead-letter list.",
	}, []string{"queue", "reason"})

	JobsScheduled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cogs", Name: "jobs_scheduled_total",
		Help: "Jobs enqueued by the cron scheduler.",
	}, []string{"queue"})

	// LeaseRenewRejected counts renewals refused because the lease was already
	// gone. Every increment is a job that was about to be double-processed and
	// wasn't, so this is a correctness signal, not just an error count.
	LeaseRenewRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cogs", Name: "lease_renew_rejected_total",
		Help: "Renewals refused because the lease had already expired.",
	})

	IsLeaderGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cogs", Name: "is_leader",
		Help: "1 if this process currently runs the background loops.",
	})
)

// NewRegistry returns a registry with every Cogs collector registered. A
// dedicated registry rather than the default one keeps test runs isolated from
// each other.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		RequestDuration, ClaimBatchSize,
		JobsEnqueued, JobsClaimed, JobsAcked, JobsFailed,
		JobsRequeued, JobsRetried, JobsDead, JobsScheduled,
		LeaseRenewRejected, IsLeaderGauge,
	)
	return reg
}
