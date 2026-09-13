package cogs

import (
	"os"
	"strconv"
	"time"
)

// Config is read from the environment once at startup.
//
// Short leases reduce recovery delay at the cost of more heartbeat traffic.
// LeaseTTL + ReaperInterval is a nominal recovery budget, not an upper bound:
// queue scans, Redis latency, leader failover, and scheduling delays add time.
type Config struct {
	Port          string
	RedisAddr     string
	RedisPassword string
	AuthToken     string

	LeaseTTL          time.Duration
	ReaperInterval    time.Duration
	RetryInterval     time.Duration
	SchedulerInterval time.Duration
	LeaderLockTTL     time.Duration

	MaxClaimBatch     int
	MaxClaimWait      time.Duration
	HeartbeatDivisor  int
	MaxRetries        int
	BackoffBase       time.Duration
	IdempotencyWindow time.Duration

	MaxBodyBytes int64
	// DrainLimit caps how many due entries one drain pass moves. It bounds the
	// time a single Lua script holds Redis, which is single-threaded: an
	// unbounded scan after a mass worker death would block every other command
	// and spike claim latency for everyone.
	DrainLimit int
}

func LoadConfig() Config {
	return Config{
		Port:          env("PORT", "8080"),
		RedisAddr:     env("REDIS_ADDR", "localhost:6379"),
		RedisPassword: env("REDIS_PASSWORD", ""),
		AuthToken:     env("AUTH_TOKEN", ""),

		LeaseTTL:          time.Duration(envInt("DEFAULT_LEASE_SECONDS", 3)) * time.Second,
		ReaperInterval:    time.Duration(envInt("REAPER_INTERVAL_MS", 500)) * time.Millisecond,
		RetryInterval:     time.Duration(envInt("RETRY_INTERVAL_MS", 500)) * time.Millisecond,
		SchedulerInterval: time.Duration(envInt("SCHEDULER_INTERVAL_MS", 1000)) * time.Millisecond,
		LeaderLockTTL:     time.Duration(envInt("LEADER_LOCK_TTL_MS", 10000)) * time.Millisecond,

		MaxClaimBatch:     envInt("MAX_CLAIM_BATCH", 64),
		MaxClaimWait:      time.Duration(envInt("CLAIM_WAIT_MS", 5000)) * time.Millisecond,
		HeartbeatDivisor:  envInt("HEARTBEAT_DIVISOR", 3),
		MaxRetries:        envInt("MAX_RETRIES", 5),
		BackoffBase:       time.Duration(envInt("BACKOFF_BASE_MS", 1000)) * time.Millisecond,
		IdempotencyWindow: time.Duration(envInt("IDEMPOTENCY_WINDOW_SECONDS", 3600)) * time.Second,

		MaxBodyBytes: int64(envInt("MAX_BODY_BYTES", 1<<20)),
		DrainLimit:   envInt("DRAIN_LIMIT", 500),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
