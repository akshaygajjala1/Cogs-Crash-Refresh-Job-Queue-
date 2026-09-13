// Command cogs is the Cogs server: enqueue and lease API, reaper, retry
// drainer, and cron scheduler in one binary.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akshaygajjala1/cogs/internal/cogs"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg := cogs.LoadConfig()

	store := cogs.NewStore(cfg)
	defer store.Close()

	// Fail fast at startup rather than serving 500s: a server that cannot
	// reach Redis can do nothing useful, and an orchestrator restarting it is
	// the right response.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := store.Ping(ctx); err != nil {
		log.Error("cannot reach redis", "addr", cfg.RedisAddr, "err", err)
		cancel()
		os.Exit(1)
	}
	// Push the Lua scripts up front so the hot path uses EVALSHA (a 40-byte
	// hash) rather than shipping every script body on every call.
	if err := store.LoadScripts(ctx); err != nil {
		log.Error("cannot load lua scripts", "err", err)
		cancel()
		os.Exit(1)
	}
	cancel()

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt := cogs.NewRuntime(store, cfg, log)
	rt.Start(runCtx)

	reg := cogs.NewRegistry()
	api := cogs.NewAPI(store, cfg, log, rt)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: api.Routes(promhttp.HandlerFor(reg, promhttp.HandlerOpts{})),
		// ReadHeaderTimeout bounds how long a connection can sit having sent
		// nothing, which is what a slowloris attack relies on.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		// WriteTimeout must exceed the longest legitimate response, and claim
		// long-polls hold the connection open for MaxClaimWait by design.
		WriteTimeout: cfg.MaxClaimWait + 30*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("cogs listening",
			"addr", srv.Addr,
			"redis", cfg.RedisAddr,
			"lease_ttl", cfg.LeaseTTL,
			"reaper_interval", cfg.ReaperInterval,
			// Excludes queue scans, Redis latency, and leader failover delays.
			"nominal_requeue_budget", cfg.LeaseTTL+cfg.ReaperInterval,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-runCtx.Done()
	log.Info("shutting down")

	// Drain in-flight requests before exiting. In-flight *jobs* need no such
	// care: their leases simply expire and the reaper re-queues them, which is
	// the same path a crash takes. Graceful shutdown here is a latency
	// optimisation, not a correctness requirement.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
	log.Info("stopped")
}
