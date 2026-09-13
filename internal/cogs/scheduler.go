package cogs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// Schedule is a recurring job definition.
type Schedule struct {
	ID       string          `json:"id"`
	Queue    string          `json:"queue"`
	Cron     string          `json:"cron"`
	Payload  json.RawMessage `json:"payload"`
	NextRun  int64           `json:"next_run_ms"`
	LastRun  int64           `json:"last_run_ms,omitempty"`
	Disabled bool            `json:"disabled,omitempty"`
}

func scheduleKey(id string) string { return "schedule:" + id }

const schedulesKey = "schedules"

// cronParser accepts standard 5-field cron expressions plus descriptors like
// @hourly. Seconds are deliberately not enabled: allowing a 6-field form would
// let a user register a once-per-second schedule, which is a job firehose
// wearing a cron expression's clothes.
var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// scheduleDrainScript fires every due schedule and reschedules it in ONE
// atomic step per schedule.
//
// The atomicity is the whole point. If enqueueing the job and writing the next
// run time were two calls, a crash in between would leave the schedule still
// due, and the next scheduler pass would fire it again -- users would get
// duplicate nightly reports after every unlucky restart. Because the caller
// cannot know the next cron time from inside Lua, the Go side computes it
// first and passes it in; the script's ZADD is conditional on the schedule
// still being due, so two racing leaders cannot both fire it.
//
// ARGV layout: now, then repeating quads of
//
//	scheduleID, jobID, payload, nextRunMS
//
// Returns the IDs of schedules that actually fired.
var scheduleDrainScript = redis.NewScript(`
local schedulesKey = KEYS[1]
local now = tonumber(ARGV[1])
local fired = {}

local i = 2
while i + 3 <= #ARGV do
  local schedID, jobID, payload, nextRun = ARGV[i], ARGV[i+1], ARGV[i+2], tonumber(ARGV[i+3])
  local score = redis.call('ZSCORE', schedulesKey, schedID)

  -- Re-check due-ness inside the script: another leader may have fired this
  -- schedule between our read and this write.
  if score and tonumber(score) <= now then
    local queue = redis.call('HGET', 'schedule:'..schedID, 'queue')
    if queue then
      redis.call('HSET', 'job:'..jobID,
        'payload', payload, 'queue', queue, 'attempts', 0,
        'idempotency_key', '', 'created_at', now)
      redis.call('LPUSH', 'queue:'..queue..':pending', jobID)
      redis.call('HSET', 'schedule:'..schedID, 'last_run', now)
      redis.call('ZADD', schedulesKey, nextRun, schedID)
      fired[#fired+1] = schedID
      fired[#fired+1] = queue
    end
  end
  i = i + 4
end
return fired
`)

type Scheduler struct {
	store *Store
	cfg   Config
	log   *slog.Logger
}

func NewScheduler(store *Store, cfg Config, log *slog.Logger) *Scheduler {
	return &Scheduler{store: store, cfg: cfg, log: log}
}

// Create registers a recurring job. The cron expression is validated here, at
// write time, so a typo fails the user's API call instead of silently never
// firing.
func (s *Scheduler) Create(ctx context.Context, queue, spec string, payload json.RawMessage) (*Schedule, error) {
	sched, err := cronParser.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}
	id, err := NewID()
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	next := sched.Next(time.Now()).UnixMilli()

	// Execute both writes without other Redis commands interleaving.
	pipe := s.store.rdb.TxPipeline()
	pipe.HSet(ctx, scheduleKey(id),
		"queue", queue, "cron", spec, "payload", string(payload), "created_at", nowMS())
	pipe.ZAdd(ctx, schedulesKey, redis.Z{Score: float64(next), Member: id})
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return &Schedule{ID: id, Queue: queue, Cron: spec, Payload: payload, NextRun: next}, nil
}

func (s *Scheduler) Delete(ctx context.Context, id string) error {
	// Prevent other Redis commands from interleaving between these removals.
	pipe := s.store.rdb.TxPipeline()
	pipe.ZRem(ctx, schedulesKey, id)
	pipe.Del(ctx, scheduleKey(id))
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Scheduler) List(ctx context.Context) ([]Schedule, error) {
	entries, err := s.store.rdb.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Schedule, 0, len(entries))
	for _, e := range entries {
		id, _ := e.Member.(string)
		m, err := s.store.rdb.HGetAll(ctx, scheduleKey(id)).Result()
		if err != nil || len(m) == 0 {
			continue
		}
		out = append(out, Schedule{
			ID:      id,
			Queue:   m["queue"],
			Cron:    m["cron"],
			Payload: json.RawMessage(m["payload"]),
			NextRun: int64(e.Score),
		})
	}
	return out, nil
}

// Pass fires every schedule that is due. Same shape as the reaper and retry
// drainer -- a sorted set scored by a future timestamp -- and driven by the
// same drainDue loop, so it too only runs on the leader.
//
// Note it enqueues one job per due schedule regardless of how many runs were
// missed while the server was down. Firing 400 catch-up jobs after a
// seven-hour outage is almost never what anyone wants from a nightly cleanup;
// skipping to the next scheduled time is.
func (s *Scheduler) Pass(ctx context.Context) error {
	now := nowMS()
	due, err := s.store.rdb.ZRangeByScore(ctx, schedulesKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", now),
		Count: int64(s.cfg.DrainLimit),
	}).Result()
	if err != nil || len(due) == 0 {
		return err
	}

	args := make([]interface{}, 0, 1+len(due)*4)
	args = append(args, now)
	for _, schedID := range due {
		m, err := s.store.rdb.HGetAll(ctx, scheduleKey(schedID)).Result()
		if err != nil || len(m) == 0 {
			// Orphaned sorted-set entry with no hash behind it; drop it rather
			// than retrying forever.
			s.store.rdb.ZRem(ctx, schedulesKey, schedID)
			continue
		}
		parsed, err := cronParser.Parse(m["cron"])
		if err != nil {
			s.log.Error("schedule has an unparseable cron expression; disabling",
				"schedule", schedID, "cron", m["cron"])
			s.store.rdb.ZRem(ctx, schedulesKey, schedID)
			continue
		}
		jobID, err := NewID()
		if err != nil {
			return err
		}
		next := parsed.Next(time.Now()).UnixMilli()
		args = append(args, schedID, jobID, m["payload"], next)
	}
	if len(args) == 1 {
		return nil
	}

	fired, err := scheduleDrainScript.Run(ctx, s.store.rdb, []string{schedulesKey}, args...).Slice()
	if err != nil {
		return err
	}
	for i := 0; i+1 < len(fired); i += 2 {
		queue, _ := fired[i+1].(string)
		JobsScheduled.WithLabelValues(queue).Inc()
	}
	if len(fired) > 0 {
		s.log.Info("fired schedules", "count", len(fired)/2)
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

type createScheduleRequest struct {
	Queue   string          `json:"queue"`
	Cron    string          `json:"cron"`
	Payload json.RawMessage `json:"payload"`
}

func (a *API) handleCreateSchedule(w http.ResponseWriter, r *http.Request) int {
	var req createScheduleRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return http.StatusBadRequest
	}
	if !ValidQueueName(req.Queue) {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_queue", "queue is required")
		return http.StatusUnprocessableEntity
	}
	sched, err := a.rt.sched.Create(r.Context(), req.Queue, req.Cron, req.Payload)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_cron", err.Error())
		return http.StatusUnprocessableEntity
	}
	writeJSON(w, http.StatusCreated, sched)
	return http.StatusCreated
}

func (a *API) handleListSchedules(w http.ResponseWriter, r *http.Request) int {
	out, err := a.rt.sched.List(r.Context())
	if err != nil {
		return a.serverError(w, "list_schedules", err)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"schedules": out})
	return http.StatusOK
}

func (a *API) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) int {
	if err := a.rt.sched.Delete(r.Context(), r.PathValue("id")); err != nil {
		return a.serverError(w, "delete_schedule", err)
	}
	w.WriteHeader(http.StatusNoContent)
	return http.StatusNoContent
}
