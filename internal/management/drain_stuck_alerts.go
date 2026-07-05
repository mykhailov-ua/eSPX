package management

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type stuckDrainJob struct {
	version   int32
	slot      int16
	state     string
	lastError string
	updatedAt time.Time
}

// CheckStuckDrainJobs alerts operators when slot migration drain jobs exceed the configured threshold.
func (s *Service) CheckStuckDrainJobs(ctx context.Context) {
	if s == nil || s.alerter == nil || s.GetPool() == nil {
		return
	}
	thresholdSec := 900
	if s.cfg != nil && s.cfg.Management.DrainStuckThresholdSec > 0 {
		thresholdSec = s.cfg.Management.DrainStuckThresholdSec
	}
	jobs, err := s.listStuckDrainJobs(ctx, time.Duration(thresholdSec)*time.Second)
	if err != nil {
		slog.Error("failed to list stuck drain jobs", "error", err)
		return
	}
	for _, job := range jobs {
		s.alerter.AlertDrainStuck(job.version, job.slot, job.state, job.lastError, job.updatedAt)
	}
}

func (s *Service) listStuckDrainJobs(ctx context.Context, olderThan time.Duration) ([]stuckDrainJob, error) {
	rows, err := s.GetPool().Query(ctx, `
		SELECT version, slot, state::text, COALESCE(last_error, ''), updated_at
		FROM redis_slot_migration
		WHERE state IN ('draining', 'failed')
		  AND updated_at < NOW() - $1::interval
		ORDER BY updated_at ASC
	`, fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []stuckDrainJob
	for rows.Next() {
		var job stuckDrainJob
		if err := rows.Scan(&job.version, &job.slot, &job.state, &job.lastError, &job.updatedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}
