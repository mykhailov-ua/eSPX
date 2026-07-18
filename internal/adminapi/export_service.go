package adminapi

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	JobStatusPending   = "PENDING"
	JobStatusRunning   = "RUNNING"
	JobStatusCompleted = "COMPLETED"
	JobStatusFailed    = "FAILED"
)

// JobSpec is the POST body for async billing export (EXP-02).
type JobSpec struct {
	CustomerID string `json:"customer_id"`
	From       string `json:"from"`
	To         string `json:"to"`
	Format     string `json:"format"`
}

// JobStatusDTO is returned by GET /api/v1/billing/exports/{job_id} (EXP-03).
type JobStatusDTO struct {
	ID          string `json:"id"`
	CustomerID  string `json:"customer_id"`
	Format      string `json:"format"`
	Status      string `json:"status"`
	Bytes       int64  `json:"bytes,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
	Error       string `json:"error,omitempty"`
	CreatedAt   string `json:"created_at"`
	CompletedAt string `json:"completed_at,omitempty"`
}

type jobRecord struct {
	spec        JobSpec
	customerID  uuid.UUID
	from        time.Time
	to          time.Time
	status      string
	bytes       int64
	errMsg      string
	filePath    string
	createdAt   time.Time
	completedAt time.Time
}

// JobRunner runs async billing ledger exports to disk.
type JobRunner struct {
	ledgerReads *CompositeReadService
	exportDir   string
	mu          sync.RWMutex
	jobs        map[string]*jobRecord
}

// NewJobRunner constructs the billing export job runner.
func NewJobRunner(ledgerReads *CompositeReadService, exportDir string) *JobRunner {
	if exportDir == "" {
		exportDir = "./data/billing-export"
	}
	return &JobRunner{
		ledgerReads: ledgerReads,
		exportDir:   exportDir,
		jobs:        make(map[string]*jobRecord),
	}
}

// CreateJob enqueues a ledger export and returns the job id.
func (s *JobRunner) CreateJob(ctx context.Context, spec JobSpec) (string, error) {
	if s == nil || s.ledgerReads == nil {
		return "", fmt.Errorf("export job runner not configured")
	}
	customerID, err := uuid.Parse(spec.CustomerID)
	if err != nil {
		return "", fmt.Errorf("invalid customer_id")
	}
	from, to, err := ParseStatementPeriod(spec.From, spec.To, "")
	if err != nil {
		return "", err
	}
	format := spec.Format
	if format == "" {
		format = "csv"
	}
	if format != "csv" && format != "ndjson" {
		return "", fmt.Errorf("format must be csv or ndjson")
	}

	jobID := uuid.New().String()
	rec := &jobRecord{
		spec:       spec,
		customerID: customerID,
		from:       from,
		to:         to,
		status:     JobStatusPending,
		createdAt:  time.Now().UTC(),
	}
	s.mu.Lock()
	s.jobs[jobID] = rec
	s.mu.Unlock()

	go s.runJob(context.Background(), jobID)
	return jobID, nil
}

// GetJob returns export job status.
func (s *JobRunner) GetJob(jobID string) (JobStatusDTO, bool) {
	s.mu.RLock()
	rec, ok := s.jobs[jobID]
	s.mu.RUnlock()
	if !ok {
		return JobStatusDTO{}, false
	}
	return s.toDTO(jobID, rec), true
}

// OpenDownload opens the completed export file for streaming download.
func (s *JobRunner) OpenDownload(jobID string) (*os.File, JobStatusDTO, error) {
	s.mu.RLock()
	rec, ok := s.jobs[jobID]
	s.mu.RUnlock()
	if !ok {
		return nil, JobStatusDTO{}, fmt.Errorf("job not found")
	}
	if rec.status != JobStatusCompleted || rec.filePath == "" {
		return nil, s.toDTO(jobID, rec), fmt.Errorf("export not ready")
	}
	f, err := os.Open(rec.filePath)
	if err != nil {
		return nil, JobStatusDTO{}, err
	}
	return f, s.toDTO(jobID, rec), nil
}

func (s *JobRunner) toDTO(jobID string, rec *jobRecord) JobStatusDTO {
	dto := JobStatusDTO{
		ID:         jobID,
		CustomerID: rec.customerID.String(),
		Format:     rec.spec.Format,
		Status:     rec.status,
		Bytes:      rec.bytes,
		CreatedAt:  rec.createdAt.UTC().Format(time.RFC3339),
	}
	if rec.status == JobStatusCompleted {
		dto.DownloadURL = "/api/v1/billing/exports/" + jobID + "/download"
	}
	if rec.errMsg != "" {
		dto.Error = rec.errMsg
	}
	if !rec.completedAt.IsZero() {
		dto.CompletedAt = rec.completedAt.UTC().Format(time.RFC3339)
	}
	return dto
}

func (s *JobRunner) runJob(ctx context.Context, jobID string) {
	s.mu.Lock()
	rec, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return
	}
	rec.status = JobStatusRunning
	s.mu.Unlock()

	if err := os.MkdirAll(s.exportDir, 0o755); err != nil {
		s.failJob(jobID, err)
		return
	}

	ext := rec.spec.Format
	if ext == "" {
		ext = "csv"
	}
	filePath := filepath.Join(s.exportDir, jobID+"."+ext)
	var writeErr error
	switch ext {
	case "ndjson":
		writeErr = s.writeNDJSON(ctx, filePath, rec)
	default:
		writeErr = s.writeCSV(ctx, filePath, rec)
	}
	if writeErr != nil {
		s.failJob(jobID, writeErr)
		return
	}

	info, err := os.Stat(filePath)
	if err != nil {
		s.failJob(jobID, err)
		return
	}

	s.mu.Lock()
	if rec, ok := s.jobs[jobID]; ok {
		rec.status = JobStatusCompleted
		rec.filePath = filePath
		rec.bytes = info.Size()
		rec.completedAt = time.Now().UTC()
	}
	s.mu.Unlock()
}

func (s *JobRunner) failJob(jobID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.jobs[jobID]; ok {
		rec.status = JobStatusFailed
		rec.errMsg = err.Error()
		rec.completedAt = time.Now().UTC()
	}
}

func (s *JobRunner) writeCSV(ctx context.Context, path string, rec *jobRecord) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	cw := csv.NewWriter(f)
	if err := cw.Write([]string{"id", "amount_micro", "ledger_type", "created_at"}); err != nil {
		return err
	}

	var cursor int64
	for {
		lines, next, err := s.ledgerReads.ListLedgerLinesInWindow(ctx, rec.customerID, rec.from, rec.to, cursor, 1000)
		if err != nil {
			return err
		}
		for _, line := range lines {
			if err := cw.Write([]string{
				fmt.Sprintf("%d", line.ID),
				fmt.Sprintf("%d", line.AmountMicro),
				line.LedgerType,
				line.CreatedAt,
			}); err != nil {
				return err
			}
		}
		if next == "" {
			break
		}
		var parsed int64
		_, _ = fmt.Sscanf(next, "%d", &parsed)
		cursor = parsed
	}
	cw.Flush()
	return cw.Error()
}

func (s *JobRunner) writeNDJSON(ctx context.Context, path string, rec *jobRecord) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	var cursor int64
	for {
		lines, next, err := s.ledgerReads.ListLedgerLinesInWindow(ctx, rec.customerID, rec.from, rec.to, cursor, 1000)
		if err != nil {
			return err
		}
		for _, line := range lines {
			if err := enc.Encode(line); err != nil {
				return err
			}
		}
		if next == "" {
			break
		}
		var parsed int64
		_, _ = fmt.Sscanf(next, "%d", &parsed)
		cursor = parsed
	}
	return nil
}
