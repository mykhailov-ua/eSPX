package logcompactor

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CheckpointRecord tracks one successfully compacted hot segment.
type CheckpointRecord struct {
	SourceKey     string    `json:"source_key"`
	DestKey       string    `json:"dest_key"`
	SourceSHA256  string    `json:"source_sha256"`
	DestSHA256    string    `json:"dest_sha256"`
	OriginalCount int64     `json:"original_count"`
	KeptCount     int64     `json:"kept_count"`
	CompactedAt   time.Time `json:"compacted_at"`
}

// CheckpointStore persists compaction progress across restarts.
type CheckpointStore struct {
	path string
	mu   sync.Mutex
	bySource map[string]CheckpointRecord
}

// NewCheckpointStore opens or creates the JSON-lines checkpoint file at path.
func NewCheckpointStore(path string) *CheckpointStore {
	return &CheckpointStore{
		path:     path,
		bySource: make(map[string]CheckpointRecord),
	}
}

// Load reads all checkpoint records into memory.
func (store *CheckpointStore) Load() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	data, err := os.ReadFile(store.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	store.bySource = make(map[string]CheckpointRecord)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record CheckpointRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("%w at line %d", ErrCheckpointCorrupt, lineNo)
		}
		if record.SourceKey == "" {
			continue
		}
		store.bySource[record.SourceKey] = record
	}
	return scanner.Err()
}

// IsCompacted reports whether sourceKey with sourceSHA256 was already compacted.
func (store *CheckpointStore) IsCompacted(sourceKey, sourceSHA256 string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	record, ok := store.bySource[sourceKey]
	if !ok {
		return false
	}
	if sourceSHA256 == "" {
		return true
	}
	return record.SourceSHA256 == sourceSHA256
}

// Has reports whether sourceKey was already compacted.
func (store *CheckpointStore) Has(sourceKey string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, ok := store.bySource[sourceKey]
	return ok
}

// Count returns the number of checkpointed source segments.
func (store *CheckpointStore) Count() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.bySource)
}

// Get returns a checkpoint record for sourceKey.
func (store *CheckpointStore) Get(sourceKey string) (CheckpointRecord, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	record, ok := store.bySource[sourceKey]
	return record, ok
}

// Save atomically appends one checkpoint record to disk.
func (store *CheckpointStore) Save(record CheckpointRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(store.path), 0o755); err != nil {
		return err
	}

	line, err := json.Marshal(record)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(store.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}

	store.bySource[record.SourceKey] = record
	return nil
}
