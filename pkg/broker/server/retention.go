package server

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"espx/internal/metrics"
	"espx/pkg/broker/log"
)

const defaultRetentionCheckInterval = 5 * time.Minute

// SetRetentionPolicy configures sealed-segment age and byte limits for all topics.
func (s *Server) SetRetentionPolicy(policy log.RetentionPolicy) {
	s.retention = policy
}

// RetentionPolicy returns the configured retention limits.
func (s *Server) RetentionPolicy() log.RetentionPolicy {
	return s.retention
}

// SetRetentionCheckInterval sets how often the background retention worker runs.
func (s *Server) SetRetentionCheckInterval(d time.Duration) {
	if d > 0 {
		s.retentionCheckInterval = d
	}
}

func (s *Server) retentionInterval() time.Duration {
	if s.retentionCheckInterval > 0 {
		return s.retentionCheckInterval
	}
	return defaultRetentionCheckInterval
}

// runRetentionWorker deletes sealed segments on leaders when age or byte limits are exceeded.
func (s *Server) runRetentionWorker() {
	interval := s.retentionInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeChan:
			return
		case <-ticker.C:
			s.runRetentionPass()
		}
	}
}

func (s *Server) runRetentionPass() {
	if !s.retention.Enabled() {
		return
	}

	for _, topic := range s.listTopicNames() {
		if s.coord != nil && !s.coord.IsLeader(topic) {
			continue
		}

		pl, err := s.getOrCreatePartition(topic)
		if err != nil {
			continue
		}

		policy := s.retention
		policy.FloorOffset = s.retentionFloorForTopic(topic)

		result, err := pl.ApplyRetention(policy)
		if err != nil {
			continue
		}

		s.refreshConsumerLagMetrics(topic)

		if result.DeletedSegments > 0 {
			metrics.BrokerRetentionDeletedSegments.Add(float64(result.DeletedSegments))
		}
		metrics.BrokerRetentionDiskUsageBytes.WithLabelValues(topic).Set(float64(result.TotalBytes - result.BytesFreed))
		if result.OldestSegmentAge > 0 {
			metrics.BrokerRetentionOldestSegmentAgeSeconds.WithLabelValues(topic).Set(result.OldestSegmentAge.Seconds())
		}
	}
}

func (s *Server) listTopicNames() []string {
	seen := make(map[string]struct{})

	s.topics.Range(func(key, _ any) bool {
		seen[key.(string)] = struct{}{}
		return true
	})

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return mapKeys(seen)
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		topicName := e.Name()
		subs, subErr := os.ReadDir(filepath.Join(s.dataDir, topicName))
		if subErr == nil && len(subs) > 0 {
			hasPartitionSubdirs := false
			for _, sub := range subs {
				if sub.IsDir() {
					hasPartitionSubdirs = true
					seen[topicName+"/"+sub.Name()] = struct{}{}
				}
			}
			if hasPartitionSubdirs {
				continue
			}
		}
		seen[topicName] = struct{}{}
	}
	return mapKeys(seen)
}

func mapKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
