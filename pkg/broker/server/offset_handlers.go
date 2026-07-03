package server

import (
	"context"
	"errors"
	"time"

	"espx/internal/metrics"
	"espx/pkg/broker/protocol"
	"github.com/panjf2000/gnet/v2"
)

func (s *Server) handleCommitOffset(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	topic, partition, group, offset, err := protocol.DecodeCommitOffsetRequest(payload)
	if err != nil {
		resp := protocol.EncodeCommitOffsetResponse(buf, seq, 1, 0)
		_, _ = c.Write(resp)
		return
	}
	tpKey := protocol.TopicPartitionID(topic, partition)

	stored, err := s.commitOffset(tpKey, group, offset)
	if err != nil {
		resp := protocol.EncodeCommitOffsetResponse(buf, seq, 2, 0)
		_, _ = c.Write(resp)
		return
	}

	resp := protocol.EncodeCommitOffsetResponse(buf, seq, 0, stored)
	_, _ = c.Write(resp)
}

func (s *Server) handleCommittedOffset(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	topic, partition, group, err := protocol.DecodeOffsetKeyRequest(payload)
	if err != nil {
		resp := protocol.EncodeCommittedOffsetResponse(buf, seq, 1, 0)
		_, _ = c.Write(resp)
		return
	}
	tpKey := protocol.TopicPartitionID(topic, partition)

	offset, err := s.committedOffset(tpKey, group)
	if err != nil {
		resp := protocol.EncodeCommittedOffsetResponse(buf, seq, 2, 0)
		_, _ = c.Write(resp)
		return
	}

	resp := protocol.EncodeCommittedOffsetResponse(buf, seq, 0, offset)
	_, _ = c.Write(resp)
}

func (s *Server) commitOffset(tpKey, group string, offset uint64) (uint64, error) {
	if s.offsetStore == nil {
		return 0, errOffsetStoreUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), offsetStoreTimeout())
	defer cancel()

	stored, err := s.offsetStore.Commit(ctx, tpKey, group, offset)
	if err != nil {
		return 0, err
	}
	metrics.BrokerConsumerCommitsTotal.WithLabelValues(tpKey, group).Inc()
	s.updateConsumerLag(tpKey, group)
	return stored, nil
}

func (s *Server) committedOffset(tpKey, group string) (uint64, error) {
	if s.offsetStore == nil {
		return 0, errOffsetStoreUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), offsetStoreTimeout())
	defer cancel()
	return s.offsetStore.Committed(ctx, tpKey, group)
}

func (s *Server) retentionFloorForTopic(tpKey string) uint64 {
	if s.offsetStore == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), offsetStoreTimeout())
	defer cancel()
	min, ok, err := s.offsetStore.MinCommitted(ctx, tpKey)
	if err != nil || !ok {
		return 0
	}
	return min
}

func (s *Server) updateConsumerLag(tpKey, group string) {
	pl, err := s.getOrCreatePartition(tpKey)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), offsetStoreTimeout())
	defer cancel()
	committed, err := s.offsetStore.Committed(ctx, tpKey, group)
	if err != nil {
		return
	}
	hwm := pl.NextOffset()
	var lag float64
	if hwm > committed {
		lag = float64(hwm - committed)
	}
	metrics.BrokerConsumerLagMessages.WithLabelValues(tpKey, group).Set(lag)
}

func (s *Server) refreshConsumerLagMetrics(tpKey string) {
	if s.offsetStore == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), offsetStoreTimeout())
	defer cancel()
	groups, err := s.offsetStore.ListGroups(ctx, tpKey)
	if err != nil || len(groups) == 0 {
		return
	}
	pl, err := s.getOrCreatePartition(tpKey)
	if err != nil {
		return
	}
	hwm := pl.NextOffset()
	for group, committed := range groups {
		var lag float64
		if hwm > committed {
			lag = float64(hwm - committed)
		}
		metrics.BrokerConsumerLagMessages.WithLabelValues(tpKey, group).Set(lag)
	}
}

func offsetStoreTimeout() time.Duration {
	return 2 * time.Second
}

var errOffsetStoreUnavailable = errors.New("offset store unavailable")
