package server

import (
	"time"

	"espx/internal/metrics"
	"espx/pkg/broker/protocol"
	"github.com/panjf2000/gnet/v2"
)

func produceStatusLabel(status byte) string {
	switch status {
	case 0:
		return "ok"
	case 1:
		return "malformed"
	case 2:
		return "partition"
	case 3:
		return "append"
	case 4:
		return "not_leader"
	case 5:
		return "stale_epoch"
	case 6:
		return "catching_up"
	case 7:
		return "overloaded"
	default:
		return "unknown"
	}
}

func recordProduce(topic string, status byte) {
	metrics.BrokerProduceTotal.WithLabelValues(topic, produceStatusLabel(status)).Inc()
}

func recordFetch(topic string) {
	metrics.BrokerFetchTotal.WithLabelValues(topic).Inc()
}

func observeProduceDuration(tpKey string, start time.Time) {
	metrics.BrokerProduceDuration.WithLabelValues(tpKey).Observe(time.Since(start).Seconds())
}

func observeFetchDuration(tpKey string, start time.Time) {
	metrics.BrokerFetchDuration.WithLabelValues(tpKey).Observe(time.Since(start).Seconds())
}

func finishProduce(c gnet.Conn, buf []byte, seq uint64, tpKey string, start time.Time, timed bool, status byte, offset uint64) {
	if timed {
		observeProduceDuration(tpKey, start)
	}
	recordProduce(tpKey, status)
	resp := protocol.EncodeProduceResponse(buf, seq, status, offset)
	_, _ = c.Write(resp)
}

func recordReplicationError(topic, reason string) {
	metrics.BrokerReplicationErrors.WithLabelValues(topic, reason).Inc()
}
