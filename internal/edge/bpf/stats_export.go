package bpf

import (
	"espx/internal/edge/xdpstats"
	"espx/internal/metrics"

	"github.com/cilium/ebpf"
)

// ExportStatsToPrometheus publishes absolute per-CPU-aggregated counters as Prometheus deltas.
func ExportStatsToPrometheus(m *ebpf.Map, last []uint64) []uint64 {
	totals, err := AggregateStats(m)
	if err != nil || len(totals) != StatMax {
		return last
	}
	if len(last) != StatMax {
		last = make([]uint64, StatMax)
	}
	for idx := uint32(0); idx < StatMax; idx++ {
		delta := totals[idx]
		if last[idx] > 0 {
			delta = totals[idx] - last[idx]
		}
		if delta == 0 {
			continue
		}
		reason := StatReason(idx)
		switch idx {
		case StatPass, StatPassAllowlist:
			metrics.XDPPassTotal.WithLabelValues(reason).Add(float64(delta))
		case StatSynCookie:
			metrics.XDPPassTotal.WithLabelValues(reason).Add(float64(delta))
		case StatFingerprint:
			metrics.XDPFingerprintTotal.Add(float64(delta))
		default:
			metrics.XDPDropTotal.WithLabelValues(reason).Add(float64(delta))
		}
	}
	copy(last, totals)
	return last
}

// BuildSnapshot converts aggregated stats into an operator dashboard snapshot.
func BuildSnapshot(totals []uint64) xdpstats.Snapshot {
	snap := xdpstats.Snapshot{Drops: make(map[string]uint64)}
	if len(totals) != StatMax {
		return snap
	}
	snap.Pass = totals[StatPass]
	snap.PassAllow = totals[StatPassAllowlist]
	snap.Fingerprints = totals[StatFingerprint]
	for idx := uint32(StatDropBlocklist); idx < StatMax; idx++ {
		if idx == StatSynCookie || idx == StatFingerprint {
			continue
		}
		if totals[idx] > 0 {
			snap.Drops[StatReason(idx)] = totals[idx]
		}
	}
	return snap
}
