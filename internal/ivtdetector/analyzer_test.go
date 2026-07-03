package ivtdetector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Guards mergeSuspiciousIPs keeps the highest-scoring reason per IP.
func TestMergeSuspiciousIPs_dedupesByIP(t *testing.T) {
	merged := mergeSuspiciousIPs(
		[]SuspiciousIP{
			{IP: "1.2.3.4", Reason: "ivt_high_click_to_imp_ratio", Score: 4.0},
			{IP: "5.6.7.8", Reason: "ivt_shared_fingerprint_cluster", Score: 8.0},
		},
		[]SuspiciousIP{
			{IP: "1.2.3.4", Reason: "ivt_shared_fingerprint_cluster", Score: 9.0},
		},
	)

	assert.Len(t, merged, 2)
	byIP := make(map[string]SuspiciousIP, len(merged))
	for _, candidate := range merged {
		byIP[candidate.IP] = candidate
	}
	assert.Equal(t, "ivt_shared_fingerprint_cluster", byIP["1.2.3.4"].Reason)
	assert.InDelta(t, 9.0, byIP["1.2.3.4"].Score, 0.001)
	assert.Equal(t, "ivt_shared_fingerprint_cluster", byIP["5.6.7.8"].Reason)
}
