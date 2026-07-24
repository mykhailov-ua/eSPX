package ingestion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEvaluateRtbLiveGate_insufficientShadow(t *testing.T) {
	ResetRtbShadowDiffBuckets()
	gate := EvaluateRtbLiveGate(time.Hour)
	assert.False(t, gate.Ready)
	assert.Contains(t, gate.Reasons, rtbLiveGateInsufficient)
}

func TestEvaluateRtbLiveGate_parityOk(t *testing.T) {
	ResetRtbShadowDiffBuckets()
	b := &rtbShadowDiffRing[rtbShadowDiffBucketIdx(time.Now())]
	for i := 0; i < 120; i++ {
		b.shadowEvals.Add(1)
		b.parityMatch.Add(1)
		b.shadowWinnerMatch.Add(1)
		b.liveWouldAccept.Add(1)
	}
	gate := EvaluateRtbLiveGate(time.Hour)
	assert.True(t, gate.Ready, gate.Reasons)
	assert.GreaterOrEqual(t, gate.Shadow.ParityRate, rtbLiveGateMinParityRate)
}
