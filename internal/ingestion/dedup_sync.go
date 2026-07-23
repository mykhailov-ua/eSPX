package ingestion

import (
	"context"

	"espx/internal/dedup"
	"espx/pkg/dedupkey"

	"github.com/google/uuid"
)

// SetDedupAdapter wires the D3 v2 claim/confirm adapter (M4-04).
func (w *SyncWorker) SetDedupAdapter(adapter *dedup.Adapter) {
	if w == nil {
		return
	}
	w.dedup = adapter
}

func (w *SyncWorker) resolveSpendDedup(ctx context.Context, item *SpendFlushItem, shardID int16) (apply bool, err error) {
	if w == nil || w.dedup == nil || item == nil {
		return true, nil
	}

	seq := dedupkey.InflightSeq(item.TxID)
	scope := w.dedup.RegionScope(dedupkey.SyncWorkerSourceID(shardID, item.CampaignID), seq, seq)
	factorU := dedupkey.FactorU(dedupkey.CanonicalSpendPayload([]dedupkey.SpendPair{{
		CampaignID:  item.CampaignID,
		AmountMicro: item.AmountMicro,
	}}))

	result, err := w.dedup.ClaimConfirm(ctx, scope, factorU)
	if err != nil {
		return false, err
	}
	if guardErr := dedup.GuardOutcome(result); guardErr != nil {
		return false, guardErr
	}
	item.TxID = result.DedupKey

	if result.Outcome == dedup.OutcomeAlreadyConfirmed {
		resume, resumeErr := w.dedup.NeedsResumeApply(ctx, result.DedupKey)
		if resumeErr != nil {
			return false, resumeErr
		}
		return resume, nil
	}
	return true, nil
}

func (w *SyncWorker) shardForCampaign(id uuid.UUID) int16 {
	sharder := NewStaticSlotSharder(4)
	return int16(sharder.GetShard(id))
}
