package ingestion

import (
	"context"
	"fmt"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"
)

// splitStoreBatch isolates poison-pill rows via binary split instead of per-row inserts.
// Returns global indices into the original batch for successes and failures.
func (consumer *StreamConsumer) splitStoreBatch(ctx context.Context, batch []*campaignmodel.Event, msgIDs []string, baseIdx int) (successIdx, failIdx []int) {
	if len(batch) == 0 {
		return nil, nil
	}

	storeCtx, cancel := context.WithTimeout(ctx, consumer.writeTimeout)
	if len(msgIDs) > 0 {
		token := fmt.Sprintf("%s_%s_%d", msgIDs[0], msgIDs[len(msgIDs)-1], len(msgIDs))
		storeCtx = context.WithValue(storeCtx, campaignmodel.DeduplicationTokenKey, token)
	}
	err := consumer.store.StoreBatch(storeCtx, batch)
	cancel()

	if err == nil {
		successIdx = make([]int, len(batch))
		for i := range batch {
			successIdx[i] = baseIdx + i
		}
		return successIdx, nil
	}

	if isRetriableStoreError(err) {
		for i := range batch {
			failIdx = append(failIdx, baseIdx+i)
		}
		return nil, failIdx
	}

	if len(batch) == 1 {
		metrics.CHSingleRowInsertsTotal.Inc()
		return nil, []int{baseIdx}
	}

	mid := len(batch) / 2
	leftOK, leftFail := consumer.splitStoreBatch(ctx, batch[:mid], msgIDs[:mid], baseIdx)
	rightOK, rightFail := consumer.splitStoreBatch(ctx, batch[mid:], msgIDs[mid:], baseIdx+mid)
	return append(leftOK, rightOK...), append(leftFail, rightFail...)
}
