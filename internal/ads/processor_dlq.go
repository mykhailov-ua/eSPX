package ads

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"espx/internal/ads/pb"
	redis "github.com/redis/go-redis/v9"
)

var (
	dlqEventPool = sync.Pool{
		New: func() any {
			return new(pb.AdDLQEvent)
		},
	}
	dlqValuesPool = sync.Pool{
		New: func() any {
			slice := make([]any, 2)
			slice[0] = "d"
			return &slice
		},
	}
)

func (p *StreamConsumer) moveToDLQ(ctx context.Context, batch []*Event, msgIDs []string, workerID string, retryCount int, err error) error {
	errStr := err.Error()

	pipeWrite := p.rdb.Pipeline()

	writtenMsgIDs := make([]string, 0, len(batch))
	valuesPtrs := make([]*[]any, 0, len(batch))
	bufPtrs := make([]*[]byte, 0, len(batch))
	wrapPtrs := make([]*ByteSliceValue, 0, len(batch))
	defer func() {
		for _, ptr := range valuesPtrs {
			dlqValuesPool.Put(ptr)
		}
		for _, ptr := range bufPtrs {
			byteBufPool.Put(ptr)
		}
		for _, ptr := range wrapPtrs {
			byteSliceValuePool.Put(ptr)
		}
	}()

	execCtx, execCancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer execCancel()

	for i, e := range batch {
		pbDLQ := dlqEventPool.Get().(*pb.AdDLQEvent)
		if pbDLQ.OriginalEvent == nil {
			pbDLQ.OriginalEvent = new(pb.AdStreamEvent)
		} else {
			DeepResetAdStreamEvent(pbDLQ.OriginalEvent)
		}
		pbDLQ.Error = append(pbDLQ.Error[:0], errStr...)
		pbDLQ.OriginalId = append(pbDLQ.OriginalId[:0], msgIDs[i]...)
		pbDLQ.FailedAtUnix = time.Now().Unix()
		pbDLQ.WorkerId = append(pbDLQ.WorkerId[:0], workerID...)
		pbDLQ.RetryCount = int32(retryCount)

		pbDLQ.OriginalEvent.ClickId = append(pbDLQ.OriginalEvent.ClickId[:0], e.ClickID...)
		pbDLQ.OriginalEvent.CampaignId = append(pbDLQ.OriginalEvent.CampaignId[:0], e.CampaignID[:]...)
		pbDLQ.OriginalEvent.EventType = append(pbDLQ.OriginalEvent.EventType[:0], e.Type...)
		pbDLQ.OriginalEvent.Payload = append(pbDLQ.OriginalEvent.Payload[:0], e.Payload...)
		pbDLQ.OriginalEvent.Ip = append(pbDLQ.OriginalEvent.Ip[:0], e.IP...)
		pbDLQ.OriginalEvent.Ua = append(pbDLQ.OriginalEvent.Ua[:0], e.UA...)
		pbDLQ.OriginalEvent.CreatedAtUnix = e.CreatedAt.Unix()

		size := pbDLQ.SizeVT()
		bufPtr := byteBufPool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < size {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}

		n, marshalErr := pbDLQ.MarshalToSizedBufferVT(buf)
		if marshalErr != nil {
			slog.Error("failed to marshal DLQ event", "error", marshalErr)
			DeepResetAdDLQEvent(pbDLQ)
			dlqEventPool.Put(pbDLQ)
			*bufPtr = buf
			byteBufPool.Put(bufPtr)
			continue
		}

		data := buf[:n]
		*bufPtr = buf
		bufPtrs = append(bufPtrs, bufPtr)

		DeepResetAdDLQEvent(pbDLQ)
		dlqEventPool.Put(pbDLQ)
		writtenMsgIDs = append(writtenMsgIDs, msgIDs[i])

		valuesPtr := dlqValuesPool.Get().(*[]any)
		values := *valuesPtr

		wrap := byteSliceValuePool.Get().(*ByteSliceValue)
		wrap.b = data
		values[1] = wrap
		wrapPtrs = append(wrapPtrs, wrap)

		valuesPtrs = append(valuesPtrs, valuesPtr)

		pipeWrite.XAdd(execCtx, &redis.XAddArgs{
			Stream: "ad:events:dlq",
			MaxLen: 100000,
			Approx: true,
			Values: values,
		})
	}

	if len(writtenMsgIDs) == 0 {
		return nil
	}

	cmders, execErr := pipeWrite.Exec(execCtx)

	var hasError bool
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		slog.Error("DLQ write pipeline returned error", "error", execErr)
		hasError = true
	}

	// Step 2: For every successfully written message, send XAck and XDel to clean up the main stream
	pipeAck := p.rdb.Pipeline()
	ackedMsgIDs := make([]string, 0, len(batch))

	ackCtx, ackCancel := context.WithTimeout(context.Background(), p.writeTimeout)
	defer ackCancel()

	for i, cmder := range cmders {
		if cmder.Err() == nil {
			msgID := writtenMsgIDs[i]
			pipeAck.XAck(ackCtx, p.streamName, p.groupName, msgID)
			pipeAck.XDel(ackCtx, p.streamName, msgID)
			ackedMsgIDs = append(ackedMsgIDs, msgID)
		} else {
			slog.Error("individual DLQ write failed", "error", cmder.Err(), "msgID", writtenMsgIDs[i])
			hasError = true
		}
	}

	if len(ackedMsgIDs) > 0 {
		_, ackErr := pipeAck.Exec(ackCtx)
		if ackErr != nil {
			slog.Error("DLQ ack/del pipeline failed", "error", ackErr)
			return ackErr
		}
	}

	if hasError || len(ackedMsgIDs) < len(writtenMsgIDs) {
		return fmt.Errorf("DLQ write partial failure: wrote %d of %d messages", len(ackedMsgIDs), len(writtenMsgIDs))
	}

	return nil
}
