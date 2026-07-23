package bpf

import (
	"encoding/binary"
	"errors"
	"os"
	"time"

	"github.com/cilium/ebpf/ringbuf"
)

// ViolationHandler processes ringbuf violation events (cold path).
type ViolationHandler struct {
	onEvent func(ViolationEvent) error
}

// NewViolationHandler returns a handler for violation events.
func NewViolationHandler(onEvent func(ViolationEvent) error) *ViolationHandler {
	return &ViolationHandler{onEvent: onEvent}
}

// Drain reads ringbuf records until idle or timeout.
func (h *ViolationHandler) Drain(rd *ringbuf.Reader, idle time.Duration) (int, error) {
	if rd == nil || h.onEvent == nil {
		return 0, nil
	}
	seen := make(map[uint32]struct{})
	deadline := time.Now().Add(idle)
	var handled int

	for time.Now().Before(deadline) {
		rd.SetDeadline(time.Now().Add(1 * time.Millisecond))
		record, err := rd.Read()
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			if isRingbufClosed(err) {
				return handled, nil
			}
			return handled, err
		}
		if len(record.RawSample) < 13 {
			continue
		}
		evt := decodeViolation(record.RawSample)
		if _, dup := seen[evt.SrcIP]; dup {
			continue
		}
		seen[evt.SrcIP] = struct{}{}
		if err := h.onEvent(evt); err != nil {
			return handled, err
		}
		handled++
		deadline = time.Now().Add(idle)
	}
	return handled, nil
}

func decodeViolation(raw []byte) ViolationEvent {
	return ViolationEvent{
		TsNs:   binary.LittleEndian.Uint64(raw[0:8]),
		SrcIP:  binary.LittleEndian.Uint32(raw[8:12]),
		Reason: raw[12],
	}
}

func isRingbufClosed(err error) bool {
	return errors.Is(err, ringbuf.ErrClosed)
}
