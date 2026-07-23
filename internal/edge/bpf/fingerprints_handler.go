package bpf

import (
	"encoding/binary"
	"errors"
	"os"
	"time"

	"github.com/cilium/ebpf/ringbuf"
)

// FingerprintHandler processes ringbuf SYN fingerprint events (cold path).
type FingerprintHandler struct {
	onEvent func(FingerprintEvent) error
}

// NewFingerprintHandler returns a handler for fingerprint events.
func NewFingerprintHandler(onEvent func(FingerprintEvent) error) *FingerprintHandler {
	return &FingerprintHandler{onEvent: onEvent}
}

// Drain reads ringbuf records until idle or timeout.
func (h *FingerprintHandler) Drain(rd *ringbuf.Reader, idle time.Duration) (int, error) {
	if rd == nil || h.onEvent == nil {
		return 0, nil
	}
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
		if len(record.RawSample) < 20 {
			continue
		}
		evt := decodeFingerprint(record.RawSample)
		if err := h.onEvent(evt); err != nil {
			return handled, err
		}
		handled++
		deadline = time.Now().Add(idle)
	}
	return handled, nil
}

func decodeFingerprint(raw []byte) FingerprintEvent {
	return FingerprintEvent{
		TsNs:    binary.LittleEndian.Uint64(raw[0:8]),
		SrcIP:   binary.LittleEndian.Uint32(raw[8:12]),
		TCPHash: binary.LittleEndian.Uint32(raw[12:16]),
		Window:  binary.LittleEndian.Uint16(raw[16:18]),
		TTL:     raw[18],
		MSS:     raw[19],
	}
}
