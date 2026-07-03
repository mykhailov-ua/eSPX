package protocol

import (
	"encoding/binary"
	"testing"
)

func TestEncodeDecodeFetchResponseMeta(t *testing.T) {
	buf := make([]byte, 64)
	header := EncodeFetchResponseHeader(buf, 99, 0, 3, 128, 42)
	if len(header) != 27 {
		t.Fatalf("expected header len 27, got %d", len(header))
	}

	payload := append([]byte(nil), header[14:]...)
	payload = append(payload, make([]byte, 128)...)

	status, count, hwm, err := DecodeFetchResponseMeta(payload[:FetchRespMetaLen])
	if err != nil {
		t.Fatal(err)
	}
	if status != 0 || count != 3 || hwm != 42 {
		t.Fatalf("meta mismatch: status=%d count=%d hwm=%d", status, count, hwm)
	}

	totalLen := uint32(2 + 8 + FetchRespMetaLen + 128 + 4)
	gotLen := binary.BigEndian.Uint32(header[0:4])
	if gotLen != totalLen {
		t.Fatalf("frame totalLen: got %d want %d", gotLen, totalLen)
	}
}

func TestDecodeFetchResponseMetaTooShort(t *testing.T) {
	_, _, _, err := DecodeFetchResponseMeta([]byte{0, 1})
	if err == nil {
		t.Fatal("expected error for short payload")
	}
}
