package protocol

import (
	"testing"
)

func TestEncodeDecodeProduceBatchResponse(t *testing.T) {
	buf := make([]byte, 64)
	encoded := EncodeProduceBatchResponse(buf, 7, 0, 42, 3)
	if len(encoded) != 31 {
		t.Fatalf("expected wire len 31, got %d", len(encoded))
	}

	payload := encoded[14 : 14+ProduceBatchRespMetaLen]
	status, offset, committed, err := DecodeProduceBatchResponse(payload)
	if err != nil {
		t.Fatal(err)
	}
	if status != 0 || offset != 42 || committed != 3 {
		t.Fatalf("got status=%d offset=%d committed=%d", status, offset, committed)
	}
}
