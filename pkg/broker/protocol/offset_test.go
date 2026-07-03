package protocol

import (
	"bytes"
	"testing"
)

func TestCommitOffsetWireRoundtrip(t *testing.T) {
	var writeBuf [256]byte
	req := EncodeCommitOffsetRequest(writeBuf[:], 42, "tracker-logs", 2, "processor-pg", 100)
	payload := req[14 : len(req)-4]

	topic, partition, group, offset, err := DecodeCommitOffsetRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "tracker-logs" || partition != 2 || group != "processor-pg" || offset != 100 {
		t.Fatalf("decode mismatch: %q %d %q %d", topic, partition, group, offset)
	}

	resp := EncodeCommitOffsetResponse(writeBuf[:], 42, 0, 100)
	cmd, seq, respPayload, err := ReadFrame(bytes.NewReader(resp), writeBuf[:], writeBuf[:4])
	if err != nil {
		t.Fatal(err)
	}
	if cmd != CmdCommitOffsetResp || seq != 42 {
		t.Fatalf("cmd=%d seq=%d", cmd, seq)
	}
	status, stored, err := DecodeCommitOffsetResponse(respPayload)
	if err != nil || status != 0 || stored != 100 {
		t.Fatalf("status=%d stored=%d err=%v", status, stored, err)
	}
}

func TestCommittedOffsetWireRoundtrip(t *testing.T) {
	var writeBuf [256]byte
	req := EncodeCommittedOffsetRequest(writeBuf[:], 7, "tracker-logs", 1, "processor-ch")
	payload := req[14 : len(req)-4]

	topic, partition, group, err := DecodeOffsetKeyRequest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "tracker-logs" || partition != 1 || group != "processor-ch" {
		t.Fatalf("got %q %d %q", topic, partition, group)
	}

	resp := EncodeCommittedOffsetResponse(writeBuf[:], 7, 0, 55)
	cmd, seq, respPayload, err := ReadFrame(bytes.NewReader(resp), writeBuf[:], writeBuf[:4])
	if err != nil {
		t.Fatal(err)
	}
	if cmd != CmdCommittedOffsetResp || seq != 7 {
		t.Fatalf("cmd=%d seq=%d", cmd, seq)
	}
	status, offset, err := DecodeCommittedOffsetResponse(respPayload)
	if err != nil || status != 0 || offset != 55 {
		t.Fatalf("status=%d offset=%d err=%v", status, offset, err)
	}
}

func TestCommitOffsetMonotonicDecode(t *testing.T) {
	_, _, _, _, err := DecodeCommitOffsetRequest([]byte{0, 5, 't', 'o', 'p', 'i', 'c'})
	if err == nil {
		t.Fatal("expected truncated commit payload to fail")
	}
}

func TestTopicPartitionIDRoundtrip(t *testing.T) {
	id := TopicPartitionID("tracker-logs", 3)
	topic, part := ParseTopicPartitionID(id)
	if topic != "tracker-logs" || part != 3 {
		t.Fatalf("got %q %d", topic, part)
	}
}
