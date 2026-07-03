package server

import (
	"net"
	"os"
	"testing"
	"time"

	"espx/pkg/broker/protocol"
)

func TestProduceBatch_AfterWireRegister_SameConn(t *testing.T) {
	dir, err := os.MkdirTemp("", "batch-same-conn-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	time.Sleep(150 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", s.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writeBuf := make([]byte, 4096)
	readBuf := make([]byte, 4096)
	lenBuf := make([]byte, 4)

	reg := protocol.EncodeRegisterTopicRequest(writeBuf, 1, "batch-topic")
	if _, err := conn.Write(reg); err != nil {
		t.Fatal(err)
	}
	_, _, respPayload, err := protocol.ReadFrame(conn, readBuf, lenBuf)
	if err != nil {
		t.Fatal(err)
	}
	regStatus, topicID, err := protocol.DecodeRegisterTopicResponse(respPayload)
	if err != nil {
		t.Fatal(err)
	}
	if regStatus != 0 {
		t.Fatalf("register status %d", regStatus)
	}

	var batch []byte
	batch = protocol.AppendBatchMessage(batch, topicID, []byte("m0"))
	batch = protocol.AppendBatchMessage(batch, topicID, []byte("m1"))
	batch = protocol.AppendBatchMessage(batch, 9999, []byte("bad-topic"))
	req := protocol.EncodeProduceBatchRequest(writeBuf, 2, batch)
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
	_, _, batchResp, err := protocol.ReadFrame(conn, readBuf, lenBuf)
	if err != nil {
		t.Fatal(err)
	}
	status, offset, committed, err := protocol.DecodeProduceBatchResponse(batchResp)
	if err != nil {
		t.Fatal(err)
	}
	if status != 2 || committed != 2 || offset != 1 {
		t.Fatalf("status=%d committed=%d offset=%d", status, committed, offset)
	}
}

func TestProduceBatch_AfterWireRegister_SeparateConn(t *testing.T) {
	dir, err := os.MkdirTemp("", "batch-wire-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	time.Sleep(150 * time.Millisecond)

	writeBuf := make([]byte, 4096)
	readBuf := make([]byte, 4096)
	lenBuf := make([]byte, 4)

	regConn, err := net.DialTimeout("tcp", s.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reg := protocol.EncodeRegisterTopicRequest(writeBuf, 1, "batch-topic")
	if _, err := regConn.Write(reg); err != nil {
		t.Fatal(err)
	}
	_, _, respPayload, err := protocol.ReadFrame(regConn, readBuf, lenBuf)
	regConn.Close()
	if err != nil {
		t.Fatal(err)
	}
	regStatus, topicID, err := protocol.DecodeRegisterTopicResponse(respPayload)
	if err != nil {
		t.Fatal(err)
	}
	if regStatus != 0 {
		t.Fatalf("register status %d", regStatus)
	}

	batchConn, err := net.DialTimeout("tcp", s.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer batchConn.Close()

	var batch []byte
	batch = protocol.AppendBatchMessage(batch, topicID, []byte("m0"))
	batch = protocol.AppendBatchMessage(batch, topicID, []byte("m1"))
	batch = protocol.AppendBatchMessage(batch, 9999, []byte("bad-topic"))
	req := protocol.EncodeProduceBatchRequest(writeBuf, 2, batch)
	if _, err := batchConn.Write(req); err != nil {
		t.Fatal(err)
	}
	_, _, batchResp, err := protocol.ReadFrame(batchConn, readBuf, lenBuf)
	if err != nil {
		t.Fatal(err)
	}
	status, offset, committed, err := protocol.DecodeProduceBatchResponse(batchResp)
	if err != nil {
		t.Fatal(err)
	}
	if status != 2 || committed != 2 || offset != 1 {
		t.Fatalf("status=%d committed=%d offset=%d", status, committed, offset)
	}
}

func TestProduceBatch_PartialCommitCount(t *testing.T) {
	dir, err := os.MkdirTemp("", "batch-partial-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	time.Sleep(150 * time.Millisecond)

	topicID, err := s.registry.Register("batch-topic")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.DialTimeout("tcp", s.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writeBuf := make([]byte, 4096)
	readBuf := make([]byte, 4096)
	lenBuf := make([]byte, 4)

	var batch []byte
	batch = protocol.AppendBatchMessage(batch, topicID, []byte("m0"))
	batch = protocol.AppendBatchMessage(batch, topicID, []byte("m1"))
	batch = protocol.AppendBatchMessage(batch, 9999, []byte("bad-topic"))

	req := protocol.EncodeProduceBatchRequest(writeBuf, 2, batch)
	if _, err := conn.Write(req); err != nil {
		t.Fatal(err)
	}
	_, _, batchResp, err := protocol.ReadFrame(conn, readBuf, lenBuf)
	if err != nil {
		t.Fatal(err)
	}
	status, offset, committed, err := protocol.DecodeProduceBatchResponse(batchResp)
	if err != nil {
		t.Fatal(err)
	}
	if status != 2 {
		t.Fatalf("expected status 2 (unknown topic), got %d", status)
	}
	if committed != 2 {
		t.Fatalf("expected 2 committed messages before failure, got %d", committed)
	}
	if offset != 1 {
		t.Fatalf("expected last offset 1, got %d", offset)
	}
}

func TestRegisterTopic_WireRoundtrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "batch-register-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	time.Sleep(150 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", s.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	writeBuf := make([]byte, 4096)
	readBuf := make([]byte, 4096)
	lenBuf := make([]byte, 4)

	reg := protocol.EncodeRegisterTopicRequest(writeBuf, 1, "wire-topic")
	if _, err := conn.Write(reg); err != nil {
		t.Fatal(err)
	}
	cmd, _, respPayload, err := protocol.ReadFrame(conn, readBuf, lenBuf)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != protocol.CmdRegisterTopicResp {
		t.Fatalf("expected register response, got cmd %d", cmd)
	}
	regStatus, topicID, err := protocol.DecodeRegisterTopicResponse(respPayload)
	if err != nil {
		t.Fatal(err)
	}
	if regStatus != 0 {
		t.Fatalf("register status %d", regStatus)
	}
	if _, ok := s.registry.Lookup(topicID); !ok {
		t.Fatalf("topic id %d not in server registry after wire register", topicID)
	}
}

func TestLeaderz_StandaloneOK(t *testing.T) {
	dir, err := os.MkdirTemp("", "leaderz-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	s.SetHealthAddr("127.0.0.1:0")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	time.Sleep(150 * time.Millisecond)

	url := "http://" + s.HealthAddr() + "/leaderz?topic=tracker-logs"
	if code := httpGet(t, url); code != 200 {
		t.Fatalf("standalone leaderz: expected 200, got %d", code)
	}
}
