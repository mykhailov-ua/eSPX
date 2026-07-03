package broker

import (
	"testing"
	"time"

	"espx/internal/ads/pb"
	"espx/internal/domain"

	"github.com/google/uuid"
)

func marshalAdLogRecord(t *testing.T, rec *pb.AdLogRecord) []byte {
	t.Helper()
	size := rec.SizeVT()
	buf := make([]byte, size)
	n, err := rec.MarshalToSizedBufferVT(buf)
	if err != nil {
		t.Fatal(err)
	}
	return buf[:n]
}

func marshalAdStreamEvent(t *testing.T, evt *pb.AdStreamEvent) []byte {
	t.Helper()
	size := evt.SizeVT()
	buf := make([]byte, size)
	n, err := evt.MarshalToSizedBufferVT(buf)
	if err != nil {
		t.Fatal(err)
	}
	return buf[:n]
}

func TestParseBrokerPayload_AdLogRecord(t *testing.T) {
	campID := uuid.New()
	rec := &pb.AdLogRecord{
		TimestampUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("click-1"),
		EventType:     []byte("impression"),
		Priority:      1,
	}
	evt, err := ParseBrokerPayload(marshalAdLogRecord(t, rec))
	if err != nil {
		t.Fatal(err)
	}
	defer domain.EventPool.Put(evt)

	if evt.ClickID != "click-1" || evt.Type != "impression" {
		t.Fatalf("unexpected event: %+v", evt)
	}
	if evt.CampaignID != campID {
		t.Fatalf("campaign mismatch: %s", evt.CampaignID)
	}
}

func TestParseBrokerPayload_AdStreamEvent(t *testing.T) {
	campID := uuid.New()
	pbEvt := &pb.AdStreamEvent{
		ClickId:       []byte("click-2"),
		CampaignId:    campID[:],
		EventType:     []byte("click"),
		Payload:       []byte(`{"k":"v"}`),
		Ip:            []byte("1.2.3.4"),
		Ua:            []byte("agent"),
		CreatedAtUnix: time.Now().Unix(),
		FraudScore:    10,
	}
	evt, err := ParseBrokerPayload(marshalAdStreamEvent(t, pbEvt))
	if err != nil {
		t.Fatal(err)
	}
	defer domain.EventPool.Put(evt)

	if evt.ClickID != "click-2" || evt.IP != "1.2.3.4" || evt.FraudScore != 10 {
		t.Fatalf("unexpected event: %+v", evt)
	}
	if string(evt.Payload) != `{"k":"v"}` {
		t.Fatalf("payload: %q", evt.Payload)
	}
}

func TestParseBrokerPayload_Unrecognized(t *testing.T) {
	_, err := ParseBrokerPayload([]byte("not-a-proto"))
	if err != ErrBrokerPayloadUnrecognized {
		t.Fatalf("expected ErrBrokerPayloadUnrecognized, got %v", err)
	}
}
