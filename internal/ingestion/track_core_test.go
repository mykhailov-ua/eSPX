package ingestion

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/database"
	"github.com/google/uuid"
)

func TestProcessTrack_accepted(t *testing.T) {
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	defer campaignmodel.EventPool.Put(evt)
	evt.CampaignID = uuid.New()
	evt.Type = "click"

	out := processTrack(newTrackProcessor(nil, &mockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusAccepted {
		t.Fatalf("status=%d want accepted", out.Status)
	}
}

func TestProcessTrack_rejected(t *testing.T) {
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	defer campaignmodel.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(NewFilterEngine(0, &errFilter{err: ErrCampaignNotFound}), &mockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusRejected || out.RejectKind != filterRejectCampaignNotFound {
		t.Fatalf("outcome=%+v", out)
	}
}

func TestProcessTrack_fraudAccepted(t *testing.T) {
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	defer campaignmodel.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(NewFilterEngine(0, &errFilter{err: ErrFraudDetected}), &mockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusFraudAccepted || out.RejectKind != filterRejectFraud {
		t.Fatalf("outcome=%+v", out)
	}
}

func TestProcessTrack_shadowAccepted(t *testing.T) {
	geo := &MockGeoProvider{}
	fraud := NewFraudFilter(geo)
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	defer campaignmodel.EventPool.Put(evt)
	evt.CampaignID = uuid.New()
	evt.IP = "1.1.1.66"
	evt.StringBuffer = make([]byte, 0, 64)

	engine := NewFilterEngine(0, fraud)
	engine.SetRegistry(&mockRegistry{})
	out := processTrack(newTrackProcessor(engine, &mockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusAccepted {
		t.Fatalf("status=%d want accepted shadow", out.Status)
	}
	if !evt.ShadowEvent {
		t.Fatal("expected shadow flag")
	}
}

func TestProcessTrack_internalError(t *testing.T) {
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	defer campaignmodel.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(NewFilterEngine(0, &errFilter{err: errors.New("unexpected")}), &mockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusInternalError {
		t.Fatalf("status=%d", out.Status)
	}
}

func TestProcessTrack_infraReject(t *testing.T) {
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	defer campaignmodel.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(NewFilterEngine(0, &errFilter{err: database.ErrRedisCircuitOpen}), &mockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusRejected || out.RejectKind != filterRejectInfra {
		t.Fatalf("outcome=%+v", out)
	}
	if filterRejectSpecs[out.RejectKind].status != http.StatusServiceUnavailable {
		t.Fatal("expected 503 spec")
	}
}

func TestProcessTrack_filterTimeout(t *testing.T) {
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	defer campaignmodel.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(NewFilterEngine(50*time.Millisecond, &slowFilter{delay: 200 * time.Millisecond}), &mockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusRejected || out.RejectKind != filterRejectTimeout {
		t.Fatalf("outcome=%+v", out)
	}
}
