package ingest

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"espx/internal/ads/filter"
	adstest "espx/internal/ads/testutil"
	"espx/internal/database"
	"espx/internal/domain"

	"github.com/google/uuid"
)

func TestProcessTrack_accepted(t *testing.T) {
	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.CampaignID = uuid.New()
	evt.Type = "click"

	out := processTrack(newTrackProcessor(nil, &adstest.MockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusAccepted {
		t.Fatalf("status=%d want accepted", out.Status)
	}
}

func TestProcessTrack_rejected(t *testing.T) {
	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(filter.NewFilterEngine(0, &errFilter{err: filter.ErrCampaignNotFound}), &adstest.MockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusRejected || out.RejectKind != filter.FilterRejectCampaignNotFound {
		t.Fatalf("outcome=%+v", out)
	}
}

func TestProcessTrack_fraudAccepted(t *testing.T) {
	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(filter.NewFilterEngine(0, &errFilter{err: filter.ErrFraudDetected}), &adstest.MockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusFraudAccepted || out.RejectKind != filter.FilterRejectFraud {
		t.Fatalf("outcome=%+v", out)
	}
}

func TestProcessTrack_shadowAccepted(t *testing.T) {
	geo := &filter.MockGeoProvider{}
	fraud := filter.NewFraudFilter(geo)
	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.CampaignID = uuid.New()
	evt.IP = "1.1.1.66"
	evt.StringBuffer = make([]byte, 0, 64)

	engine := filter.NewFilterEngine(0, fraud)
	engine.SetRegistry(&adstest.MockRegistry{})
	out := processTrack(newTrackProcessor(engine, &adstest.MockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusAccepted {
		t.Fatalf("status=%d want accepted shadow", out.Status)
	}
	if !evt.ShadowEvent {
		t.Fatal("expected shadow flag")
	}
}

func TestProcessTrack_internalError(t *testing.T) {
	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(filter.NewFilterEngine(0, &errFilter{err: errors.New("unexpected")}), &adstest.MockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusInternalError {
		t.Fatalf("status=%d", out.Status)
	}
}

func TestProcessTrack_infraReject(t *testing.T) {
	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(filter.NewFilterEngine(0, &errFilter{err: database.ErrRedisCircuitOpen}), &adstest.MockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusRejected || out.RejectKind != filter.FilterRejectInfra {
		t.Fatalf("outcome=%+v", out)
	}
	if filter.FilterRejectSpecs[out.RejectKind].Status != http.StatusServiceUnavailable {
		t.Fatal("expected 503 spec")
	}
}

func TestProcessTrack_filterTimeout(t *testing.T) {
	evt := domain.EventPool.Get().(*domain.Event)
	defer domain.EventPool.Put(evt)
	evt.CampaignID = uuid.New()

	out := processTrack(newTrackProcessor(filter.NewFilterEngine(50*time.Millisecond, &slowFilter{delay: 200 * time.Millisecond}), &adstest.MockRegistry{}, nil), evt, nil)
	if out.Status != trackStatusRejected || out.RejectKind != filter.FilterRejectTimeout {
		t.Fatalf("outcome=%+v", out)
	}
}
