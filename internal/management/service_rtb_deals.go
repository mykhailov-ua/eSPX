package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/rtb"
	"espx/pkg/cold"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrRtbDealNotFound     = errors.New("rtb deal not found")
	ErrInvalidDealPacing   = rtb.ErrInvalidDealPacing
	ErrDuplicateDealID    = errors.New("deal_id already exists")
	ErrDealCustomerMissing = errors.New("customer not found")
	ErrInvalidDealSeats    = errors.New("seats must be at least 1")
)

// RtbDealDTO is the admin API view of one OpenRTB PMP deal.
type RtbDealDTO struct {
	ID         int64  `json:"id"`
	DealID     string `json:"deal_id"`
	FloorMicro int64  `json:"floor_micro"` // bidfloor in micro-units (OpenRTB CPM)
	GeoMask    int64  `json:"geo_mask"`
	CatMask    int64  `json:"cat_mask"`
	Pacing     string `json:"pacing"`
	Seats      int32  `json:"seats"`
	CustomerID string `json:"customer_id"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

// RtbDealCreateSpec is the request body for POST /admin/rtb/deals.
type RtbDealCreateSpec struct {
	DealID     string `json:"deal_id"`
	FloorMicro int64  `json:"floor_micro"`
	GeoMask    int64  `json:"geo_mask"`
	CatMask    int64  `json:"cat_mask"`
	Pacing     string `json:"pacing"`
	Seats      int32  `json:"seats"`
	CustomerID string `json:"customer_id"`
}

// RtbDealUpdateSpec is the request body for PUT /admin/rtb/deals/{id}.
type RtbDealUpdateSpec struct {
	DealID     string `json:"deal_id"`
	FloorMicro int64  `json:"floor_micro"`
	GeoMask    int64  `json:"geo_mask"`
	CatMask    int64  `json:"cat_mask"`
	Pacing     string `json:"pacing"`
	Seats      int32  `json:"seats"`
	CustomerID string `json:"customer_id"`
}

// RtbCatalogReloadPayload is the outbox payload for RELOAD_RTB_CATALOG.
type RtbCatalogReloadPayload struct {
	Trigger string `json:"trigger"`
}

func toRtbDealDTO(r db.RtbDeal) RtbDealDTO {
	return RtbDealDTO{
		ID:         r.ID,
		DealID:     r.DealID,
		FloorMicro: r.FloorMicro,
		GeoMask:    r.GeoMask,
		CatMask:    r.CatMask,
		Pacing:     rtb.DealPacingLabel(r.Pacing),
		Seats:      r.Seats,
		CustomerID: uuid.UUID(r.CustomerID.Bytes).String(),
		CreatedAt:  r.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:  r.UpdatedAt.Time.Format(time.RFC3339),
	}
}

func parseDealCustomerID(raw string) (uuid.UUID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return uuid.Nil, fmt.Errorf("customer_id is required")
	}
	return uuid.Parse(raw)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func normalizeDealSeats(seats int32) (int32, error) {
	if seats < 0 {
		return 0, ErrInvalidDealSeats
	}
	if seats == 0 {
		seats = 1
	}
	return seats, nil
}

func (s *Service) enqueueRtbCatalogReload(ctx context.Context, q db.Querier, trigger string) error {
	payload, err := json.Marshal(RtbCatalogReloadPayload{Trigger: trigger})
	if err != nil {
		return err
	}
	_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "RELOAD_RTB_CATALOG",
		Payload:   payload,
	})
	return err
}

// PublishRtbCatalogReload notifies trackers to rebuild the RTB catalog and deal index.
func (s *Service) PublishRtbCatalogReload(ctx context.Context) error {
	rdb := s.getPubSubRDB()
	if rdb == nil {
		return fmt.Errorf("no redis pubsub client available")
	}
	return rdb.Publish(ctx, ads.RtbCatalogReloadChannel(s.cfg), "reload").Err()
}

// ListRtbDeals returns all PMP deals for admin CRUD.
func (s *Service) ListRtbDeals(ctx context.Context) ([]RtbDealDTO, error) {
	rows, err := db.New(s.GetPool()).ListRtbDeals(ctx)
	if err != nil {
		return nil, err
	}
	return cold.MapSlice(rows, toRtbDealDTO), nil
}

// GetRtbDeal returns one deal by internal id.
func (s *Service) GetRtbDeal(ctx context.Context, id int64) (RtbDealDTO, error) {
	row, err := db.New(s.GetPool()).GetRtbDeal(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RtbDealDTO{}, ErrRtbDealNotFound
		}
		return RtbDealDTO{}, err
	}
	return toRtbDealDTO(row), nil
}

// CreateRtbDeal persists a deal and queues catalog reload propagation.
func (s *Service) CreateRtbDeal(ctx context.Context, spec RtbDealCreateSpec) (RtbDealDTO, error) {
	pacing, err := rtb.ParseDealPacingString(spec.Pacing)
	if err != nil {
		return RtbDealDTO{}, err
	}
	if strings.TrimSpace(spec.DealID) == "" {
		return RtbDealDTO{}, fmt.Errorf("deal_id is required")
	}
	if spec.FloorMicro < 0 {
		return RtbDealDTO{}, fmt.Errorf("floor_micro must be non-negative")
	}
	seats, err := normalizeDealSeats(spec.Seats)
	if err != nil {
		return RtbDealDTO{}, err
	}
	customerID, err := parseDealCustomerID(spec.CustomerID)
	if err != nil {
		return RtbDealDTO{}, err
	}

	var out RtbDealDTO
	err = pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		if _, err := q.GetCustomerByID(ctx, ads.ToUUID(customerID)); err != nil {
			return ErrDealCustomerMissing
		}
		row, err := q.CreateRtbDeal(ctx, db.CreateRtbDealParams{
			DealID:     strings.TrimSpace(spec.DealID),
			FloorMicro: spec.FloorMicro,
			GeoMask:    spec.GeoMask,
			CatMask:    spec.CatMask,
			Pacing:     pacing,
			CustomerID: ads.ToUUID(customerID),
			Seats:      seats,
		})
		if err != nil {
			if isUniqueViolation(err) {
				return ErrDuplicateDealID
			}
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "CREATE_RTB_DEAL", "rtb", nil, map[string]any{
			"deal_id": row.DealID,
		}, nil)

		if err := s.enqueueRtbCatalogReload(ctx, q, "create_rtb_deal"); err != nil {
			return err
		}
		out = toRtbDealDTO(row)
		return nil
	})
	return out, err
}

// UpdateRtbDeal updates a deal and queues catalog reload propagation.
func (s *Service) UpdateRtbDeal(ctx context.Context, id int64, spec RtbDealUpdateSpec) (RtbDealDTO, error) {
	pacing, err := rtb.ParseDealPacingString(spec.Pacing)
	if err != nil {
		return RtbDealDTO{}, err
	}
	if strings.TrimSpace(spec.DealID) == "" {
		return RtbDealDTO{}, fmt.Errorf("deal_id is required")
	}
	if spec.FloorMicro < 0 {
		return RtbDealDTO{}, fmt.Errorf("floor_micro must be non-negative")
	}
	seats, err := normalizeDealSeats(spec.Seats)
	if err != nil {
		return RtbDealDTO{}, err
	}
	customerID, err := parseDealCustomerID(spec.CustomerID)
	if err != nil {
		return RtbDealDTO{}, err
	}

	var out RtbDealDTO
	err = pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		if _, err := q.GetCustomerByID(ctx, ads.ToUUID(customerID)); err != nil {
			return ErrDealCustomerMissing
		}
		row, err := q.UpdateRtbDeal(ctx, db.UpdateRtbDealParams{
			ID:         id,
			DealID:     strings.TrimSpace(spec.DealID),
			FloorMicro: spec.FloorMicro,
			GeoMask:    spec.GeoMask,
			CatMask:    spec.CatMask,
			Pacing:     pacing,
			CustomerID: ads.ToUUID(customerID),
			Seats:      seats,
		})
		if err != nil {
			if isUniqueViolation(err) {
				return ErrDuplicateDealID
			}
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrRtbDealNotFound
			}
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UPDATE_RTB_DEAL", "rtb", nil, map[string]any{
			"id":      id,
			"deal_id": row.DealID,
		}, nil)

		if err := s.enqueueRtbCatalogReload(ctx, q, "update_rtb_deal"); err != nil {
			return err
		}
		out = toRtbDealDTO(row)
		return nil
	})
	return out, err
}

// DeleteRtbDeal removes a deal and queues catalog reload propagation.
func (s *Service) DeleteRtbDeal(ctx context.Context, id int64) error {
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		row, err := q.GetRtbDeal(ctx, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrRtbDealNotFound
			}
			return err
		}
		if err := q.DeleteRtbDeal(ctx, id); err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "DELETE_RTB_DEAL", "rtb", nil, map[string]any{
			"id":      id,
			"deal_id": row.DealID,
		}, nil)
		return s.enqueueRtbCatalogReload(ctx, q, "delete_rtb_deal")
	})
}
