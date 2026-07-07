package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/pkg/cold"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	maxSupplyChainHops   = 10
	sellersJSONCacheTTL  = 60 * time.Second
	sellersJSONVersion   = "1.0"
	supplySettingOwner   = "supply_owner_domain"
	supplySettingManager = "supply_manager_domain"
	supplySettingContact = "supply_contact_email"
)

var (
	ErrSellerNotFound      = errors.New("seller not found")
	ErrAdsTxtEntryNotFound = errors.New("ads.txt entry not found")
	ErrInvalidSellerType   = errors.New("seller_type must be PUBLISHER, INTERMEDIARY, or BOTH")
	ErrInvalidRelationship = errors.New("relationship must be DIRECT or RESELLER")
	ErrSupplyChainTooLong  = fmt.Errorf("supply chain exceeds %d hops", maxSupplyChainHops)
	ErrSellersJSONInvalid  = errors.New("sellers.json schema validation failed")
)

// SupplyChainNode is one hop in an OpenRTB schain (stored on campaigns.supply_chain_nodes).
type SupplyChainNode struct {
	ASI string `json:"asi"`
	SID string `json:"sid"`
	RID string `json:"rid,omitempty"`
	HP  int    `json:"hp"`
}

// SellerDTO is the admin API view of an IAB sellers.json entry.
type SellerDTO struct {
	ID             int64  `json:"id"`
	SellerID       string `json:"seller_id"`
	Domain         string `json:"domain"`
	SellerType     string `json:"seller_type"`
	Name           string `json:"name"`
	IsConfidential bool   `json:"is_confidential"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// SellerCreateSpec is the request body for POST /admin/supply/sellers.
type SellerCreateSpec struct {
	SellerID       string `json:"seller_id"`
	Domain         string `json:"domain"`
	SellerType     string `json:"seller_type"`
	Name           string `json:"name"`
	IsConfidential bool   `json:"is_confidential"`
}

// SellerUpdateSpec is the request body for PUT /admin/supply/sellers/{id}.
type SellerUpdateSpec struct {
	SellerID       string `json:"seller_id"`
	Domain         string `json:"domain"`
	SellerType     string `json:"seller_type"`
	Name           string `json:"name"`
	IsConfidential bool   `json:"is_confidential"`
}

// AdsTxtEntryDTO is the admin API view of one ads.txt line.
type AdsTxtEntryDTO struct {
	ID                 int64  `json:"id"`
	Domain             string `json:"domain"`
	PublisherAccountID string `json:"publisher_account_id"`
	Relationship       string `json:"relationship"`
	CertAuthorityID    string `json:"cert_authority_id,omitempty"`
	SortOrder          int32  `json:"sort_order"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
}

// AdsTxtEntryCreateSpec is the request body for POST /admin/supply/ads-txt.
type AdsTxtEntryCreateSpec struct {
	Domain             string `json:"domain"`
	PublisherAccountID string `json:"publisher_account_id"`
	Relationship       string `json:"relationship"`
	CertAuthorityID    string `json:"cert_authority_id"`
	SortOrder          int32  `json:"sort_order"`
}

// AdsTxtEntryUpdateSpec is the request body for PUT /admin/supply/ads-txt/{id}.
type AdsTxtEntryUpdateSpec struct {
	Domain             string `json:"domain"`
	PublisherAccountID string `json:"publisher_account_id"`
	Relationship       string `json:"relationship"`
	CertAuthorityID    string `json:"cert_authority_id"`
	SortOrder          int32  `json:"sort_order"`
}

// CampaignSupplyChainDTO is the admin API view of campaign schain nodes.
type CampaignSupplyChainDTO struct {
	CampaignID string            `json:"campaign_id"`
	Nodes      []SupplyChainNode `json:"nodes"`
}

// SupplyFilesPayload is the outbox payload for UPDATE_SUPPLY_FILES.
type SupplyFilesPayload struct {
	Trigger string `json:"trigger"`
}

// sellersJSONCacheEntry holds a cached sellers.json body with expiry.
type sellersJSONCacheEntry struct {
	body    []byte
	expires time.Time
}

type sellersJSONCache struct {
	mu sync.RWMutex
	v  sellersJSONCacheEntry
}

var sellersCache sellersJSONCache

func sellerToDTO(r db.Seller) SellerDTO {
	return SellerDTO{
		ID:             r.ID,
		SellerID:       r.SellerID,
		Domain:         r.Domain,
		SellerType:     r.SellerType,
		Name:           r.Name,
		IsConfidential: r.IsConfidential,
		CreatedAt:      r.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:      r.UpdatedAt.Time.Format(time.RFC3339),
	}
}

func adsTxtToDTO(r db.AdsTxtEntry) AdsTxtEntryDTO {
	return AdsTxtEntryDTO{
		ID:                 r.ID,
		Domain:             r.Domain,
		PublisherAccountID: r.PublisherAccountID,
		Relationship:       r.Relationship,
		CertAuthorityID:    r.CertAuthorityID,
		SortOrder:          r.SortOrder,
		CreatedAt:          r.CreatedAt.Time.Format(time.RFC3339),
		UpdatedAt:          r.UpdatedAt.Time.Format(time.RFC3339),
	}
}

func normalizeSellerType(v string) (string, error) {
	v = strings.ToUpper(strings.TrimSpace(v))
	switch v {
	case "PUBLISHER", "INTERMEDIARY", "BOTH":
		return v, nil
	default:
		return "", ErrInvalidSellerType
	}
}

func normalizeRelationship(v string) (string, error) {
	v = strings.ToUpper(strings.TrimSpace(v))
	switch v {
	case "DIRECT", "RESELLER":
		return v, nil
	default:
		return "", ErrInvalidRelationship
	}
}

func validateSupplyChainNodes(nodes []SupplyChainNode) error {
	if len(nodes) > maxSupplyChainHops {
		return ErrSupplyChainTooLong
	}
	for i, n := range nodes {
		if strings.TrimSpace(n.ASI) == "" || strings.TrimSpace(n.SID) == "" {
			return fmt.Errorf("supply chain node %d: asi and sid are required", i)
		}
		if n.HP != 0 && n.HP != 1 {
			return fmt.Errorf("supply chain node %d: hp must be 0 or 1", i)
		}
	}
	return nil
}

func (s *Service) enqueueSupplyFilesUpdate(ctx context.Context, q db.Querier, trigger string) error {
	invalidateSellersJSONCache()
	payload, err := json.Marshal(SupplyFilesPayload{Trigger: trigger})
	if err != nil {
		return err
	}
	_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "UPDATE_SUPPLY_FILES",
		Payload:   payload,
	})
	return err
}

func invalidateSellersJSONCache() {
	sellersCache.mu.Lock()
	sellersCache.v = sellersJSONCacheEntry{}
	sellersCache.mu.Unlock()
}

// ListSellers returns all sellers for admin CRUD.
func (s *Service) ListSellers(ctx context.Context) ([]SellerDTO, error) {
	rows, err := db.New(s.GetPool()).ListSellers(ctx)
	if err != nil {
		return nil, err
	}
	return cold.MapSlice(rows, sellerToDTO), nil
}

// GetSeller returns one seller by internal id.
func (s *Service) GetSeller(ctx context.Context, id int64) (SellerDTO, error) {
	row, err := db.New(s.GetPool()).GetSeller(ctx, id)
	if err != nil {
		return SellerDTO{}, ErrSellerNotFound
	}
	return sellerToDTO(row), nil
}

// CreateSeller persists a seller and queues supply file export.
func (s *Service) CreateSeller(ctx context.Context, spec SellerCreateSpec) (SellerDTO, error) {
	sellerType, err := normalizeSellerType(spec.SellerType)
	if err != nil {
		return SellerDTO{}, err
	}
	if strings.TrimSpace(spec.SellerID) == "" || strings.TrimSpace(spec.Domain) == "" {
		return SellerDTO{}, fmt.Errorf("seller_id and domain are required")
	}

	var out SellerDTO
	err = pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		row, err := q.CreateSeller(ctx, db.CreateSellerParams{
			SellerID:       strings.TrimSpace(spec.SellerID),
			Domain:         strings.TrimSpace(spec.Domain),
			SellerType:     sellerType,
			Name:           strings.TrimSpace(spec.Name),
			IsConfidential: spec.IsConfidential,
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "CREATE_SELLER", "supply", nil, map[string]any{
			"seller_id": row.SellerID,
			"domain":    row.Domain,
		}, nil)

		if err := s.enqueueSupplyFilesUpdate(ctx, q, "create_seller"); err != nil {
			return err
		}
		out = sellerToDTO(row)
		return nil
	})
	return out, err
}

// UpdateSeller updates a seller and queues supply file export.
func (s *Service) UpdateSeller(ctx context.Context, id int64, spec SellerUpdateSpec) (SellerDTO, error) {
	sellerType, err := normalizeSellerType(spec.SellerType)
	if err != nil {
		return SellerDTO{}, err
	}
	if strings.TrimSpace(spec.SellerID) == "" || strings.TrimSpace(spec.Domain) == "" {
		return SellerDTO{}, fmt.Errorf("seller_id and domain are required")
	}

	var out SellerDTO
	err = pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		row, err := q.UpdateSeller(ctx, db.UpdateSellerParams{
			ID:             id,
			SellerID:       strings.TrimSpace(spec.SellerID),
			Domain:         strings.TrimSpace(spec.Domain),
			SellerType:     sellerType,
			Name:           strings.TrimSpace(spec.Name),
			IsConfidential: spec.IsConfidential,
		})
		if err != nil {
			return ErrSellerNotFound
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UPDATE_SELLER", "supply", nil, map[string]any{
			"id":        id,
			"seller_id": row.SellerID,
		}, nil)

		if err := s.enqueueSupplyFilesUpdate(ctx, q, "update_seller"); err != nil {
			return err
		}
		out = sellerToDTO(row)
		return nil
	})
	return out, err
}

// DeleteSeller removes a seller and queues supply file export.
func (s *Service) DeleteSeller(ctx context.Context, id int64) error {
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		if _, err := q.GetSeller(ctx, id); err != nil {
			return ErrSellerNotFound
		}
		if err := q.DeleteSeller(ctx, id); err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "DELETE_SELLER", "supply", nil, map[string]any{"id": id}, nil)
		return s.enqueueSupplyFilesUpdate(ctx, q, "delete_seller")
	})
}

// ListAdsTxtEntries returns all ads.txt lines for admin CRUD.
func (s *Service) ListAdsTxtEntries(ctx context.Context) ([]AdsTxtEntryDTO, error) {
	rows, err := db.New(s.GetPool()).ListAdsTxtEntries(ctx)
	if err != nil {
		return nil, err
	}
	return cold.MapSlice(rows, adsTxtToDTO), nil
}

// GetAdsTxtEntry returns one ads.txt line by id.
func (s *Service) GetAdsTxtEntry(ctx context.Context, id int64) (AdsTxtEntryDTO, error) {
	row, err := db.New(s.GetPool()).GetAdsTxtEntry(ctx, id)
	if err != nil {
		return AdsTxtEntryDTO{}, ErrAdsTxtEntryNotFound
	}
	return adsTxtToDTO(row), nil
}

// CreateAdsTxtEntry persists an ads.txt line and queues supply file export.
func (s *Service) CreateAdsTxtEntry(ctx context.Context, spec AdsTxtEntryCreateSpec) (AdsTxtEntryDTO, error) {
	rel, err := normalizeRelationship(spec.Relationship)
	if err != nil {
		return AdsTxtEntryDTO{}, err
	}
	if strings.TrimSpace(spec.Domain) == "" || strings.TrimSpace(spec.PublisherAccountID) == "" {
		return AdsTxtEntryDTO{}, fmt.Errorf("domain and publisher_account_id are required")
	}

	var out AdsTxtEntryDTO
	err = pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		row, err := q.CreateAdsTxtEntry(ctx, db.CreateAdsTxtEntryParams{
			Domain:             strings.TrimSpace(spec.Domain),
			PublisherAccountID: strings.TrimSpace(spec.PublisherAccountID),
			Relationship:       rel,
			CertAuthorityID:    strings.TrimSpace(spec.CertAuthorityID),
			SortOrder:          spec.SortOrder,
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "CREATE_ADS_TXT", "supply", nil, map[string]any{
			"domain": spec.Domain,
		}, nil)

		if err := s.enqueueSupplyFilesUpdate(ctx, q, "create_ads_txt"); err != nil {
			return err
		}
		out = adsTxtToDTO(row)
		return nil
	})
	return out, err
}

// UpdateAdsTxtEntry updates an ads.txt line and queues supply file export.
func (s *Service) UpdateAdsTxtEntry(ctx context.Context, id int64, spec AdsTxtEntryUpdateSpec) (AdsTxtEntryDTO, error) {
	rel, err := normalizeRelationship(spec.Relationship)
	if err != nil {
		return AdsTxtEntryDTO{}, err
	}
	if strings.TrimSpace(spec.Domain) == "" || strings.TrimSpace(spec.PublisherAccountID) == "" {
		return AdsTxtEntryDTO{}, fmt.Errorf("domain and publisher_account_id are required")
	}

	var out AdsTxtEntryDTO
	err = pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		row, err := q.UpdateAdsTxtEntry(ctx, db.UpdateAdsTxtEntryParams{
			ID:                 id,
			Domain:             strings.TrimSpace(spec.Domain),
			PublisherAccountID: strings.TrimSpace(spec.PublisherAccountID),
			Relationship:       rel,
			CertAuthorityID:    strings.TrimSpace(spec.CertAuthorityID),
			SortOrder:          spec.SortOrder,
		})
		if err != nil {
			return ErrAdsTxtEntryNotFound
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UPDATE_ADS_TXT", "supply", nil, map[string]any{"id": id}, nil)

		if err := s.enqueueSupplyFilesUpdate(ctx, q, "update_ads_txt"); err != nil {
			return err
		}
		out = adsTxtToDTO(row)
		return nil
	})
	return out, err
}

// DeleteAdsTxtEntry removes an ads.txt line and queues supply file export.
func (s *Service) DeleteAdsTxtEntry(ctx context.Context, id int64) error {
	return pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		if _, err := q.GetAdsTxtEntry(ctx, id); err != nil {
			return ErrAdsTxtEntryNotFound
		}
		if err := q.DeleteAdsTxtEntry(ctx, id); err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "DELETE_ADS_TXT", "supply", nil, map[string]any{"id": id}, nil)
		return s.enqueueSupplyFilesUpdate(ctx, q, "delete_ads_txt")
	})
}

// GetCampaignSupplyChain returns schain nodes for a campaign.
func (s *Service) GetCampaignSupplyChain(ctx context.Context, campaignID uuid.UUID) (CampaignSupplyChainDTO, error) {
	row, err := db.New(s.GetPool()).GetCampaignFull(ctx, ads.ToUUID(campaignID))
	if err != nil {
		return CampaignSupplyChainDTO{}, err
	}
	nodes, err := parseSupplyChainNodes(row.SupplyChainNodes)
	if err != nil {
		return CampaignSupplyChainDTO{}, err
	}
	return CampaignSupplyChainDTO{
		CampaignID: campaignID.String(),
		Nodes:      nodes,
	}, nil
}

// UpdateCampaignSupplyChain persists schain nodes (max 10 hops) with audit log.
func (s *Service) UpdateCampaignSupplyChain(ctx context.Context, campaignID uuid.UUID, nodes []SupplyChainNode) (CampaignSupplyChainDTO, error) {
	if err := validateSupplyChainNodes(nodes); err != nil {
		return CampaignSupplyChainDTO{}, err
	}

	nodesJSON, err := json.Marshal(nodes)
	if err != nil {
		return CampaignSupplyChainDTO{}, err
	}

	var out CampaignSupplyChainDTO
	err = pgx.BeginFunc(ctx, s.GetPool(), func(tx pgx.Tx) error {
		q := db.New(tx)
		locked, err := q.GetCampaignForUpdate(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}

		oldNodes, _ := parseSupplyChainNodes(locked.SupplyChainNodes)

		updated, err := q.UpdateCampaignSupplyChain(ctx, db.UpdateCampaignSupplyChainParams{
			ID:               ads.ToUUID(campaignID),
			SupplyChainNodes: nodesJSON,
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UPDATE_CAMPAIGN_SUPPLY_CHAIN", "campaign", &campaignID, map[string]any{
			"old_nodes": oldNodes,
			"new_nodes": nodes,
		}, nil)

		parsed, err := parseSupplyChainNodes(updated.SupplyChainNodes)
		if err != nil {
			return err
		}
		out = CampaignSupplyChainDTO{
			CampaignID: campaignID.String(),
			Nodes:      parsed,
		}
		return nil
	})
	return out, err
}

func parseSupplyChainNodes(raw []byte) ([]SupplyChainNode, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []SupplyChainNode{}, nil
	}
	var nodes []SupplyChainNode
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// iabSellersJSON is the IAB sellers.json Final 2019 root document.
type iabSellersJSON struct {
	ContactEmail string          `json:"contact_email,omitempty"`
	Version      string          `json:"version"`
	Sellers      []iabSellerJSON `json:"sellers"`
}

type iabSellerJSON struct {
	SellerID       string `json:"seller_id"`
	Name           string `json:"name,omitempty"`
	Domain         string `json:"domain"`
	SellerType     string `json:"seller_type"`
	IsConfidential int    `json:"is_confidential,omitempty"`
}

func validateSellersJSON(doc iabSellersJSON) error {
	if strings.TrimSpace(doc.Version) == "" {
		return fmt.Errorf("%w: version required", ErrSellersJSONInvalid)
	}
	if doc.Sellers == nil {
		return fmt.Errorf("%w: sellers array required", ErrSellersJSONInvalid)
	}
	for i, s := range doc.Sellers {
		if strings.TrimSpace(s.SellerID) == "" || strings.TrimSpace(s.Domain) == "" {
			return fmt.Errorf("%w: seller %d missing seller_id or domain", ErrSellersJSONInvalid, i)
		}
		if _, err := normalizeSellerType(s.SellerType); err != nil {
			return fmt.Errorf("%w: seller %d invalid seller_type", ErrSellersJSONInvalid, i)
		}
	}
	return nil
}

// BuildSellersJSON assembles the IAB sellers.json document from Postgres.
func (s *Service) BuildSellersJSON(ctx context.Context) ([]byte, error) {
	q := db.New(s.GetPool())
	rows, err := q.ListSellers(ctx)
	if err != nil {
		return nil, err
	}

	settings, err := q.GetAllSystemSettings(ctx)
	if err != nil {
		return nil, err
	}
	settingsMap := cold.KeyByValue(settings, func(r db.GetAllSystemSettingsRow) string { return r.Key }, func(r db.GetAllSystemSettingsRow) string { return r.Value })

	doc := iabSellersJSON{
		Version: sellersJSONVersion,
		Sellers: make([]iabSellerJSON, 0, len(rows)),
	}
	if email := strings.TrimSpace(settingsMap[supplySettingContact]); email != "" {
		doc.ContactEmail = email
	}

	for _, row := range rows {
		entry := iabSellerJSON{
			SellerID:   row.SellerID,
			Domain:     row.Domain,
			SellerType: row.SellerType,
			Name:       row.Name,
		}
		if row.IsConfidential {
			entry.IsConfidential = 1
		}
		doc.Sellers = append(doc.Sellers, entry)
	}

	if err := validateSellersJSON(doc); err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

// GetSellersJSON returns sellers.json with a 60-second in-memory cache.
func (s *Service) GetSellersJSON(ctx context.Context) ([]byte, error) {
	now := time.Now()
	sellersCache.mu.RLock()
	if len(sellersCache.v.body) > 0 && now.Before(sellersCache.v.expires) {
		body := sellersCache.v.body
		sellersCache.mu.RUnlock()
		return body, nil
	}
	sellersCache.mu.RUnlock()

	body, err := s.BuildSellersJSON(ctx)
	if err != nil {
		return nil, err
	}

	sellersCache.mu.Lock()
	sellersCache.v = sellersJSONCacheEntry{body: body, expires: now.Add(sellersJSONCacheTTL)}
	sellersCache.mu.Unlock()
	return body, nil
}

// BuildAdsTxt assembles ads.txt 1.1 plain text from Postgres.
func (s *Service) BuildAdsTxt(ctx context.Context) (string, error) {
	q := db.New(s.GetPool())
	rows, err := q.ListAdsTxtEntries(ctx)
	if err != nil {
		return "", err
	}
	settings, err := q.GetAllSystemSettings(ctx)
	if err != nil {
		return "", err
	}
	settingsMap := cold.KeyByValue(settings, func(r db.GetAllSystemSettingsRow) string { return r.Key }, func(r db.GetAllSystemSettingsRow) string { return r.Value })

	var b strings.Builder
	if owner := strings.TrimSpace(settingsMap[supplySettingOwner]); owner != "" {
		b.WriteString("OWNERDOMAIN=")
		b.WriteString(owner)
		b.WriteByte('\n')
	}
	if manager := strings.TrimSpace(settingsMap[supplySettingManager]); manager != "" {
		b.WriteString("MANAGERDOMAIN=")
		b.WriteString(manager)
		b.WriteByte('\n')
	}
	if b.Len() > 0 {
		b.WriteByte('\n')
	}

	for _, row := range rows {
		b.WriteString(row.Domain)
		b.WriteString(", ")
		b.WriteString(row.PublisherAccountID)
		b.WriteString(", ")
		b.WriteString(row.Relationship)
		if cert := strings.TrimSpace(row.CertAuthorityID); cert != "" {
			b.WriteString(", ")
			b.WriteString(cert)
		}
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// SupplyExportPath returns the directory for nginx-facing supply file exports.
func (s *Service) SupplyExportPath() string {
	if s.cfg != nil && s.cfg.Management.SupplyExportPath != "" {
		return s.cfg.Management.SupplyExportPath
	}
	return "./data/supply-export"
}
