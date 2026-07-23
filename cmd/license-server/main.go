package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"espx/internal/billing/db"
	"espx/internal/licensing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	privKey ed25519.PrivateKey
}

func main() {
	port := flag.String("port", "8120", "Port to run vendor license server on")
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "Database DSN")
	flag.Parse()

	if *dsn == "" {
		*dsn = "postgres://postgres:postgres@localhost:5432/espx?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer pool.Close()

	// In a real scenario we'd load this from env or a secure vault.
	// For local-dev/tests we'll generate or load.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatalf("Failed to generate signing key: %v", err)
	}

	srv := &Server{
		pool:    pool,
		queries: db.New(pool),
		privKey: priv,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/licenses/issue", srv.handleIssue)
	mux.HandleFunc("/v1/licenses/renew", srv.handleRenew)
	mux.HandleFunc("/v1/licenses/revoke", srv.handleRevoke)
	mux.HandleFunc("/v1/activate", srv.handleActivate)
	mux.HandleFunc("/v1/heartbeat", srv.handleHeartbeat)

	log.Printf("Vendor License Server running on port %s", *port)
	if err := http.ListenAndServe(":"+*port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

type IssueRequest struct {
	LicenseKey   string               `json:"license_key"`
	CustomerName string               `json:"customer_name"`
	Plan         string               `json:"plan"`
	ValidFrom    time.Time            `json:"valid_from"`
	ValidUntil   time.Time            `json:"valid_until"`
	GraceDays    int                  `json:"grace_days"`
	Limits       licensing.Limits     `json:"limits"`
	Features     licensing.FeatureSet `json:"features"`
	SupportTier  string               `json:"support_tier"`
}

func (s *Server) handleIssue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req IssueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	limitsJSON, _ := json.Marshal(req.Limits)
	featuresJSON, _ := json.Marshal(req.Features)

	if req.ValidFrom.IsZero() {
		req.ValidFrom = time.Now()
	}

	_, err := s.queries.InsertVendorLicense(r.Context(), db.InsertVendorLicenseParams{
		LicenseKey:   req.LicenseKey,
		CustomerName: req.CustomerName,
		PlanCode:     req.Plan,
		ValidFrom:    pgtype.Timestamptz{Time: req.ValidFrom, Valid: true},
		ValidUntil:   pgtype.Timestamptz{Time: req.ValidUntil, Valid: true},
		GraceDays:    int32(req.GraceDays),
		LimitsJson:   limitsJSON,
		FeaturesJson: featuresJSON,
		SupportTier:  req.SupportTier,
		Revoked:      false,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "issued"})
}

type RenewRequest struct {
	LicenseKey string    `json:"license_key"`
	ValidUntil time.Time `json:"valid_until"`
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RenewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err := s.queries.RenewVendorLicense(r.Context(), db.RenewVendorLicenseParams{
		LicenseKey: req.LicenseKey,
		ValidUntil: pgtype.Timestamptz{Time: req.ValidUntil, Valid: true},
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = s.queries.RecordVendorRenewalEvent(r.Context(), db.RecordVendorRenewalEventParams{
		LicenseKey:    req.LicenseKey,
		NewValidUntil: pgtype.Timestamptz{Time: req.ValidUntil, Valid: true},
	})

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "renewed"})
}

type RevokeRequest struct {
	LicenseKey string `json:"license_key"`
}

func (s *Server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RevokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := s.queries.RevokeVendorLicense(r.Context(), req.LicenseKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "revoked"})
}

type ActivateRequest struct {
	LicenseKey   string `json:"license_key"`
	DeploymentID string `json:"deployment_id"`
	Fingerprint  string `json:"fingerprint"`
}

func (s *Server) handleActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ActivateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	lic, err := s.queries.GetVendorLicense(r.Context(), req.LicenseKey)
	if err != nil {
		http.Error(w, "License not found", http.StatusNotFound)
		return
	}
	if lic.Revoked {
		http.Error(w, "License is revoked", http.StatusForbidden)
		return
	}

	depID, err := uuid.Parse(req.DeploymentID)
	if err != nil {
		http.Error(w, "Invalid deployment ID format", http.StatusBadRequest)
		return
	}

	_, err = s.queries.UpsertVendorDeployment(r.Context(), db.UpsertVendorDeploymentParams{
		DeploymentID: pgtype.UUID{Bytes: depID, Valid: true},
		LicenseKey:   req.LicenseKey,
		Fingerprint:  req.Fingerprint,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	token, err := s.generateSignedToken(lic, depID, req.Fingerprint)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"token": token})
}

type HeartbeatRequest struct {
	LicenseKey    string `json:"license_key"`
	DeploymentID  string `json:"deployment_id"`
	Fingerprint   string `json:"fingerprint"`
	Version       string `json:"version"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	lic, err := s.queries.GetVendorLicense(r.Context(), req.LicenseKey)
	if err != nil {
		http.Error(w, "License not found", http.StatusNotFound)
		return
	}
	if lic.Revoked {
		http.Error(w, "License is revoked", http.StatusForbidden)
		return
	}

	depID, err := uuid.Parse(req.DeploymentID)
	if err != nil {
		http.Error(w, "Invalid deployment ID format", http.StatusBadRequest)
		return
	}

	_, err = s.queries.UpsertVendorDeployment(r.Context(), db.UpsertVendorDeploymentParams{
		DeploymentID: pgtype.UUID{Bytes: depID, Valid: true},
		LicenseKey:   req.LicenseKey,
		Fingerprint:  req.Fingerprint,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Heuristic: if nothing changed, 304 is fine.
	// For safety, we can issue a fresh signed JWT on every heartbeat.
	token, err := s.generateSignedToken(lic, depID, req.Fingerprint)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"token": token})
}

func (s *Server) generateSignedToken(lic db.VendorLicense, depID uuid.UUID, fingerprint string) (string, error) {
	var limits licensing.Limits
	_ = json.Unmarshal(lic.LimitsJson, &limits)

	var features licensing.FeatureSet
	_ = json.Unmarshal(lic.FeaturesJson, &features)

	claims := licensing.LicenseClaims{
		Issuer:       "espx-license",
		Subject:      uuid.NewString(), // unique license instance token ID
		KeyID:        "2026-01",
		DeploymentID: depID.String(),
		CustomerName: lic.CustomerName,
		Plan:         lic.PlanCode,
		ValidFrom:    lic.ValidFrom.Time,
		ValidUntil:   lic.ValidUntil.Time,
		GraceDays:    int(lic.GraceDays),
		Limits:       limits,
		Features:     features,
		SupportTier:  lic.SupportTier,
	}
	claims.Bind.Mode = "soft"
	claims.Bind.Fingerprint = fingerprint

	header := map[string]string{
		"alg": "EdDSA",
		"typ": "JWT",
		"kid": "2026-01",
	}

	headerBytes, _ := json.Marshal(header)
	claimsBytes, _ := json.Marshal(claims)

	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsBytes)

	signingInput := headerB64 + "." + claimsB64
	sig := ed25519.Sign(s.privKey, []byte(signingInput))
	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signingInput + "." + sigB64, nil
}
