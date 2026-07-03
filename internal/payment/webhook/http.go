package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"espx/internal/config"
	"espx/internal/payment"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const stripeSignatureMaxAge = 5 * time.Minute

type Server struct {
	service *payment.Service
	cfg     *config.Config
	now     func() time.Time
}

// NewServer serves Stripe webhooks and health endpoints on the payment HTTP sidecar.
func NewServer(service *payment.Service, cfg *config.Config) *Server {
	return &Server{
		service: service,
		cfg:     cfg,
		now:     time.Now,
	}
}

// RegisterRoutes mounts webhook, health, and metrics handlers on the shared mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/webhooks/stripe", s.handleStripeWebhook)
	mux.HandleFunc("/health", s.handleHealth)
	mux.Handle("/metrics", promhttp.Handler())
}

// handleHealth answers load-balancer probes without touching payment state.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

type stripeEvent struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		Object struct {
			ID     string `json:"id"`
			Amount int64  `json:"amount"`
		} `json:"object"`
	} `json:"data"`
}

// handleStripeWebhook verifies Stripe HMAC, caps body size, then runs the transactional service layer.
func (s *Server) handleStripeWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	secret := string(s.cfg.StripeWebhookSecret)
	if secret == "" {
		slog.Error("stripe webhook secret not configured")
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Warn("failed to read webhook body", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	sigHeader := r.Header.Get("Stripe-Signature")
	if !verifyStripeSignature(body, sigHeader, secret, s.now()) {
		slog.Warn("invalid stripe webhook signature")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	var event stripeEvent
	if err := json.Unmarshal(body, &event); err != nil {
		slog.Warn("failed to unmarshal stripe event", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if event.ID == "" || event.Type == "" {
		slog.Warn("stripe event missing id or type")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	providerRef := event.Data.Object.ID
	if providerRef == "" {
		slog.Warn("stripe event missing provider ref object id")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	err = s.service.ProcessStripeWebhook(r.Context(), event.ID, event.Type, body, providerRef, event.Data.Object.Amount, string(body))
	if err != nil {
		slog.Error("failed to process stripe webhook", "event_id", event.ID, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// verifyStripeSignature validates the Stripe-Signature header with constant-time MAC compare.
func verifyStripeSignature(payload []byte, sigHeader string, secret string, now time.Time) bool {
	if secret == "" {
		return false
	}
	parts := strings.Split(sigHeader, ",")
	var timestamp string
	var signature string
	for _, part := range parts {
		subparts := strings.SplitN(part, "=", 2)
		if len(subparts) != 2 {
			continue
		}
		key := strings.TrimSpace(subparts[0])
		val := strings.TrimSpace(subparts[1])
		switch key {
		case "t":
			timestamp = val
		case "v1":
			signature = val
		}
	}
	if timestamp == "" || signature == "" {
		return false
	}

	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return false
	}
	eventTime := time.Unix(ts, 0)
	age := now.Sub(eventTime)
	if age > stripeSignatureMaxAge || age < -time.Minute {
		return false
	}

	signedPayload := []byte(timestamp + "." + string(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(signedPayload)
	expectedMAC := mac.Sum(nil)
	expectedSignature := hex.EncodeToString(expectedMAC)

	return subtle.ConstantTimeCompare([]byte(signature), []byte(expectedSignature)) == 1
}
