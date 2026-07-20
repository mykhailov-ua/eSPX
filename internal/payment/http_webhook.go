package payment

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"espx/internal/config"
	"espx/pkg/coldpath"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const stripeSignatureMaxAge = 5 * time.Minute

// WebhookHandler serves Stripe ingress on a dedicated HTTP listener separate from gRPC intent API.
type WebhookHandler struct {
	service *Service
	cfg     *config.Config
	now     func() time.Time
}

// NewWebhookHandler serves Stripe ingress on a dedicated port so webhook volume does not contend with gRPC.
func NewWebhookHandler(service *Service, cfg *config.Config) *WebhookHandler {
	return &WebhookHandler{
		service: service,
		cfg:     cfg,
		now:     time.Now,
	}
}

// RegisterRoutes colocates webhook, health, and metrics on the sidecar mux for a single listen port.
func (h *WebhookHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/webhooks/stripe", h.handleStripeWebhook)
	mux.HandleFunc("/webhooks/crypto", h.handleCryptoWebhook)
	mux.HandleFunc("/health", h.handleHealth)
	mux.Handle("/metrics", promhttp.Handler())
}

// handleHealth gives orchestrators a cheap liveness probe independent of Stripe or Postgres depth.
func (h *WebhookHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// stripeEvent is a minimal Stripe webhook envelope; only fields needed for intent/refund correlation are decoded.
type stripeEvent struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		Object struct {
			ID            string `json:"id"`
			Amount        int64  `json:"amount"`
			PaymentIntent string `json:"payment_intent"`
			Status        string `json:"status"`
		} `json:"object"`
	} `json:"data"`
}

// handleStripeWebhook verifies signatures before persistence because forged events must not move intent state.
func (webhookHandler *WebhookHandler) handleStripeWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	secret := string(webhookHandler.cfg.StripeWebhookSecret)
	if secret == "" {
		slog.Error("stripe webhook secret not configured")
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	body, err := coldpath.ReadLimitedBody(w, r, 64*1024)
	if err != nil {
		slog.Warn("failed to read webhook body", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	sigHeader := r.Header.Get("Stripe-Signature")
	if !verifyStripeSignature(body, sigHeader, secret, webhookHandler.now()) {
		slog.Warn("invalid stripe webhook signature")
		WebhookSignatureFailuresTotal.Inc()
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	event, err := coldpath.DecodeBody[stripeEvent](body)
	if err != nil {
		slog.Warn("failed to unmarshal stripe event", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if event.ID == "" || event.Type == "" {
		slog.Warn("stripe event missing id or type")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	switch event.Type {
	case "refund.created", "refund.updated", "refund.failed":
		webhookHandler.handleStripeRefundEvent(w, r, event, body)
		return
	case "charge.dispute.created", "charge.dispute.updated", "charge.dispute.closed",
		"charge.dispute.funds_withdrawn", "charge.dispute.funds_reinstated":
		webhookHandler.handleStripeDisputeEvent(w, r, event, body)
		return
	}

	providerRef := event.Data.Object.ID
	if providerRef == "" {
		slog.Warn("stripe event missing provider ref object id")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	amountMicro := StripeAmountToMicro(event.Data.Object.Amount)

	err = webhookHandler.service.ProcessStripeWebhook(r.Context(), event.ID, event.Type, body, providerRef, amountMicro, string(body))
	if err != nil {
		slog.Error("failed to process stripe webhook", "event_id", event.ID, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// handleStripeRefundEvent routes Stripe refund lifecycle events to the refund settlement path.
func (webhookHandler *WebhookHandler) handleStripeRefundEvent(w http.ResponseWriter, r *http.Request, event stripeEvent, body []byte) {
	providerRefundID := event.Data.Object.ID
	if providerRefundID == "" {
		slog.Warn("stripe refund event missing refund id")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	paymentIntentRef := event.Data.Object.PaymentIntent
	if paymentIntentRef == "" {
		slog.Warn("stripe refund event missing payment_intent", "event_id", event.ID)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	refundAmountMicro := StripeAmountToMicro(event.Data.Object.Amount)
	refundStatus := event.Data.Object.Status
	if event.Type == "refund.failed" {
		refundStatus = "failed"
	}

	err := webhookHandler.service.ProcessStripeRefundWebhook(
		r.Context(), event.ID, event.Type, body, providerRefundID, paymentIntentRef, refundAmountMicro, refundStatus,
	)
	if err != nil {
		slog.Error("failed to process stripe refund webhook", "event_id", event.ID, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// handleStripeDisputeEvent routes Stripe dispute lifecycle events to the chargeback settlement path.
func (webhookHandler *WebhookHandler) handleStripeDisputeEvent(w http.ResponseWriter, r *http.Request, event stripeEvent, body []byte) {
	providerDisputeID := event.Data.Object.ID
	if providerDisputeID == "" {
		slog.Warn("stripe dispute event missing dispute id")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	paymentIntentRef := event.Data.Object.PaymentIntent
	disputeAmountMicro := StripeAmountToMicro(event.Data.Object.Amount)

	err := webhookHandler.service.ProcessStripeDisputeWebhook(
		r.Context(), event.ID, event.Type, body, providerDisputeID, paymentIntentRef, disputeAmountMicro, event.Data.Object.Status,
	)
	if err != nil {
		slog.Error("failed to process stripe dispute webhook", "event_id", event.ID, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// verifyStripeSignature enforces Stripe timestamp tolerance to block replayed webhook payloads.
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

// cryptoEvent is a normalized crypto webhook payload.
type cryptoEvent struct {
	ID            string `json:"id"`
	Type          string `json:"type"`
	TxHash        string `json:"tx_hash"`
	AmountMicro   int64  `json:"amount_micro"`
	Currency      string `json:"currency"`
	Confirmations int    `json:"confirmations"`
	ProviderRef   string `json:"provider_ref"`
}

// handleCryptoWebhook processes incoming crypto payment notifications.
func (webhookHandler *WebhookHandler) handleCryptoWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	secret := string(webhookHandler.cfg.CryptoWebhookSecret)
	if secret == "" {
		slog.Error("crypto webhook secret not configured")
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	body, err := coldpath.ReadLimitedBody(w, r, 64*1024)
	if err != nil {
		slog.Warn("failed to read crypto webhook body", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	sigHeader := r.Header.Get("Crypto-Signature")
	if !verifyStripeSignature(body, sigHeader, secret, webhookHandler.now()) {
		slog.Warn("invalid crypto webhook signature")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	event, err := coldpath.DecodeBody[cryptoEvent](body)
	if err != nil {
		slog.Warn("failed to unmarshal crypto event", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if event.ID == "" || event.Type == "" {
		slog.Warn("crypto event missing id or type")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	err = webhookHandler.service.ProcessCryptoWebhook(
		r.Context(),
		event.ID,
		event.Type,
		body,
		event.ProviderRef,
		event.AmountMicro,
		event.TxHash,
		event.Confirmations,
	)
	if err != nil {
		slog.Error("failed to process crypto webhook", "event_id", event.ID, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
