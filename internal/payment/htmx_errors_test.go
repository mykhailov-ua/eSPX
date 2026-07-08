package payment

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapHTMXError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantMsg    string
	}{
		{
			name:       "no rows",
			err:        pgx.ErrNoRows,
			wantStatus: StatusNotFound,
			wantCode:   CodeNotFound,
			wantMsg:    "Payment resource was not found.",
		},
		{
			name:       "idempotency conflict",
			err:        fmt.Errorf("%w: existing intent has customer=...", ErrIdempotencyConflict),
			wantStatus: StatusConflict,
			wantCode:   CodeConflict,
		},
		{
			name:       "invalid amount",
			err:        errValidation("amount is required"),
			wantStatus: StatusValidation,
			wantCode:   CodeInvalidAmount,
		},
		{
			name:       "provider failure",
			err:        fmt.Errorf("%w: timeout", ErrCheckoutUnavailable),
			wantStatus: StatusUnavailable,
			wantCode:   CodeUnavailable,
		},
		{
			name:       "internal leak blocked",
			err:        errors.New("dial tcp: connection refused"),
			wantStatus: StatusFailed,
			wantCode:   CodeFailed,
			wantMsg:    "Something went wrong processing your payment. Try again or contact support.",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, code, msg := MapHTMXError(tc.err)
			assert.Equal(t, tc.wantStatus, status)
			assert.Equal(t, tc.wantCode, code)
			if tc.wantMsg != "" {
				assert.Equal(t, tc.wantMsg, msg)
			}
			assert.NotContains(t, msg, "dial tcp")
			assert.NotContains(t, msg, "SQLSTATE")
		})
	}
}

func TestWriteHTMX_fragment(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ui/payment/intents", nil)
	req.Header.Set("HX-Request", "true")

	WriteHTMX(rec, req, StatusValidation, CodeInvalidAmount, "Enter a valid payment amount.")

	assert.Equal(t, StatusValidation, rec.Code)
	assert.Equal(t, CodeInvalidAmount, rec.Header().Get("X-Payment-Error-Code"))
	body := rec.Body.String()
	assert.Equal(t, `<div id="payment-error" data-code="PAYMENT_INVALID_AMOUNT">Enter a valid payment amount.</div>`, body)
	assert.NotContains(t, body, "<!DOCTYPE html>")
	assert.NotContains(t, body, "role=")
}

func TestWriteHTMX_nonHTMXSameFragment(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ui/payment/topup", nil)

	WriteHTMX(rec, req, StatusFailed, CodeFailed, "Something went wrong processing your payment. Try again or contact support.")

	assert.Equal(t, StatusFailed, rec.Code)
	body := rec.Body.String()
	assert.Equal(t, `<div id="payment-error" data-code="PAYMENT_FAILED">Something went wrong processing your payment. Try again or contact support.</div>`, body)
	assert.NotContains(t, body, "<!DOCTYPE html>")
}

func TestHTMXHandler_createIntent_invalidCustomer_returns463(t *testing.T) {
	h := NewHTMXHandler(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ui/payment/intents", strings.NewReader("customer_id=not-a-uuid&amount=10"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")

	h.handleCreateIntent(rec, req)

	require.Equal(t, StatusNotFound, rec.Code)
	assert.Equal(t, CodeInvalidCustomer, rec.Header().Get("X-Payment-Error-Code"))
}
