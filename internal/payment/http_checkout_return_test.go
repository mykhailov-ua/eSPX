package payment

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTMXHandler_checkoutReturn(t *testing.T) {
	h := NewHTMXHandler(nil)

	t.Run("success", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/ui/payment/return?status=success&session_id=cs_test", nil)
		rr := httptest.NewRecorder()
		h.handleCheckoutReturn(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "Payment submitted")
		assert.Contains(t, rr.Body.String(), "cs_test")
	})

	t.Run("cancelled", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/ui/payment/return?status=cancelled", nil)
		rr := httptest.NewRecorder()
		h.handleCheckoutReturn(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "cancelled")
	})
}

func TestCreateStripeCheckoutSession_requiresURLs(t *testing.T) {
	_, _, err := createStripeCheckoutSession("sk_test_x", "", "", 10_000_000, "USD", nil, "idem-1")
	assert.Error(t, err)
}
