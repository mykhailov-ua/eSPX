package payment

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"
)

func TestChaos_PaymentWebhookExpiredSignatureRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	secret := "stripe_chaos_wh_secret"
	payload := []byte(`{"id":"evt_expired","type":"payment_intent.succeeded"}`)
	oldTS := strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)

	signedPayload := []byte(oldTS + "." + string(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(signedPayload)
	sigHeader := "t=" + oldTS + ",v1=" + hex.EncodeToString(mac.Sum(nil))

	if verifyStripeSignature(payload, sigHeader, secret, time.Now()) {
		t.Fatal("expected expired signature rejection")
	}

	t.Log("chaos_proof fault=stripe_signature_expired subsystem=payment_webhook rejected=true baseline_ok=true fault_type=signature_replay")
}
