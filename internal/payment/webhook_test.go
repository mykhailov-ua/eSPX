package payment

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestVerifyStripeSignature_timestampWindow(t *testing.T) {
	secret := "whsec_test"
	payload := []byte(`{"id":"evt","type":"payment_intent.succeeded"}`)

	mkSig := func(ts int64) string {
		tsStr := fmt.Sprintf("%d", ts)
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(tsStr + "." + string(payload)))
		return fmt.Sprintf("t=%s,v1=%s", tsStr, hex.EncodeToString(mac.Sum(nil)))
	}

	now := time.Unix(1_700_000_000, 0)

	assertOK := verifyStripeSignature(payload, mkSig(now.Unix()), secret, now)
	if !assertOK {
		t.Fatal("expected valid signature within window")
	}

	old := now.Add(-6 * time.Minute)
	if verifyStripeSignature(payload, mkSig(old.Unix()), secret, now) {
		t.Fatal("expected rejection for expired signature")
	}

	future := now.Add(2 * time.Minute)
	if verifyStripeSignature(payload, mkSig(future.Unix()), secret, now) {
		t.Fatal("expected rejection for future skew beyond 1 minute")
	}
}
