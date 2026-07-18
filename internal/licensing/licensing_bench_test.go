package licensing

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func BenchmarkEffective(b *testing.B) {
	dep := Entitlements{
		Limits:   Limits{MaxRPS: 50000, MaxRequestsPerDay: 10_000_000, MaxActiveCampaigns: 500},
		Features: FeatureSet{RtbLive: true, MlFraudBoost: true},
	}
	cust := Entitlements{
		Limits:   Limits{MaxRPS: 10000, MaxRequestsPerDay: 500_000, MaxActiveCampaigns: 50},
		Features: FeatureSet{RtbLive: false, MlFraudBoost: false},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Effective(dep, cust)
	}
}

func BenchmarkVerifyJWT(b *testing.B) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	claims := LicenseClaims{
		Issuer:       "espx-license",
		Subject:      "lic-bench",
		KeyID:        "2026-01",
		DeploymentID: "dep-bench",
		Plan:         "growth",
		ValidFrom:    time.Now().Add(-time.Hour),
		ValidUntil:   time.Now().Add(24 * time.Hour),
		GraceDays:    7,
	}
	claims.Limits.MaxRPS = 1000
	token := benchJWT(b, priv, claims)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := VerifyJWT(token, pub); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLicenseSpoolAppend(b *testing.B) {
	dir := b.TempDir()
	cfg := DefaultLicenseSpoolConfig()
	cfg.SegmentSizeBytes = alignToPageSize(1024 * 1024)
	spool, err := OpenLicenseSpoolWithConfig(dir, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = spool.Close() }()

	token := strings.Repeat("x", 400) + ".payload.sig"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := spool.AppendDurably(token); err != nil {
			b.Fatal(err)
		}
	}
}

func benchJWT(tb testing.TB, priv ed25519.PrivateKey, claims LicenseClaims) string {
	tb.Helper()
	header := map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": "2026-01"}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		tb.Fatal(err)
	}
	claimsBytes, err := json.Marshal(claims)
	if err != nil {
		tb.Fatal(err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsBytes)
	signingInput := headerB64 + "." + claimsB64
	sig := ed25519.Sign(priv, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}
