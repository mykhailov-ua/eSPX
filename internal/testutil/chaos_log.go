package testutil

import (
	"strings"
	"testing"
)

// LogChaosProof emits a structured line CI can grep: chaos_proof fault=... k=v ...
func LogChaosProof(t testing.TB, fault string, kv map[string]string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("chaos_proof fault=")
	b.WriteString(fault)
	for k, v := range kv {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	t.Log(b.String())
}
