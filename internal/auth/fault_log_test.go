package auth

import (
	"strings"
	"testing"
)

// logChaosProof emits a structured line CI can grep: chaos_proof fault=... k=v ...
func logChaosProof(t *testing.T, fault string, kv map[string]string) {
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
