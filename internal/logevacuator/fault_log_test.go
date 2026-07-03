package logevacuator

import (
	"strings"
	"testing"
)

// logChaosProof emits a structured line CI can grep: chaos_proof fault=... k=v ...
func logChaosProof(t *testing.T, fault string, kv map[string]string) {
	t.Helper()
	var builder strings.Builder
	builder.WriteString("chaos_proof fault=")
	builder.WriteString(fault)
	for key, value := range kv {
		builder.WriteByte(' ')
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(value)
	}
	t.Log(builder.String())
}
