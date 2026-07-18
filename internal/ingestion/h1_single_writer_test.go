package ingestion

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestH1_UpdateSpendSingleWriter audits production code for forbidden management spend writes.
func TestH1_UpdateSpendSingleWriter(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Join(filepath.Dir(filename), "..", "..")

	var violations []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(rel, "internal/management/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		body := string(data)
		if strings.Contains(body, "UpdateCampaignSpend(") {
			violations = append(violations, rel)
		}
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, violations, "management must not call spend sync writers directly")
}
