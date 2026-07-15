package fraudscoring

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func runAtModuleRoot(t *testing.T, name string, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = moduleRoot(t)
	return cmd.CombinedOutput()
}

func TestTrackerDepGraphExcludesFraudScoringRuntime(t *testing.T) {
	out, err := runAtModuleRoot(t, "go", "list", "-deps", "./cmd/tracker")
	require.NoError(t, err, string(out))

	deps := string(out)
	assert.NotContains(t, deps, "espx/internal/fraudscoring")
	assert.NotContains(t, deps, "github.com/zhongdai/go-lgbm")
	assert.NotContains(t, deps, "onnxruntime")
}

func TestFraudScoringPackageBuilds(t *testing.T) {
	out, err := runAtModuleRoot(t, "go", "test", "-c", "-o", os.DevNull, "./internal/fraudscoring")
	require.NoError(t, err, "fraudscoring build failed: %s", string(out))
}

func TestTrackerBuildsWithoutFraudScoring(t *testing.T) {
	out, err := runAtModuleRoot(t, "go", "build", "-o", os.DevNull, "./cmd/tracker")
	require.NoError(t, err, "tracker build failed: %s", strings.TrimSpace(string(out)))
}
