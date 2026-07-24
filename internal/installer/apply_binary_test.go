package installer

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestInstallYAMLRoundTrip(t *testing.T) {
	original := InstallProfile{
		Type:             ProfileComposeDev,
		IngressSchema:    IngressSchemaOpenRTB3,
		TelemetryEnabled: true,
		Tracker: ServiceDeploy{
			Version: "1.0.0",
		},
	}
	data, err := yaml.Marshal(&original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded InstallProfile
	if err := yaml.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	if decoded.IngressSchema != IngressSchemaOpenRTB3 {
		t.Fatalf("ingress_schema = %s, want openrtb_3", decoded.IngressSchema)
	}
	if decoded.Tracker.Version != "1.0.0" {
		t.Fatalf("tracker.version = %s", decoded.Tracker.Version)
	}
}

func TestRenderSecretsIngressSchema(t *testing.T) {
	profile := &InstallProfile{
		Type:          ProfileComposeDev,
		IngressSchema: IngressSchemaESPXNative,
	}
	content := string(renderSecrets(profile))
	if want := "TRACKER_INGRESS_SCHEMA=espx_native"; !containsLine(content, want) {
		t.Fatalf("secrets missing %q:\n%s", want, content)
	}
	if want := "GOGC=300"; !containsLine(content, want) {
		t.Fatalf("secrets missing %q:\n%s", want, content)
	}
}

func TestBinaryDeployBadBinaryRollsBack(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ESPX_INSTALL_ROOT", root)

	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	bad := filepath.Join(dir, "bad")
	target := filepath.Join(dir, "tracker")

	writeProbeScript(t, good, 0)
	writeProbeScript(t, bad, 1)
	if err := copyFile(good, target); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	deploy := &BinaryDeploy{
		Service:    "tracker",
		SourcePath: bad,
		TargetPath: target,
		HealthURL:  "http://127.0.0.1/health",
		Version:    "test",
	}
	err := deploy.DeployBinary()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected deploy failure for bad binary")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("rollback took %v, want < 2s", elapsed)
	}

	if err := RunHealthProbe(target, "http://127.0.0.1/health"); err != nil {
		t.Fatalf("target not restored to good binary: %v", err)
	}
}

func TestRollbackServiceRestoresMarker(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ESPX_INSTALL_ROOT", root)

	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	target := filepath.Join(dir, "tracker")
	writeProbeScript(t, good, 0)
	if err := copyFile(good, target); err != nil {
		t.Fatal(err)
	}
	if _, err := BackupBinary("tracker", target, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("#!/bin/sh\nexit 1\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := RollbackService("tracker", target); err != nil {
		t.Fatal(err)
	}
	if err := RunHealthProbe(target, "http://127.0.0.1/health"); err != nil {
		t.Fatal(err)
	}
}

func writeProbeScript(t *testing.T, path string, exitCode int) {
	t.Helper()
	script := "#!/bin/sh\n"
	if exitCode == 0 {
		script += "url=\"$2\"\n"
		script += "case \"$url\" in http://*) ;; *) exit 1 ;; esac\nexit 0\n"
	} else {
		script += "exit 1\n"
	}
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
}

func containsLine(haystack, line string) bool {
	for _, l := range splitLines(haystack) {
		if l == line {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			out = append(out, line)
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func TestHealthProbeWithHTTPServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	bin := filepath.Join(dir, "probe")
	writeProbeScript(t, bin, 0)
	if err := RunHealthProbe(bin, srv.URL); err != nil {
		t.Fatal(err)
	}
}
