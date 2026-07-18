package licensing

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLicenseSpool_CrashRecoveryAfterFsync(t *testing.T) {
	dir := t.TempDir()
	spool1, err := OpenLicenseSpool(dir)
	require.NoError(t, err)

	tokenA := "headerA.payloadA.sigA"
	require.NoError(t, spool1.AppendDurably(tokenA))
	pos := spool1.WritePos()

	// Simulate crash mid-record: partial header written without fsync/CRC completion.
	seg := spool1.seg
	copy(seg.mmap[pos:pos+4], licenseSpoolMagic[:])
	require.NoError(t, spool1.Close())

	spool2, err := OpenLicenseSpool(dir)
	require.NoError(t, err)
	defer func() { _ = spool2.Close() }()

	latest, err := spool2.LatestToken()
	require.NoError(t, err)
	assert.Equal(t, tokenA, latest)
}

func TestLicenseSpool_BufferOverflowRejected(t *testing.T) {
	cfg := DefaultLicenseSpoolConfig()
	cfg.SegmentSizeBytes = alignToPageSize(4096)
	cfg.MaxTokenBytes = 3800
	dir := t.TempDir()
	spool, err := OpenLicenseSpoolWithConfig(dir, cfg)
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	big := strings.Repeat("z", 4000)
	err = spool.AppendDurably(big)
	require.ErrorIs(t, err, ErrLicenseTokenTooLarge)

	token := strings.Repeat("t", 3800)
	require.NoError(t, spool.AppendDurably(token))
	err = spool.AppendDurably(token)
	require.ErrorIs(t, err, ErrLicenseSpoolFull)

	logLicensingChaosProof(t, "license_spool_buffer_overflow", map[string]string{
		"subsystem":    "licensing",
		"segment_size": "4096",
		"rejected":     "true",
	})
}

func TestLicenseSpool_PageAlignment(t *testing.T) {
	cfg := LicenseSpoolConfig{SegmentSizeBytes: 5000, MaxTokenBytes: 1024}
	dir := t.TempDir()
	spool, err := OpenLicenseSpoolWithConfig(dir, cfg)
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	page := int64(os.Getpagesize())
	assert.Equal(t, ((5000+page-1)/page)*page, spool.SegmentSize())
}

func TestLicenseSpool_CorruptTailIgnored(t *testing.T) {
	dir := t.TempDir()
	spool, err := OpenLicenseSpool(dir)
	require.NoError(t, err)
	require.NoError(t, spool.AppendDurably("valid.token.here"))
	pos := spool.WritePos()
	require.NoError(t, spool.Close())

	path := filepath.Join(dir, licenseSpoolFileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o640)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, pos)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	spool2, err := OpenLicenseSpool(dir)
	require.NoError(t, err)
	defer func() { _ = spool2.Close() }()

	latest, err := spool2.LatestToken()
	require.NoError(t, err)
	assert.Equal(t, "valid.token.here", latest)
}

func TestDecodeJSONStrict_MalformedAndOversized(t *testing.T) {
	var dst struct {
		PlanCode string `json:"plan_code"`
		Status   string `json:"status"`
	}
	err := DecodeJSONStrict(bytes.NewReader([]byte(`{"plan_code":"basic","status":"active",}`)), 1024, &dst)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrJSONMalformed)

	huge := []byte(`{"plan_code":"` + strings.Repeat("a", 128*1024) + `"}`)
	err = DecodeJSONStrict(bytes.NewReader(huge), 4096, &dst)
	require.ErrorIs(t, err, ErrJSONTooLarge)

	err = DecodeJSONStrict(bytes.NewReader([]byte(`{"plan_code":"basic","status":"active","unknown":1}`)), 1024, &dst)
	require.Error(t, err)
}

func TestChaos_LicenseSpoolConcurrentAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}
	dir := t.TempDir()
	spool, err := OpenLicenseSpool(dir)
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	const workers = 24
	var wg sync.WaitGroup
	wg.Add(workers)
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func(n int) {
			defer wg.Done()
			token := strings.Repeat("c", 20) + ".payload.sig"
			if err := spool.AppendDurably(token); err != nil && !errors.Is(err, ErrLicenseSpoolFull) {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	latest, err := spool.LatestToken()
	require.NoError(t, err)
	require.NotEmpty(t, latest)
	logLicensingChaosProof(t, "license_spool_concurrent_append", map[string]string{
		"subsystem": "licensing",
		"workers":   "24",
		"recovered": "true",
	})
}

func logLicensingChaosProof(t *testing.T, fault string, kv map[string]string) {
	t.Helper()
	msg := "chaos_proof fault=" + fault
	for k, v := range kv {
		msg += " " + k + "=" + v
	}
	t.Log(msg)
}
