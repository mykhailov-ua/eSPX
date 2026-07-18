package ingestion

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHSpool_TID05_PartialFlushRecovery verifies mmap WAL survives reopen after simulated crash.
func TestCHSpool_TID05_PartialFlushRecovery(t *testing.T) {
	dir := t.TempDir()

	spool1, err := OpenCHSpool(dir)
	require.NoError(t, err)

	evt := &campaignmodel.Event{
		ClickID:    "tid05-" + uuid.NewString(),
		CampaignID: uuid.New(),
		Type:       "click",
		IP:         "203.0.113.50",
		UA:         "tid05-agent",
		Payload:    []byte(`{"tid":"05"}`),
		CreatedAt:  time.Unix(1_700_000_500, 0).UTC(),
	}
	token := "tid05-token"
	require.NoError(t, spool1.AppendDurably(token, []*campaignmodel.Event{evt}))
	pos := spool1.WritePos()
	require.NoError(t, spool1.Close())

	spool2, err := OpenCHSpool(dir)
	require.NoError(t, err)
	defer func() { _ = spool2.Close() }()

	assert.Equal(t, pos, spool2.WritePos())
	records, err := spool2.Scan()
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, token, records[0].DedupToken)
	require.Len(t, records[0].Events, 1)
	assert.Equal(t, evt.ClickID, records[0].Events[0].ClickID)

	require.NoError(t, spool2.TruncatePrefix(records[0].EndOffset))
	assert.Equal(t, int64(0), spool2.WritePos())
}

func TestCHSpool_CorruptTailIgnored(t *testing.T) {
	dir := t.TempDir()
	spool, err := OpenCHSpool(dir)
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	evt := &campaignmodel.Event{
		ClickID:    "corrupt-" + uuid.NewString(),
		CampaignID: uuid.New(),
		Type:       "impression",
		CreatedAt:  time.Now().UTC(),
	}
	require.NoError(t, spool.AppendDurably("tok", []*campaignmodel.Event{evt}))
	pos := spool.WritePos()

	path := filepath.Join(dir, "events.wal")
	f, err := os.OpenFile(path, os.O_RDWR, 0o640)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, pos)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	spool2, err := OpenCHSpool(dir)
	require.NoError(t, err)
	defer func() { _ = spool2.Close() }()

	records, err := spool2.Scan()
	require.NoError(t, err)
	assert.Len(t, records, 1)
}
