package rtb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func chaosWriteBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, data, 0644))
}

func TestChaos_G1_emptySnapshotFile(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "empty.bin")
	chaosWriteBytes(t, path, nil)

	reg := NewRegistry(NewBudgetStore())
	err := reg.LoadSnapshot(path)
	assert.Error(t, err)
	logRtbChaosProof(t, "rtb_empty_snapshot", map[string]string{"rejected": "true"})
}

func TestChaos_G2_wrongMagic(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "badmagic.bin")
	chaosWriteBytes(t, path, []byte("GARBAGE0"))

	reg := NewRegistry(NewBudgetStore())
	err := reg.LoadSnapshot(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid snapshot magic")
	logRtbChaosProof(t, "rtb_wrong_magic", map[string]string{"rejected": "true"})
}

func TestChaos_G3_unsupportedVersion(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "v999.bin")
	data := []byte(snapshotMagic)
	data = append(data, 0xE7, 0x03, 0x00, 0x00) // version 999
	chaosWriteBytes(t, path, data)

	reg := NewRegistry(NewBudgetStore())
	err := reg.LoadSnapshot(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported snapshot version")
	logRtbChaosProof(t, "rtb_unsupported_version", map[string]string{"rejected": "true"})
}

func TestChaos_G4_truncatedAfterHeader(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "trunc.bin")
	data := []byte(snapshotMagic)
	data = append(data, 4, 0, 0, 0) // version 4
	chaosWriteBytes(t, path, data)

	reg := NewRegistry(NewBudgetStore())
	err := reg.LoadSnapshot(path)
	assert.Error(t, err)

	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		_ = reg.LoadSnapshot(path)
	}()
	assert.False(t, panicked)
	logRtbChaosProof(t, "rtb_truncated_snapshot", map[string]string{
		"no_panic": fmt.Sprintf("%v", !panicked),
	})
}

func TestChaos_G5_v4RoundTripP2Fields(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "v4.bin")

	store := NewBudgetStore()
	reg := NewRegistry(store)
	cust := CustomerID(99)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 100, DailyBudget: 500, PacingOpen: PacingOpen,
		CustomerID: cust, CustomerBudget: 2000,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000,
	}})

	require.NoError(t, reg.SaveSnapshot(path))

	newStore := NewBudgetStore()
	newReg := NewRegistry(newStore)
	require.NoError(t, newReg.LoadSnapshot(path))

	sh := newReg.LoadShard(7)
	require.NotNil(t, sh)
	require.Equal(t, 1, sh.Count)
	assert.Equal(t, int64(500), sh.DailyBudgets[0])
	assert.Equal(t, PacingOpen, sh.PacingOpen[0])
	assert.NotEqual(t, invalidCustomerBudgetIdx, sh.CustomerBudgetIndices[0])
	assert.Equal(t, int64(1000), newStore.GetBudget(CampaignID(1)))
	logRtbChaosProof(t, "rtb_v4_roundtrip", map[string]string{"p2_fields": "preserved"})
}
