package dedupkey

import (
	"crypto/sha256"
	"sort"

	"github.com/google/uuid"
)

// FactorU derives the userspace proof UUID from canonical payload bytes.
func FactorU(payload []byte) uuid.UUID {
	sum := sha256.Sum256(payload)
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

// SpendPair is one campaign amount in a consolidated flush batch.
type SpendPair struct {
	CampaignID  uuid.UUID
	AmountMicro int64
}

// CanonicalSpendPayload serializes sorted (campaign_id, amount) pairs for factor_u.
func CanonicalSpendPayload(pairs []SpendPair) []byte {
	if len(pairs) == 0 {
		return []byte("spend|")
	}
	sorted := append([]SpendPair(nil), pairs...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CampaignID.String() < sorted[j].CampaignID.String()
	})
	buf := make([]byte, 0, len(sorted)*48)
	buf = append(buf, "spend|"...)
	for i, p := range sorted {
		if i > 0 {
			buf = append(buf, ';')
		}
		buf = append(buf, p.CampaignID.String()...)
		buf = append(buf, ':')
		buf = append(buf, fmtInt(p.AmountMicro)...)
	}
	return buf
}

// CanonicalRelayPayload hashes an outbox relay event body.
func CanonicalRelayPayload(outboxEventID int64, eventType string, payload []byte) []byte {
	buf := make([]byte, 0, len(payload)+64)
	buf = append(buf, "relay|"...)
	buf = append(buf, fmtInt(outboxEventID)...)
	buf = append(buf, '|')
	buf = append(buf, eventType...)
	buf = append(buf, '|')
	buf = append(buf, payload...)
	return buf
}

// CanonicalBrokerPayload hashes a broker ingest batch for PG dedup (M4-15).
func CanonicalBrokerPayload(clickIDs []string) []byte {
	if len(clickIDs) == 0 {
		return []byte("broker|")
	}
	sorted := append([]string(nil), clickIDs...)
	sort.Strings(sorted)
	buf := make([]byte, 0, len(sorted)*40)
	buf = append(buf, "broker|"...)
	for i, id := range sorted {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, id...)
	}
	return buf
}

func fmtInt(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
