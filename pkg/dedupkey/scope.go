package dedupkey

import (
	"fmt"

	"github.com/google/uuid"
)

// Scope is the stable logical batch identity (SSID) for D3 v2 dedup keys.
type Scope struct {
	RegionID    uuid.UUID
	SourceID    uuid.UUID
	SourceEpoch uint32
	SeqStart    int64
	SeqEnd      int64
}

// RegionUUID maps a regional cell code to a stable registry UUID.
func RegionUUID(regionCode uint8) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("espx-region:%d", regionCode)))
}

// SyncWorkerSourceID derives a per-shard sync worker source from shard and campaign.
func SyncWorkerSourceID(shardID int16, campaignID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("espx-sync:%d:%s", shardID, campaignID)))
}

// RelaySourceID is the fixed relay lane identity for RegionOutboxRelay.
func RelaySourceID(regionCode uint8) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("espx-relay:%d", regionCode)))
}

// BrokerSourceID identifies a broker topic partition consumer group.
func BrokerSourceID(topic string, partition uint16, group string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("espx-broker:%s:%d:%s", topic, partition, group)))
}

// InflightSeq maps a Redis budget:txid value to a stable sequence slot.
func InflightSeq(txID string) int64 {
	if txID == "" {
		return 0
	}
	u, err := uuid.Parse(txID)
	if err == nil {
		return int64(u.ID())
	}
	h := FactorU([]byte("txid:" + txID))
	return int64(h.ID())
}
