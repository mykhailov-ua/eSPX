package dedupkey

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const versionPrefix = "v2"

// FormatCanonical builds the dedup_key string stored in sync_idempotency.
func FormatCanonical(scope Scope, factorU, factorD uuid.UUID) string {
	return fmt.Sprintf("%s|%s|%s|%d|%d|%d|%s|%s",
		versionPrefix,
		scope.RegionID,
		scope.SourceID,
		scope.SourceEpoch,
		scope.SeqStart,
		scope.SeqEnd,
		factorU,
		factorD,
	)
}

// ParseCanonical splits a dedup_key into scope and factors.
func ParseCanonical(key string) (Scope, uuid.UUID, uuid.UUID, error) {
	parts := strings.Split(key, "|")
	if len(parts) != 8 || parts[0] != versionPrefix {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, errors.New("dedupkey: invalid canonical format")
	}
	regionID, err := uuid.Parse(parts[1])
	if err != nil {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, err
	}
	sourceID, err := uuid.Parse(parts[2])
	if err != nil {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, err
	}
	epoch64, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, err
	}
	seqStart, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, err
	}
	seqEnd, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, err
	}
	factorU, err := uuid.Parse(parts[6])
	if err != nil {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, err
	}
	factorD, err := uuid.Parse(parts[7])
	if err != nil {
		return Scope{}, uuid.UUID{}, uuid.UUID{}, err
	}
	return Scope{
		RegionID:    regionID,
		SourceID:    sourceID,
		SourceEpoch: uint32(epoch64),
		SeqStart:    seqStart,
		SeqEnd:      seqEnd,
	}, factorU, factorD, nil
}

// RedisKey returns the optional regional SET NX key (M4-06).
func RedisKey(dedupKey string) string {
	return "dedup/v2:" + dedupKey
}
