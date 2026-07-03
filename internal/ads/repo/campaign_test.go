package repo

import (
	"testing"

	"espx/internal/ads/db"
	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestCampaignFromDBRow_FraudConfig(t *testing.T) {
	id := uuid.New()
	row := db.Campaign{
		ID:                    pgtype.UUID{Bytes: id, Valid: true},
		CustomerID:            pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Status:                db.CampaignStatusTypeACTIVE,
		FraudThresholdPass:    25,
		FraudThresholdSuspect: 55,
		FraudThresholdIvt:     75,
		FraudThresholdBlock:   95,
		GhostIvtEnabled:       true,
		BehaviorFlags:         int32(domain.BehaviorLowTTC | domain.BehaviorVelIP),
	}
	camp := campaignFromDBRow(row)
	assert.Equal(t, uint8(25), camp.FraudThresholdPass)
	assert.Equal(t, uint8(55), camp.FraudThresholdSuspect)
	assert.Equal(t, uint8(75), camp.FraudThresholdIVT)
	assert.Equal(t, uint8(95), camp.FraudThresholdBlock)
	assert.True(t, camp.GhostIVTEnabled)
	assert.Equal(t, domain.BehaviorLowTTC|domain.BehaviorVelIP, camp.BehaviorFlags)
}
