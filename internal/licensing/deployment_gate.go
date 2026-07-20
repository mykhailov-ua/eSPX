package licensing

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DeploymentSnapshot is the cold-path license_status mirror used by feature workers.
type DeploymentSnapshot struct {
	State        LicenseState
	VolumeBand   VolumeBand
	Entitlements Entitlements
}

// LoadDeploymentSnapshot reads billing.license_status (single deployment row).
func LoadDeploymentSnapshot(ctx context.Context, pool *pgxpool.Pool) (DeploymentSnapshot, error) {
	var snap DeploymentSnapshot
	if pool == nil {
		return snap, pgx.ErrNoRows
	}
	var stateStr string
	var entitlementsBytes []byte
	err := pool.QueryRow(ctx, `
		SELECT state, entitlements_json
		FROM billing.license_status
		LIMIT 1`).Scan(&stateStr, &entitlementsBytes)
	if err != nil {
		return snap, err
	}
	snap.State = LicenseState(stateStr)
	if len(entitlementsBytes) > 0 {
		_ = json.Unmarshal(entitlementsBytes, &snap.Entitlements)
	}
	snap.VolumeBand = snap.Entitlements.VolumeBand
	if snap.VolumeBand == "" {
		snap.VolumeBand = VolumeBandSmall
	}
	snap.Entitlements.Features = snap.Entitlements.Features.Normalized()
	return snap, nil
}

// ModuleAllowed reports whether a subsystem may run under the deployment license.
func (s DeploymentSnapshot) ModuleAllowed(check func(FeatureSet) bool) bool {
	if s.State == StateExpired || s.State == StateRevoked {
		return false
	}
	return check(s.Entitlements.Features)
}
