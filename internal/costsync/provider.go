package costsync

import (
	"context"
	"time"
)

// Provider fetches normalized cost/revenue lines from one ad or RSOC network.
type Provider interface {
	Network() string
	Fetch(ctx context.Context, cred Credential, date time.Time) ([]CostLine, error)
}

// OAuthRefresher exchanges a refresh token for a new access token.
type OAuthRefresher interface {
	Refresh(ctx context.Context, cred Credential) (accessToken string, expiresAt time.Time, err error)
}
