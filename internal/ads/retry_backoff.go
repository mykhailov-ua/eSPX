package ads

import "time"

// Retry backoff bounds for store writes that must survive transient backend outages.
var (
	MaxRetries  = 3
	InitialWait = 100 * time.Millisecond
	MaxWait     = 2 * time.Second
)
