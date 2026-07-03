package rtb

import "errors"

// ErrSnapshotUnstable reports SaveSnapshot could not capture a consistent registry generation.
var ErrSnapshotUnstable = errors.New("rtb: snapshot capture unstable")
