package ingestion

import (
	"context"
	"errors"
	"testing"
)

func TestIsRetriableStoreError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "pg_refused", err: errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"), want: true},
		{name: "spool_max", err: errCHSpoolMaxSegments, want: true},
		{name: "logic", err: errors.New("duplicate key value violates unique constraint"), want: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetriableStoreError(tc.err); got != tc.want {
				t.Fatalf("isRetriableStoreError() = %v, want %v", got, tc.want)
			}
		})
	}
}
