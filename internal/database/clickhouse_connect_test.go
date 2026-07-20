package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectCHReadonly_RejectsEmptyDSN(t *testing.T) {
	t.Parallel()
	_, err := ConnectCHReadonly(context.Background(), "")
	require.Error(t, err)
}
