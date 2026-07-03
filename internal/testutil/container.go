package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// ContainerStopTimeout bounds container stop calls in fault tests.
const ContainerStopTimeout = 10 * time.Second

func StopContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	timeout := ContainerStopTimeout
	require.NoError(t, c.Stop(context.Background(), &timeout))
}

func StartContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.NoError(t, c.Start(context.Background()))
}

func TerminateContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.NoError(t, c.Terminate(context.Background()))
}
