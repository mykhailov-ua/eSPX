package notifier

import (
	"errors"
	"testing"

	"espx/internal/notifier/pb"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Guards validation and not-found errors map to stable gRPC codes.
func TestMapRPCError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "recipient required",
			err:      ErrRecipientRequired,
			wantCode: codes.InvalidArgument,
			wantMsg:  ErrRecipientRequired.Error(),
		},
		{
			name:     "body required",
			err:      ErrBodyRequired,
			wantCode: codes.InvalidArgument,
			wantMsg:  ErrBodyRequired.Error(),
		},
		{
			name:     "unsupported provider",
			err:      ErrUnsupportedProvider,
			wantCode: codes.InvalidArgument,
			wantMsg:  ErrUnsupportedProvider.Error(),
		},
		{
			name:     "invalid notification id",
			err:      ErrInvalidNotificationID,
			wantCode: codes.InvalidArgument,
			wantMsg:  ErrInvalidNotificationID.Error(),
		},
		{
			name:     "notification not found",
			err:      ErrNotificationNotFound,
			wantCode: codes.NotFound,
			wantMsg:  ErrNotificationNotFound.Error(),
		},
		{
			name:     "pgx no rows",
			err:      pgx.ErrNoRows,
			wantCode: codes.NotFound,
			wantMsg:  ErrNotificationNotFound.Error(),
		},
		{
			name:     "internal",
			err:      errors.New("database unavailable"),
			wantCode: codes.Internal,
			wantMsg:  "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, ok := status.FromError(mapRPCError(tt.err))
			require.True(t, ok)
			assert.Equal(t, tt.wantCode, st.Code())
			assert.Equal(t, tt.wantMsg, st.Message())
		})
	}
}

// Guards nil errors pass through unchanged.
func TestMapRPCError_nil(t *testing.T) {
	require.NoError(t, mapRPCError(nil))
}

// Guards provider enum mapping rejects unspecified values.
func TestMapPBProviderToDB_unspecified(t *testing.T) {
	_, err := MapPBProviderToDB(pb.Provider_PROVIDER_UNSPECIFIED)
	require.ErrorIs(t, err, ErrUnsupportedProvider)
}
