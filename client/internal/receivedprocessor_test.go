package internal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opamp-go/client/types"
	sharedinternal "github.com/open-telemetry/opamp-go/internal"
	"github.com/open-telemetry/opamp-go/protobufs"
)

func TestProcessErrorResponseUnavailable(t *testing.T) {
	t.Run("returns true with retry_info", func(t *testing.T) {
		processor := newReceivedProcessor(
			&sharedinternal.NopLogger{}, defaultCallbacks(), NewMockSender(), &ClientSyncedState{},
			nil, nil, -1, 0,
		)

		body := &protobufs.ServerErrorResponse{
			Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
			ErrorMessage: "server overloaded",
			Details: &protobufs.ServerErrorResponse_RetryInfo{
				RetryInfo: &protobufs.RetryInfo{
					RetryAfterNanoseconds: uint64(5 * time.Second),
				},
			},
		}

		retryAfter, shouldRetry := processor.processErrorResponse(t.Context(), body)
		assert.True(t, shouldRetry)
		assert.Equal(t, 5*time.Second, retryAfter)
	})

	t.Run("returns true with zero retryAfter when no retry_info", func(t *testing.T) {
		processor := newReceivedProcessor(
			&sharedinternal.NopLogger{}, defaultCallbacks(), NewMockSender(), &ClientSyncedState{},
			nil, nil, -1, 0,
		)

		body := &protobufs.ServerErrorResponse{
			Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
			ErrorMessage: "server overloaded",
		}

		retryAfter, shouldRetry := processor.processErrorResponse(t.Context(), body)
		assert.True(t, shouldRetry)
		assert.Equal(t, time.Duration(0), retryAfter,
			"retryAfter should be zero when no retry_info, signaling caller to use backoff")
	})

	t.Run("returns false for non-unavailable errors", func(t *testing.T) {
		processor := newReceivedProcessor(
			&sharedinternal.NopLogger{}, defaultCallbacks(), NewMockSender(), &ClientSyncedState{},
			nil, nil, -1, 0,
		)

		body := &protobufs.ServerErrorResponse{
			Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_BadRequest,
			ErrorMessage: "bad request",
		}

		_, shouldRetry := processor.processErrorResponse(t.Context(), body)
		assert.False(t, shouldRetry)
	})

	t.Run("caps retry_info when maxRetryAfter is configured", func(t *testing.T) {
		cap := 15 * time.Minute
		processor := newReceivedProcessor(
			&sharedinternal.NopLogger{}, defaultCallbacks(), NewMockSender(), &ClientSyncedState{},
			nil, nil, -1, cap,
		)

		body := &protobufs.ServerErrorResponse{
			Type: protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
			Details: &protobufs.ServerErrorResponse_RetryInfo{
				RetryInfo: &protobufs.RetryInfo{
					RetryAfterNanoseconds: uint64(500 * time.Hour),
				},
			},
		}

		retryAfter, shouldRetry := processor.processErrorResponse(t.Context(), body)
		assert.True(t, shouldRetry)
		assert.Equal(t, cap, retryAfter, "should be capped at configured maxRetryAfter")
	})

	t.Run("does not cap when maxRetryAfter is zero", func(t *testing.T) {
		processor := newReceivedProcessor(
			&sharedinternal.NopLogger{}, defaultCallbacks(), NewMockSender(), &ClientSyncedState{},
			nil, nil, -1, 0,
		)

		largeDuration := 500 * time.Hour
		body := &protobufs.ServerErrorResponse{
			Type: protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
			Details: &protobufs.ServerErrorResponse_RetryInfo{
				RetryInfo: &protobufs.RetryInfo{
					RetryAfterNanoseconds: uint64(largeDuration),
				},
			},
		}

		retryAfter, shouldRetry := processor.processErrorResponse(t.Context(), body)
		assert.True(t, shouldRetry)
		assert.Equal(t, largeDuration, retryAfter, "should not cap when maxRetryAfter is zero")
	})

	t.Run("calls OnError callback", func(t *testing.T) {
		var gotError atomic.Bool
		callbacks := defaultCallbacks()
		callbacks.OnError = func(ctx context.Context, err *protobufs.ServerErrorResponse) {
			gotError.Store(true)
		}

		processor := newReceivedProcessor(
			&sharedinternal.NopLogger{}, callbacks, NewMockSender(), &ClientSyncedState{},
			nil, nil, -1, 0,
		)

		body := &protobufs.ServerErrorResponse{
			Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
			ErrorMessage: "server overloaded",
		}

		processor.processErrorResponse(t.Context(), body)
		assert.True(t, gotError.Load())
	})
}

func TestProcessReceivedMessageUnavailableRetry(t *testing.T) {
	processor := newReceivedProcessor(
		&sharedinternal.NopLogger{}, defaultCallbacks(), NewMockSender(), &ClientSyncedState{},
		nil, nil, -1, 0,
	)

	retryDuration := 10 * time.Second
	msg := &protobufs.ServerToAgent{
		ErrorResponse: &protobufs.ServerErrorResponse{
			Type: protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
			Details: &protobufs.ServerErrorResponse_RetryInfo{
				RetryInfo: &protobufs.RetryInfo{
					RetryAfterNanoseconds: uint64(retryDuration),
				},
			},
		},
	}

	retryAfter, shouldRetry := processor.ProcessReceivedMessage(t.Context(), msg)
	require.True(t, shouldRetry)
	assert.Equal(t, retryDuration, retryAfter)
}

func defaultCallbacks() types.Callbacks {
	c := types.Callbacks{}
	c.SetDefaults()
	return c
}
