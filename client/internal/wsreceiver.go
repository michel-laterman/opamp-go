package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/internal"
	"github.com/open-telemetry/opamp-go/protobufs"
)

// wsReceiver implements the WebSocket client's receiving portion of OpAMP protocol.
type wsReceiver struct {
	conn      *websocket.Conn
	logger    types.Logger
	sender    *WSSender
	callbacks types.Callbacks
	processor receivedProcessor

	// Indicates that the receiver has fully stopped.
	stopped chan struct{}

	// throttled is true when the receiver stopped due to an UNAVAILABLE response.
	throttled bool

	// retryAfter is set when the server sends an UNAVAILABLE response with retry info.
	// Zero means the server did not specify a duration and the caller should use backoff.
	retryAfter time.Duration
}

// NewWSReceiver creates a new Receiver that uses WebSocket to receive
// messages from the server.
func NewWSReceiver(
	logger types.Logger,
	callbacks types.Callbacks,
	conn *websocket.Conn,
	sender *WSSender,
	clientSyncedState *ClientSyncedState,
	packagesStateProvider types.PackagesStateProvider,
	packageSyncMutex *sync.Mutex,
	reporterInterval time.Duration,
	maxRetryAfter time.Duration,
) *wsReceiver {
	w := &wsReceiver{
		conn:      conn,
		logger:    logger,
		sender:    sender,
		callbacks: callbacks,
		processor: newReceivedProcessor(logger, callbacks, sender, clientSyncedState, packagesStateProvider, packageSyncMutex, reporterInterval, maxRetryAfter),
		stopped:   make(chan struct{}),
	}

	return w
}

// Start starts the receiver loop.
func (r *wsReceiver) Start(ctx context.Context) {
	go r.ReceiverLoop(ctx)
}

// IsStopped returns a channel that's closed when the receiver is stopped.
func (r *wsReceiver) IsStopped() <-chan struct{} {
	return r.stopped
}

// Throttled returns true if the receiver stopped because the server sent an
// UNAVAILABLE error response.
func (r *wsReceiver) Throttled() bool {
	return r.throttled
}

// RetryAfter returns the duration the client should wait before reconnecting.
// Only meaningful when Throttled() returns true. Zero means the server did not
// specify a duration and the caller should use exponential backoff.
func (r *wsReceiver) RetryAfter() time.Duration {
	return r.retryAfter
}

// ReceiverLoop runs the receiver loop.
// To stop the receiver cancel the context and close the websocket connection
func (r *wsReceiver) ReceiverLoop(ctx context.Context) {
	type receivedMessage struct {
		message *protobufs.ServerToAgent
		err     error
	}

	defer func() { close(r.stopped) }()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			result := make(chan receivedMessage, 1)

			// To stop this goroutine, close the websocket connection
			go func() {
				var message protobufs.ServerToAgent
				err := r.receiveMessage(&message)
				result <- receivedMessage{&message, err}
			}()

			select {
			case <-ctx.Done():
				return
			case res := <-result:
				if res.err != nil {
					if !websocket.IsCloseError(res.err, websocket.CloseNormalClosure) {
						r.logger.Errorf(ctx, "Unexpected error while receiving: %v", res.err)
					}
					return
				}
				if retryAfter, shouldRetry := r.processor.ProcessReceivedMessage(ctx, res.message); shouldRetry {
					r.throttled = true
					r.retryAfter = retryAfter
					return
				}
			}
		}
	}
}

func (r *wsReceiver) receiveMessage(msg *protobufs.ServerToAgent) error {
	mt, bytes, err := r.conn.ReadMessage()
	if err != nil {
		return err
	}
	if mt != websocket.BinaryMessage {
		return fmt.Errorf("unsupported message type: %v", mt)
	}
	err = internal.DecodeWSMessage(bytes, msg)
	if err != nil {
		return fmt.Errorf("cannot decode received message: %w", err)
	}
	return err
}
