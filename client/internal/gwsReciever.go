package internal

import (
	"context"
	"sync"
	"time"

	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
)

type websocketReceiver struct {
	logger    types.Logger
	callbacks types.Callbacks
	websocket *Websocket
	sender    *WebsocketSender
	processor receivedProcessor

	// Indicates that the receiver has fully stopped.
	stopped chan struct{}
}

func NewWebsocketReceiver(
	logger types.Logger,
	callbacks types.Callbacks,
	websocket *Websocket,
	sender *WebsocketSender,
	clientSyncedState *ClientSyncedState,
	packagesStateProvider types.PackagesStateProvider,
	capabilities protobufs.AgentCapabilities,
	packageSyncMutex *sync.Mutex,
	reporterInterval time.Duration,
) *websocketReceiver {
	return &websocketReceiver{
		logger:    logger,
		callbacks: callbacks,
		websocket: websocket,
		sender:    sender,
		processor: newReceivedProcessor(logger, callbacks, sender, clientSyncedState, packagesStateProvider, capabilities, packageSyncMutex, reporterInterval),
		stopped:   make(chan struct{}),
	}
}

func (w *websocketReceiver) Start(ctx context.Context) {
	go w.ReceiverLoop(ctx)
}

func (w *websocketReceiver) IsStopped() <-chan struct{} {
	return w.stopped
}

func (w *websocketReceiver) ReceiverLoop(ctx context.Context) {
	defer func() {
		close(w.stopped)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-w.websocket.Messages():
			if msg.err != nil {
				w.logger.Errorf(ctx, "Unexpected error while receiving: %v", msg.err)
				return
			}
			// FIXME ReciverLoop should stop before it attempts to read from a closed channel to avoid this nil message
			// Remove panic after fixing
			if msg.message == nil {
				panic("nil message")
			}
			w.processor.ProcessReceivedMessage(ctx, msg.message)
		}
	}
}
