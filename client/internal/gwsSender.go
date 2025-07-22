package internal

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/internal"
	"github.com/open-telemetry/opamp-go/protobufs"
	"google.golang.org/protobuf/proto"
)

const (
	defaultSendCloseMessageTimeout = 5 * time.Second
	defaultHeartbeatIntervalMs     = 30 * 1000
)

type WebsocketSender struct {
	SenderCommon
	logger types.Logger

	stopped chan struct{}
	errors  chan error

	heartbeatIntervalUpdated chan struct{}
	heartbeatIntervalMs      atomic.Int64
	heartbeatTimer           *time.Timer
}

func NewWebsocketSender(logger types.Logger) *WebsocketSender {
	w := &WebsocketSender{
		SenderCommon:   NewSenderCommon(),
		logger:         logger,
		heartbeatTimer: time.NewTimer(0),
	}
	w.heartbeatIntervalMs.Store(defaultHeartbeatIntervalMs)
	return w
}

func (w *WebsocketSender) Start(ctx context.Context, conn *gws.Conn) error {
	err := w.sendNextMessage(ctx, conn)

	w.stopped = make(chan struct{})
	w.errors = make(chan error, 1)
	go w.run(ctx, conn)
	return err
}

func (w *WebsocketSender) IsStopped() <-chan struct{} {
	return w.stopped
}

func (w *WebsocketSender) Errors() <-chan error {
	return w.errors
}

func (w *WebsocketSender) SetHeartbeatInterval(d time.Duration) error {
	if d < 0 {
		return errors.New("heartbeat interval for wsclient must be non-negative")
	}

	w.heartbeatIntervalMs.Store(int64(d.Milliseconds()))
	select {
	case w.heartbeatIntervalUpdated <- struct{}{}:
	default:
	}
	return nil
}

func (w *WebsocketSender) shouldSendHeartbeat() <-chan time.Time {
	t := w.heartbeatTimer

	// Before Go 1.23, the only safe way to use Reset was to [Stop] and
	// explicitly drain the timer first.
	// ref: https://pkg.go.dev/time#Timer.Reset
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}

	if d := time.Duration(w.heartbeatIntervalMs.Load()) * time.Millisecond; d != 0 {
		t.Reset(d)
		return t.C
	}

	// Heartbeat interval is set to Zero, disable heartbeat.
	return nil
}

func (w *WebsocketSender) updateHeartbeatTimer() {
	t := w.heartbeatTimer
	// Before Go 1.23, the only safe way to use Reset was to [Stop] and
	// explicitly drain the timer first.
	// ref: https://pkg.go.dev/time#Timer.Reset
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}

	if d := time.Duration(w.heartbeatIntervalMs.Load()) * time.Millisecond; d != 0 {
		t.Reset(d)
	}
}

func (w *WebsocketSender) run(ctx context.Context, conn *gws.Conn) {
	defer func() {
		w.heartbeatTimer.Stop()
		close(w.stopped)
		close(w.errors)
	}()

	for {
		select {
		case <-ctx.Done():
			err := w.sendCloseMessage(conn)
			if err != nil && !errors.Is(err, gws.ErrConnClosed) {
				w.errors <- err
			}
			return
		case <-w.heartbeatIntervalUpdated:
			w.updateHeartbeatTimer()
		case <-w.hasPendingMessage:
			w.sendNextMessage(ctx, conn)
		case <-w.shouldSendHeartbeat():
			w.NextMessage().Update(func(msg *protobufs.AgentToServer) {})
			w.ScheduleSend()
		}
	}
}

func (w *WebsocketSender) sendCloseMessage(conn *gws.Conn) error {
	// TODO send AgentToServer disconnect
	err := conn.WriteClose(1000, []byte("Normal closure")) // 1000 is the code for normal closure
	return err
}

func (w *WebsocketSender) sendNextMessage(ctx context.Context, conn *gws.Conn) error {
	msg := w.nextMessage.PopPending()
	if msg != nil && !proto.Equal(msg, &protobufs.AgentToServer{}) {
		// There is a pending message and the message has some fields populated.
		return w.sendMessage(ctx, conn, msg)
	}
	return nil
}

func (w *WebsocketSender) sendMessage(ctx context.Context, conn *gws.Conn, msg *protobufs.AgentToServer) error {
	if err := internal.WriteGWSMessage(conn, msg); err != nil {
		w.logger.Errorf(ctx, "Cannot write WS message: %v", err)
		// TODO: check if it is a connection error then propagate error back to Client and reconnect.
		return err
	}
	return nil
}
