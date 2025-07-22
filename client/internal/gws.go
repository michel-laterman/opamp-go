package internal

import (
	"context"
	"errors"

	"github.com/lxzan/gws"

	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/internal"
	"github.com/open-telemetry/opamp-go/protobufs"
)

// recievedMessage is a wrapper that holds either a decoded message, or an error associated with decoding it.
type receivedMessage struct {
	message *protobufs.ServerToAgent
	err     error
}

type Websocket struct {
	logger   types.Logger
	messages chan receivedMessage
}

func NewWebsocket(logger types.Logger) *Websocket {
	return &Websocket{
		logger:   logger,
		messages: make(chan receivedMessage, 1),
	}
}

// OpOpen is a nop
func (w *Websocket) OnOpen(socket *gws.Conn) {}

// OnClose logs the websocket connection closure.
func (w *Websocket) OnClose(socket *gws.Conn, err error) {
	defer close(w.messages)
	var ce *gws.CloseError
	if errors.As(err, &ce) {
		w.logger.Debugf(context.Background(), "Recieved close frame from %s: %v", socket.RemoteAddr().String(), ce)
		return
	}
	w.logger.Errorf(context.Background(), "Connection to %s closed with error: %v", socket.RemoteAddr().String(), err)
}

// OnPing logs and responds to ping messages.
func (w *Websocket) OnPing(socket *gws.Conn, payload []byte) {
	w.logger.Debugf(context.Background(), "Received ping from %s.", socket.RemoteAddr().String())
	_ = socket.WritePong([]byte("pong"))
}

// OnPong is a nop.
func (w *Websocket) OnPong(socket *gws.Conn, payload []byte) {}

// OnMessage parses the recieved payload into a ServerToAgent message and passes it to the messages channel.
func (w *Websocket) OnMessage(socket *gws.Conn, m *gws.Message) {
	defer m.Close()
	var msg protobufs.ServerToAgent
	err := internal.DecodeWSMessage(m.Bytes(), &msg)
	if err != nil {
		w.messages <- receivedMessage{err: err}
		return
	}
	w.messages <- receivedMessage{message: &msg}
}

// Messages can be used to read parsed messages that the Websocket receives.
func (w *Websocket) Messages() <-chan receivedMessage {
	return w.messages
}
