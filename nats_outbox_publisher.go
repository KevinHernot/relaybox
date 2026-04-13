package relaybox

import (
	"context"
	"errors"
	"time"

	nats "github.com/nats-io/nats.go"
)

const defaultNATSOutboxFlushTimeout = 5 * time.Second

// NATSOutboxPublisherOption customizes a NATS outbox publisher.
type NATSOutboxPublisherOption func(*NATSOutboxPublisher)

// NATSOutboxPublisher publishes outbox messages to Core NATS.
type NATSOutboxPublisher struct {
	conn         *nats.Conn
	flushTimeout time.Duration
}

// NewNATSOutboxPublisher creates a NATS-backed outbox publisher.
func NewNATSOutboxPublisher(conn *nats.Conn, options ...NATSOutboxPublisherOption) *NATSOutboxPublisher {
	publisher := &NATSOutboxPublisher{
		conn:         conn,
		flushTimeout: defaultNATSOutboxFlushTimeout,
	}

	for _, option := range options {
		if option != nil {
			option(publisher)
		}
	}
	return publisher
}

// WithNATSOutboxFlushTimeout sets how long Publish waits for a NATS flush.
func WithNATSOutboxFlushTimeout(timeout time.Duration) NATSOutboxPublisherOption {
	return func(publisher *NATSOutboxPublisher) {
		publisher.flushTimeout = timeout
	}
}

// Publish publishes a message and flushes the connection for backpressure.
func (p *NATSOutboxPublisher) Publish(ctx context.Context, message OutboxMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || p.conn == nil {
		return errors.New("nil NATS connection")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateOutboxMessage(message); err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: message.Subject,
		Data:    append([]byte(nil), message.Payload...),
		Header:  nats.Header{},
	}
	for key, value := range message.Headers {
		msg.Header.Set(key, value)
	}
	if msg.Header.Get(nats.MsgIdHdr) == "" {
		msg.Header.Set(nats.MsgIdHdr, message.ID)
	}

	if err := p.conn.PublishMsg(msg); err != nil {
		return err
	}

	flushCtx := ctx
	if p.flushTimeout > 0 {
		var cancel context.CancelFunc
		flushCtx, cancel = context.WithTimeout(ctx, p.flushTimeout)
		defer cancel()
	}
	return p.conn.FlushWithContext(flushCtx)
}

var _ OutboxPublisher = (*NATSOutboxPublisher)(nil)
