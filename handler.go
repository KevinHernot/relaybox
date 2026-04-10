package relaybox

import (
	"context"
	"errors"
	"time"

	nats "github.com/nats-io/nats.go"
)

const (
	defaultClaimTTL     = 5 * time.Minute
	defaultProcessedTTL = 24 * time.Hour
	defaultSafetyMargin = 2 * time.Second
)

// ErrEventInProgress indicates another consumer attempt is still processing the
// same event.
var ErrEventInProgress = errors.New("event already being processed")

// ContextHandler processes a message payload with a context derived from the
// inbound NATS message.
type ContextHandler func(context.Context, []byte) error

// HandlerOptions configures the idempotent handler.
type HandlerOptions struct {
	KeyPrefix    string
	ClaimTTL     time.Duration
	ProcessedTTL time.Duration
	AckWait      time.Duration
	SafetyMargin time.Duration
}

// DefaultHandlerOptions returns production-friendly defaults.
func DefaultHandlerOptions() HandlerOptions {
	return HandlerOptions{
		KeyPrefix:    "nats:event",
		ClaimTTL:     defaultClaimTTL,
		ProcessedTTL: defaultProcessedTTL,
		SafetyMargin: defaultSafetyMargin,
	}
}

// IdempotentHandler wraps a NATS handler with duplicate protection.
type IdempotentHandler struct {
	store   Store
	handler ContextHandler
	options HandlerOptions
}

// NewIdempotentHandler creates an idempotent handler for NATS consumers.
func NewIdempotentHandler(store Store, handler ContextHandler, options ...HandlerOptions) *IdempotentHandler {
	opts := DefaultHandlerOptions()
	if len(options) > 0 {
		opts = mergeHandlerOptions(opts, options[0])
	}

	return &IdempotentHandler{
		store:   store,
		handler: handler,
		options: opts,
	}
}

// HandlerFunc returns a func(*nats.Msg) error suitable for subscriptions.
func HandlerFunc(store Store, handler ContextHandler, options ...HandlerOptions) func(*nats.Msg) error {
	return NewIdempotentHandler(store, handler, options...).Handle
}

// Handle processes a NATS message exactly once for the configured TTL window.
func (h *IdempotentHandler) Handle(msg *nats.Msg) error {
	if msg == nil {
		return errors.New("nil NATS message")
	}

	ctx, cancel := deriveAckWaitContext(context.Background(), h.options.AckWait, h.options.SafetyMargin)
	defer cancel()

	eventID, err := ExtractStableEventID(msg)
	if err != nil {
		return h.handler(ctx, msg.Data)
	}

	key := h.options.KeyPrefix + ":" + eventID
	claimResult, err := h.store.Claim(ctx, key, h.options.ClaimTTL)
	if err != nil {
		return err
	}

	switch claimResult {
	case ClaimDone:
		return nil
	case ClaimInProgress:
		return ErrEventInProgress
	case ClaimAcquired:
	default:
		return errors.New("unknown claim result")
	}

	if err := h.handler(ctx, msg.Data); err != nil {
		_ = h.store.Release(context.Background(), key)
		return err
	}

	return h.store.MarkDone(ctx, key, h.options.ProcessedTTL)
}

func deriveAckWaitContext(
	ctx context.Context,
	ackWait time.Duration,
	safetyMargin time.Duration,
) (context.Context, context.CancelFunc) {
	if ackWait <= 0 {
		return ctx, func() {}
	}

	if safetyMargin <= 0 {
		safetyMargin = defaultSafetyMargin
	}

	safeAckWait := ackWait - safetyMargin
	if safeAckWait <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, safeAckWait)
}

func mergeHandlerOptions(base HandlerOptions, override HandlerOptions) HandlerOptions {
	if override.KeyPrefix != "" {
		base.KeyPrefix = override.KeyPrefix
	}
	if override.ClaimTTL > 0 {
		base.ClaimTTL = override.ClaimTTL
	}
	if override.ProcessedTTL > 0 {
		base.ProcessedTTL = override.ProcessedTTL
	}
	if override.AckWait > 0 {
		base.AckWait = override.AckWait
	}
	if override.SafetyMargin > 0 {
		base.SafetyMargin = override.SafetyMargin
	}
	return base
}
