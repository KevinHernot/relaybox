package relaybox

import (
	"context"
	"time"
)

// ClaimResult describes the current processing state for an event key.
type ClaimResult uint8

const (
	// ClaimAcquired means the caller acquired the right to process the event.
	ClaimAcquired ClaimResult = iota
	// ClaimInProgress means another caller is currently processing the event.
	ClaimInProgress
	// ClaimDone means the event was already processed successfully.
	ClaimDone
)

// Store tracks claims for processed events.
//
// Claim returns the current state for the event. Implementations must
// distinguish between events that are already done and events that are still
// being processed so callers can avoid acknowledging in-flight redeliveries.
type Store interface {
	Claim(ctx context.Context, key string, ttl time.Duration) (ClaimResult, error)
	MarkDone(ctx context.Context, key string, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}
