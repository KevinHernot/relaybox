package relaybox

import (
	"context"
	"time"
)

// Store tracks claims for processed events.
//
// Claim returns true when the caller has acquired the right to process the
// event, or false when another caller has already claimed or completed it.
type Store interface {
	Claim(ctx context.Context, key string, ttl time.Duration) (bool, error)
	MarkDone(ctx context.Context, key string, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}
