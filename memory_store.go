package relaybox

import (
	"context"
	"sync"
	"time"
)

type memoryEntry struct {
	expiresAt time.Time
}

// MemoryStore is a process-local Store implementation suitable for tests,
// examples, CLIs, and single-process consumers.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]memoryEntry
	now     func() time.Time
}

// NewMemoryStore creates an in-memory claim store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make(map[string]memoryEntry),
		now:     time.Now,
	}
}

// Claim reserves a key for processing if it is not currently active.
func (s *MemoryStore) Claim(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked()

	if _, exists := s.entries[key]; exists {
		return false, nil
	}

	s.entries[key] = memoryEntry{expiresAt: s.now().Add(ttl)}
	return true, nil
}

// MarkDone extends the key lifetime so duplicates are skipped for the done TTL.
func (s *MemoryStore) MarkDone(ctx context.Context, key string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked()
	s.entries[key] = memoryEntry{expiresAt: s.now().Add(ttl)}
	return nil
}

// Release removes a claim so the message can be retried.
func (s *MemoryStore) Release(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, key)
	return nil
}

func (s *MemoryStore) pruneExpiredLocked() {
	now := s.now()
	for key, entry := range s.entries {
		if !entry.expiresAt.After(now) {
			delete(s.entries, key)
		}
	}
}
