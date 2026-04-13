package relaybox

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryOutboxRepository is a process-local OutboxRepository implementation.
//
// It is useful for tests, examples, local tools, and single-process services.
// Use a durable implementation for production transactional outboxes.
type MemoryOutboxRepository struct {
	mu       sync.Mutex
	messages map[string]OutboxMessage
	now      func() time.Time
}

// NewMemoryOutboxRepository creates an in-memory outbox repository.
func NewMemoryOutboxRepository() *MemoryOutboxRepository {
	return &MemoryOutboxRepository{
		messages: make(map[string]OutboxMessage),
		now:      time.Now,
	}
}

// Add stores a pending outbox message.
func (r *MemoryOutboxRepository) Add(ctx context.Context, message OutboxMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateOutboxMessage(message); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.messages[message.ID]; exists {
		return ErrOutboxMessageExists
	}

	now := r.now().UTC()
	message = cloneOutboxMessage(message)
	message.Status = OutboxStatusPending
	message.LastError = ""
	message.PublishedAt = nil
	if message.MaxAttempts <= 0 {
		message.MaxAttempts = defaultOutboxMaxAttempts
	}
	if message.AvailableAt.IsZero() {
		message.AvailableAt = now
	}
	if message.CreatedAt.IsZero() {
		message.CreatedAt = now
	}
	message.UpdatedAt = now

	r.messages[message.ID] = message
	return nil
}

// ClaimDue claims pending messages whose AvailableAt is not in the future.
func (r *MemoryOutboxRepository) ClaimDue(
	ctx context.Context,
	limit int,
	now time.Time,
) ([]OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	if now.IsZero() {
		now = r.now()
	}
	now = now.UTC()

	r.mu.Lock()
	defer r.mu.Unlock()

	due := make([]OutboxMessage, 0, limit)
	for _, message := range r.messages {
		if message.Status != OutboxStatusPending {
			continue
		}
		if message.AvailableAt.After(now) {
			continue
		}
		if message.MaxAttempts <= 0 {
			message.MaxAttempts = defaultOutboxMaxAttempts
		}
		if message.Attempts >= message.MaxAttempts {
			message.Status = OutboxStatusFailed
			message.UpdatedAt = now
			r.messages[message.ID] = message
			continue
		}
		due = append(due, message)
	}

	sort.Slice(due, func(i, j int) bool {
		if !due[i].AvailableAt.Equal(due[j].AvailableAt) {
			return due[i].AvailableAt.Before(due[j].AvailableAt)
		}
		if !due[i].CreatedAt.Equal(due[j].CreatedAt) {
			return due[i].CreatedAt.Before(due[j].CreatedAt)
		}
		return due[i].ID < due[j].ID
	})

	if len(due) > limit {
		due = due[:limit]
	}

	claimed := make([]OutboxMessage, 0, len(due))
	for _, message := range due {
		current := r.messages[message.ID]
		current.Status = OutboxStatusProcessing
		current.Attempts++
		current.UpdatedAt = now
		r.messages[message.ID] = current
		claimed = append(claimed, cloneOutboxMessage(current))
	}

	return claimed, nil
}

// MarkPublished marks a message as successfully published.
func (r *MemoryOutboxRepository) MarkPublished(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	message, exists := r.messages[id]
	if !exists {
		return ErrOutboxMessageNotFound
	}

	now := r.now().UTC()
	message.Status = OutboxStatusPublished
	message.UpdatedAt = now
	message.PublishedAt = &now
	message.LastError = ""
	r.messages[id] = message
	return nil
}

// MarkFailed records a failed publish attempt and schedules a retry if allowed.
func (r *MemoryOutboxRepository) MarkFailed(
	ctx context.Context,
	id string,
	retryAt time.Time,
	cause error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	message, exists := r.messages[id]
	if !exists {
		return ErrOutboxMessageNotFound
	}

	now := r.now().UTC()
	if retryAt.IsZero() {
		retryAt = now
	}
	if cause != nil {
		message.LastError = cause.Error()
	}
	if message.MaxAttempts <= 0 {
		message.MaxAttempts = defaultOutboxMaxAttempts
	}
	if message.Attempts >= message.MaxAttempts {
		message.Status = OutboxStatusFailed
	} else {
		message.Status = OutboxStatusPending
		message.AvailableAt = retryAt.UTC()
	}
	message.UpdatedAt = now
	r.messages[id] = message
	return nil
}

// Release makes a claimed message available again without changing Attempts.
func (r *MemoryOutboxRepository) Release(ctx context.Context, id string, availableAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	message, exists := r.messages[id]
	if !exists {
		return ErrOutboxMessageNotFound
	}
	if availableAt.IsZero() {
		availableAt = r.now()
	}

	message.Status = OutboxStatusPending
	message.AvailableAt = availableAt.UTC()
	message.UpdatedAt = r.now().UTC()
	r.messages[id] = message
	return nil
}

// Get returns a copy of a message for assertions and diagnostics.
func (r *MemoryOutboxRepository) Get(ctx context.Context, id string) (OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return OutboxMessage{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	message, exists := r.messages[id]
	if !exists {
		return OutboxMessage{}, ErrOutboxMessageNotFound
	}
	return cloneOutboxMessage(message), nil
}

// Len returns the number of messages currently stored.
func (r *MemoryOutboxRepository) Len(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.messages), nil
}

var _ OutboxRepository = (*MemoryOutboxRepository)(nil)
