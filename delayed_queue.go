package relaybox

import (
	"context"
	"errors"
	"time"
)

// DelayedQueue schedules messages into an OutboxRepository for future publish.
//
// It intentionally reuses the outbox lifecycle: delayed messages become normal
// pending outbox messages when AvailableAt is due, and OutboxProcessor handles
// publishing, retries, and terminal failure.
type DelayedQueue struct {
	repository OutboxRepository
}

// NewDelayedQueue creates a delayed queue backed by an outbox repository.
func NewDelayedQueue(repository OutboxRepository) *DelayedQueue {
	return &DelayedQueue{repository: repository}
}

// ScheduleAt schedules a message for a specific time.
func (q *DelayedQueue) ScheduleAt(
	ctx context.Context,
	id string,
	subject string,
	payload []byte,
	availableAt time.Time,
	options ...OutboxMessageOption,
) error {
	if q == nil || q.repository == nil {
		return errors.New("nil delayed queue repository")
	}
	options = append(options, WithOutboxAvailableAt(availableAt))
	return q.repository.Add(ctx, NewOutboxMessage(id, subject, payload, options...))
}

// ScheduleAfter schedules a message after a relative delay.
func (q *DelayedQueue) ScheduleAfter(
	ctx context.Context,
	id string,
	subject string,
	payload []byte,
	delay time.Duration,
	options ...OutboxMessageOption,
) error {
	if delay < 0 {
		delay = 0
	}
	return q.ScheduleAt(ctx, id, subject, payload, time.Now().UTC().Add(delay), options...)
}
