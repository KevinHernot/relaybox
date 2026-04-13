package relaybox

import (
	"context"
	"errors"
	"time"
)

const (
	defaultOutboxBatchSize    = 100
	defaultOutboxPollInterval = time.Second
	defaultOutboxMaxAttempts  = 3
	defaultOutboxRetryBackoff = time.Second
	defaultOutboxRetryCap     = 30 * time.Second
)

// OutboxStatus describes where an outbox message is in the publish lifecycle.
type OutboxStatus string

const (
	// OutboxStatusPending means the message can be claimed when AvailableAt is due.
	OutboxStatusPending OutboxStatus = "pending"
	// OutboxStatusProcessing means a processor has claimed the message.
	OutboxStatusProcessing OutboxStatus = "processing"
	// OutboxStatusPublished means the message was published successfully.
	OutboxStatusPublished OutboxStatus = "published"
	// OutboxStatusFailed means the message exhausted its retry budget.
	OutboxStatusFailed OutboxStatus = "failed"
)

var (
	// ErrOutboxMessageNotFound indicates the repository could not find a message.
	ErrOutboxMessageNotFound = errors.New("outbox message not found")
	// ErrOutboxMessageExists indicates an outbox message already exists.
	ErrOutboxMessageExists = errors.New("outbox message already exists")
	// ErrInvalidOutboxMessage indicates the message is missing required fields.
	ErrInvalidOutboxMessage = errors.New("invalid outbox message")
)

// OutboxMessage is a portable event envelope for transactional outbox storage.
//
// The core package does not force a database schema. Repository implementations
// can map these fields to PostgreSQL rows, Valkey entries, embedded storage, or
// any other durable medium.
type OutboxMessage struct {
	ID            string            `json:"id"`
	Subject       string            `json:"subject"`
	Payload       []byte            `json:"payload"`
	Headers       map[string]string `json:"headers,omitempty"`
	AggregateID   string            `json:"aggregate_id,omitempty"`
	AggregateType string            `json:"aggregate_type,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Status        OutboxStatus      `json:"status"`
	Attempts      int               `json:"attempts"`
	MaxAttempts   int               `json:"max_attempts"`
	AvailableAt   time.Time         `json:"available_at"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	PublishedAt   *time.Time        `json:"published_at,omitempty"`
	LastError     string            `json:"last_error,omitempty"`
}

// OutboxMessageOption customizes a new outbox message.
type OutboxMessageOption func(*OutboxMessage)

// NewOutboxMessage creates a pending message ready to store in an outbox.
func NewOutboxMessage(id string, subject string, payload []byte, options ...OutboxMessageOption) OutboxMessage {
	now := time.Now().UTC()
	msg := OutboxMessage{
		ID:          id,
		Subject:     subject,
		Payload:     append([]byte(nil), payload...),
		Status:      OutboxStatusPending,
		MaxAttempts: defaultOutboxMaxAttempts,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	for _, option := range options {
		if option != nil {
			option(&msg)
		}
	}

	if msg.MaxAttempts <= 0 {
		msg.MaxAttempts = defaultOutboxMaxAttempts
	}
	if msg.AvailableAt.IsZero() {
		msg.AvailableAt = now
	}
	return msg
}

// WithOutboxHeaders adds publish headers to the message.
func WithOutboxHeaders(headers map[string]string) OutboxMessageOption {
	return func(msg *OutboxMessage) {
		msg.Headers = cloneStringMap(headers)
	}
}

// WithOutboxAggregate records the aggregate that produced the event.
func WithOutboxAggregate(aggregateType string, aggregateID string) OutboxMessageOption {
	return func(msg *OutboxMessage) {
		msg.AggregateType = aggregateType
		msg.AggregateID = aggregateID
	}
}

// WithOutboxMetadata adds storage-only metadata to the message.
func WithOutboxMetadata(metadata map[string]string) OutboxMessageOption {
	return func(msg *OutboxMessage) {
		msg.Metadata = cloneStringMap(metadata)
	}
}

// WithOutboxMaxAttempts sets how many publish attempts are made before failure.
func WithOutboxMaxAttempts(maxAttempts int) OutboxMessageOption {
	return func(msg *OutboxMessage) {
		msg.MaxAttempts = maxAttempts
	}
}

// WithOutboxDelay makes the message unavailable until the delay has elapsed.
func WithOutboxDelay(delay time.Duration) OutboxMessageOption {
	return func(msg *OutboxMessage) {
		if delay < 0 {
			delay = 0
		}
		msg.AvailableAt = time.Now().UTC().Add(delay)
	}
}

// WithOutboxAvailableAt schedules the message for a specific time.
func WithOutboxAvailableAt(availableAt time.Time) OutboxMessageOption {
	return func(msg *OutboxMessage) {
		msg.AvailableAt = availableAt.UTC()
	}
}

// OutboxRepository stores, claims, and updates outbox messages.
type OutboxRepository interface {
	Add(ctx context.Context, message OutboxMessage) error
	ClaimDue(ctx context.Context, limit int, now time.Time) ([]OutboxMessage, error)
	MarkPublished(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, retryAt time.Time, cause error) error
	Release(ctx context.Context, id string, availableAt time.Time) error
}

// OutboxPublisher publishes a claimed outbox message to the broker.
type OutboxPublisher interface {
	Publish(ctx context.Context, message OutboxMessage) error
}

// RetryBackoffFunc returns the delay before retrying a failed publish attempt.
type RetryBackoffFunc func(message OutboxMessage, cause error) time.Duration

// OutboxProcessorOptions configures the polling outbox processor.
type OutboxProcessorOptions struct {
	BatchSize    int
	PollInterval time.Duration
	RetryBackoff RetryBackoffFunc
	Now          func() time.Time
}

// DefaultOutboxProcessorOptions returns production-friendly processor defaults.
func DefaultOutboxProcessorOptions() OutboxProcessorOptions {
	return OutboxProcessorOptions{
		BatchSize:    defaultOutboxBatchSize,
		PollInterval: defaultOutboxPollInterval,
		RetryBackoff: DefaultOutboxRetryBackoff,
		Now:          time.Now,
	}
}

// DefaultOutboxRetryBackoff returns a capped exponential retry delay.
func DefaultOutboxRetryBackoff(message OutboxMessage, cause error) time.Duration {
	attempt := message.Attempts
	if attempt <= 1 {
		return defaultOutboxRetryBackoff
	}
	if attempt > 31 {
		return defaultOutboxRetryCap
	}

	delay := defaultOutboxRetryBackoff * time.Duration(1<<(attempt-1))
	if delay > defaultOutboxRetryCap {
		return defaultOutboxRetryCap
	}
	return delay
}

// OutboxProcessor claims due messages and publishes them.
type OutboxProcessor struct {
	repository OutboxRepository
	publisher  OutboxPublisher
	options    OutboxProcessorOptions
}

// OutboxBatchResult reports the result of a single processing pass.
type OutboxBatchResult struct {
	Claimed   int
	Published int
	Failed    int
}

// NewOutboxProcessor creates an outbox processor.
func NewOutboxProcessor(
	repository OutboxRepository,
	publisher OutboxPublisher,
	options ...OutboxProcessorOptions,
) *OutboxProcessor {
	opts := DefaultOutboxProcessorOptions()
	if len(options) > 0 {
		opts = mergeOutboxProcessorOptions(opts, options[0])
	}

	return &OutboxProcessor{
		repository: repository,
		publisher:  publisher,
		options:    opts,
	}
}

// ProcessBatch claims and publishes one batch of due messages.
func (p *OutboxProcessor) ProcessBatch(ctx context.Context) (OutboxBatchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || p.repository == nil {
		return OutboxBatchResult{}, errors.New("nil outbox repository")
	}
	if p.publisher == nil {
		return OutboxBatchResult{}, errors.New("nil outbox publisher")
	}
	if err := ctx.Err(); err != nil {
		return OutboxBatchResult{}, err
	}

	now := p.options.Now().UTC()
	messages, err := p.repository.ClaimDue(ctx, p.options.BatchSize, now)
	if err != nil {
		return OutboxBatchResult{}, err
	}

	result := OutboxBatchResult{Claimed: len(messages)}
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		if err := p.publisher.Publish(ctx, message); err != nil {
			delay := p.options.RetryBackoff(message, err)
			if delay < 0 {
				delay = 0
			}
			retryAt := p.options.Now().UTC().Add(delay)
			if markErr := p.repository.MarkFailed(ctx, message.ID, retryAt, err); markErr != nil {
				return result, markErr
			}
			result.Failed++
			continue
		}

		if err := p.repository.MarkPublished(ctx, message.ID); err != nil {
			return result, err
		}
		result.Published++
	}

	return result, nil
}

// Run processes due outbox messages until the context is canceled.
func (p *OutboxProcessor) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if _, err := p.ProcessBatch(ctx); err != nil {
			return err
		}

		timer := time.NewTimer(p.options.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func mergeOutboxProcessorOptions(
	base OutboxProcessorOptions,
	override OutboxProcessorOptions,
) OutboxProcessorOptions {
	if override.BatchSize > 0 {
		base.BatchSize = override.BatchSize
	}
	if override.PollInterval > 0 {
		base.PollInterval = override.PollInterval
	}
	if override.RetryBackoff != nil {
		base.RetryBackoff = override.RetryBackoff
	}
	if override.Now != nil {
		base.Now = override.Now
	}
	return base
}

func validateOutboxMessage(message OutboxMessage) error {
	if message.ID == "" || message.Subject == "" {
		return ErrInvalidOutboxMessage
	}
	return nil
}

func cloneOutboxMessage(message OutboxMessage) OutboxMessage {
	cloned := message
	cloned.Payload = append([]byte(nil), message.Payload...)
	cloned.Headers = cloneStringMap(message.Headers)
	cloned.Metadata = cloneStringMap(message.Metadata)
	if message.PublishedAt != nil {
		publishedAt := *message.PublishedAt
		cloned.PublishedAt = &publishedAt
	}
	return cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
