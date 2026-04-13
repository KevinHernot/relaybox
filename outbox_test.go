package relaybox

import (
	"context"
	"errors"
	"testing"
	"time"
)

type recordingOutboxPublisher struct {
	failuresLeft int
	failAlways   bool
	published    []OutboxMessage
}

func (p *recordingOutboxPublisher) Publish(ctx context.Context, message OutboxMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.failAlways || p.failuresLeft > 0 {
		if p.failuresLeft > 0 {
			p.failuresLeft--
		}
		return errors.New("publish failed")
	}
	p.published = append(p.published, cloneOutboxMessage(message))
	return nil
}

func TestMemoryOutboxRepositoryClaimsDueMessagesInOrder(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryOutboxRepository()
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	repo.now = func() time.Time { return now }

	future := NewOutboxMessage("future", "orders.future", []byte("{}"), WithOutboxAvailableAt(now.Add(time.Hour)))
	second := NewOutboxMessage("second", "orders.second", []byte("{}"), WithOutboxAvailableAt(now.Add(-time.Second)))
	first := NewOutboxMessage("first", "orders.first", []byte("{}"), WithOutboxAvailableAt(now.Add(-2*time.Second)))

	for _, message := range []OutboxMessage{future, second, first} {
		if err := repo.Add(ctx, message); err != nil {
			t.Fatalf("Add(%s) failed: %v", message.ID, err)
		}
	}

	claimed, err := repo.ClaimDue(ctx, 10, now)
	if err != nil {
		t.Fatalf("ClaimDue failed: %v", err)
	}

	if len(claimed) != 2 {
		t.Fatalf("expected 2 due messages, got %d", len(claimed))
	}
	if claimed[0].ID != "first" || claimed[1].ID != "second" {
		t.Fatalf("messages claimed out of order: %q then %q", claimed[0].ID, claimed[1].ID)
	}
	if claimed[0].Status != OutboxStatusProcessing || claimed[0].Attempts != 1 {
		t.Fatalf("expected first message to be processing with one attempt, got status=%s attempts=%d", claimed[0].Status, claimed[0].Attempts)
	}
}

func TestMemoryOutboxRepositoryRejectsDuplicateIDs(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryOutboxRepository()
	message := NewOutboxMessage("evt-1", "orders.created", []byte("{}"))

	if err := repo.Add(ctx, message); err != nil {
		t.Fatalf("first Add failed: %v", err)
	}
	if err := repo.Add(ctx, message); !errors.Is(err, ErrOutboxMessageExists) {
		t.Fatalf("expected ErrOutboxMessageExists, got %v", err)
	}
}

func TestOutboxProcessorPublishesDueMessages(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryOutboxRepository()
	publisher := &recordingOutboxPublisher{}
	processor := NewOutboxProcessor(repo, publisher)

	message := NewOutboxMessage(
		"evt-1",
		"orders.created",
		[]byte(`{"order_id":"order-1"}`),
		WithOutboxHeaders(map[string]string{"traceparent": "00-abc"}),
	)
	if err := repo.Add(ctx, message); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	result, err := processor.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}

	if result.Claimed != 1 || result.Published != 1 || result.Failed != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("expected one publish, got %d", len(publisher.published))
	}
	if publisher.published[0].Headers["traceparent"] != "00-abc" {
		t.Fatalf("expected headers to be preserved, got %+v", publisher.published[0].Headers)
	}

	stored, err := repo.Get(ctx, "evt-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if stored.Status != OutboxStatusPublished || stored.PublishedAt == nil {
		t.Fatalf("expected published message, got status=%s published_at=%v", stored.Status, stored.PublishedAt)
	}
}

func TestOutboxProcessorRetriesFailedPublishes(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryOutboxRepository()
	publisher := &recordingOutboxPublisher{failuresLeft: 1}
	processor := NewOutboxProcessor(repo, publisher, OutboxProcessorOptions{
		RetryBackoff: func(message OutboxMessage, cause error) time.Duration {
			return 0
		},
	})

	message := NewOutboxMessage("evt-1", "orders.created", []byte("{}"), WithOutboxMaxAttempts(3))
	if err := repo.Add(ctx, message); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	first, err := processor.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("first ProcessBatch failed: %v", err)
	}
	if first.Claimed != 1 || first.Published != 0 || first.Failed != 1 {
		t.Fatalf("unexpected first result: %+v", first)
	}

	stored, err := repo.Get(ctx, "evt-1")
	if err != nil {
		t.Fatalf("Get after failure failed: %v", err)
	}
	if stored.Status != OutboxStatusPending || stored.Attempts != 1 || stored.LastError == "" {
		t.Fatalf("expected pending retry with error, got status=%s attempts=%d error=%q", stored.Status, stored.Attempts, stored.LastError)
	}

	second, err := processor.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("second ProcessBatch failed: %v", err)
	}
	if second.Claimed != 1 || second.Published != 1 || second.Failed != 0 {
		t.Fatalf("unexpected second result: %+v", second)
	}
}

func TestOutboxProcessorMarksTerminalFailure(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryOutboxRepository()
	publisher := &recordingOutboxPublisher{failAlways: true}
	processor := NewOutboxProcessor(repo, publisher)

	message := NewOutboxMessage("evt-1", "orders.created", []byte("{}"), WithOutboxMaxAttempts(1))
	if err := repo.Add(ctx, message); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	result, err := processor.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if result.Claimed != 1 || result.Published != 0 || result.Failed != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	stored, err := repo.Get(ctx, "evt-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if stored.Status != OutboxStatusFailed || stored.Attempts != 1 {
		t.Fatalf("expected terminal failure, got status=%s attempts=%d", stored.Status, stored.Attempts)
	}
}

func TestDelayedQueueSchedulesFutureMessages(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryOutboxRepository()
	queue := NewDelayedQueue(repo)
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	repo.now = func() time.Time { return now }

	if err := queue.ScheduleAt(ctx, "evt-1", "orders.reminder", []byte("{}"), now.Add(time.Hour)); err != nil {
		t.Fatalf("ScheduleAt failed: %v", err)
	}

	claimed, err := repo.ClaimDue(ctx, 10, now)
	if err != nil {
		t.Fatalf("ClaimDue now failed: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected no due messages, got %d", len(claimed))
	}

	claimed, err = repo.ClaimDue(ctx, 10, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("ClaimDue future failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "evt-1" {
		t.Fatalf("expected scheduled message to become due, got %+v", claimed)
	}
}
