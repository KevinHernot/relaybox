package relaybox

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
)

func TestExtractStableEventIDPrefersNATSHeader(t *testing.T) {
	msg := &nats.Msg{Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, "event-123")

	got, err := ExtractStableEventID(msg)
	if err != nil {
		t.Fatalf("ExtractStableEventID returned error: %v", err)
	}

	if got != "nats:event-123" {
		t.Fatalf("expected header-based ID, got %q", got)
	}
}

func TestExtractStableEventIDNormalizesJSONHash(t *testing.T) {
	msgA := &nats.Msg{Data: []byte(`{"b":2,"a":1}`)}
	msgB := &nats.Msg{Data: []byte(`{"a":1,"b":2}`)}

	idA, err := ExtractStableEventID(msgA)
	if err != nil {
		t.Fatalf("ExtractStableEventID(msgA) error: %v", err)
	}

	idB, err := ExtractStableEventID(msgB)
	if err != nil {
		t.Fatalf("ExtractStableEventID(msgB) error: %v", err)
	}

	if idA != idB {
		t.Fatalf("expected equal IDs for semantically identical JSON, got %q and %q", idA, idB)
	}
}

func TestIdempotentHandlerSkipsDuplicateMessages(t *testing.T) {
	store := NewMemoryStore()
	calls := 0

	handler := NewIdempotentHandler(store, func(ctx context.Context, data []byte) error {
		calls++
		return nil
	})

	msg := &nats.Msg{
		Subject: "orders.created",
		Data:    []byte(`{"id":"evt-1","event_type":"order_created"}`),
	}

	if err := handler.Handle(msg); err != nil {
		t.Fatalf("first handle failed: %v", err)
	}
	if err := handler.Handle(msg); err != nil {
		t.Fatalf("second handle failed: %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected handler to run once, ran %d times", calls)
	}
}

func TestIdempotentHandlerReturnsErrorForInFlightDuplicate(t *testing.T) {
	store := NewMemoryStore()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32

	handler := NewIdempotentHandler(store, func(ctx context.Context, data []byte) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return errors.New("boom")
		}
		return nil
	})

	msg := &nats.Msg{
		Subject: "orders.created",
		Data:    []byte(`{"id":"evt-2","event_type":"order_created"}`),
	}

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- handler.Handle(msg)
	}()

	<-started

	if err := handler.Handle(msg); !errors.Is(err, ErrEventInProgress) {
		t.Fatalf("expected ErrEventInProgress, got %v", err)
	}

	close(release)

	if err := <-firstResult; err == nil {
		t.Fatal("expected first handle to fail")
	}

	if err := handler.Handle(msg); err != nil {
		t.Fatalf("expected retry after release to succeed, got %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected handler to run twice, ran %d times", got)
	}
}

func TestIdempotentHandlerReleasesClaimWhenHandlerFails(t *testing.T) {
	store := NewMemoryStore()
	calls := 0

	handler := NewIdempotentHandler(store, func(ctx context.Context, data []byte) error {
		calls++
		if calls == 1 {
			return errors.New("boom")
		}
		return nil
	})

	msg := &nats.Msg{
		Subject: "orders.created",
		Data:    []byte(`{"id":"evt-2","event_type":"order_created"}`),
	}

	if err := handler.Handle(msg); err == nil {
		t.Fatal("expected first handle to fail")
	}
	if err := handler.Handle(msg); err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}

	if calls != 2 {
		t.Fatalf("expected handler to run twice, ran %d times", calls)
	}
}

func TestExtractStableEventIDDoesNotCollapseDistinctAggregateEvents(t *testing.T) {
	msgA := &nats.Msg{Data: []byte(`{"aggregate_id":"order-1","event_type":"order_updated","version":1}`)}
	msgB := &nats.Msg{Data: []byte(`{"aggregate_id":"order-1","event_type":"order_updated","version":2}`)}

	idA, err := ExtractStableEventID(msgA)
	if err != nil {
		t.Fatalf("ExtractStableEventID(msgA) error: %v", err)
	}

	idB, err := ExtractStableEventID(msgB)
	if err != nil {
		t.Fatalf("ExtractStableEventID(msgB) error: %v", err)
	}

	if idA == idB {
		t.Fatalf("expected distinct IDs for distinct aggregate events, got %q", idA)
	}
}

func TestDeriveAckWaitContextUsesCurrentDeliveryWindow(t *testing.T) {
	ctx, cancel := deriveAckWaitContext(context.Background(), 5*time.Second, time.Second)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected derived context to have a deadline")
	}

	remaining := time.Until(deadline)
	if remaining < 3500*time.Millisecond || remaining > 4500*time.Millisecond {
		t.Fatalf("expected remaining deadline close to 4s, got %v", remaining)
	}
}
