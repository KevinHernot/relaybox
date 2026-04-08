package relaybox

import (
	"context"
	"errors"
	"testing"

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
