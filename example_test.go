package relaybox_test

import (
	"context"
	"fmt"

	nats "github.com/nats-io/nats.go"

	"github.com/kevinhernot/relaybox"
)

type exampleOutboxPublisher struct{}

func (exampleOutboxPublisher) Publish(ctx context.Context, message relaybox.OutboxMessage) error {
	fmt.Printf("published %s to %s\n", message.ID, message.Subject)
	return nil
}

func ExampleNewIdempotentHandler() {
	store := relaybox.NewMemoryStore()
	processed := 0

	handler := relaybox.NewIdempotentHandler(
		store,
		func(ctx context.Context, data []byte) error {
			processed++
			fmt.Printf("processed payload: %s\n", string(data))
			return nil
		},
	)

	msg := &nats.Msg{
		Subject: "orders.created",
		Data:    []byte(`{"id":"evt-123","event_type":"order_created"}`),
	}

	_ = handler.Handle(msg)
	_ = handler.Handle(msg)

	fmt.Printf("handler runs: %d\n", processed)
	// Output:
	// processed payload: {"id":"evt-123","event_type":"order_created"}
	// handler runs: 1
}

func ExampleNewOutboxProcessor() {
	ctx := context.Background()
	repo := relaybox.NewMemoryOutboxRepository()
	processor := relaybox.NewOutboxProcessor(repo, exampleOutboxPublisher{})

	message := relaybox.NewOutboxMessage(
		"evt-456",
		"orders.created",
		[]byte(`{"order_id":"order-1"}`),
	)

	_ = repo.Add(ctx, message)
	result, _ := processor.ProcessBatch(ctx)

	fmt.Printf("published count: %d\n", result.Published)
	// Output:
	// published evt-456 to orders.created
	// published count: 1
}
