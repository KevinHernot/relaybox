package relaybox_test

import (
	"context"
	"fmt"

	nats "github.com/nats-io/nats.go"

	"github.com/kevinhernot/relaybox"
)

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
