package main

import (
	"context"
	"fmt"

	nats "github.com/nats-io/nats.go"

	"github.com/kevinhernot/relaybox"
)

func main() {
	store := relaybox.NewMemoryStore()
	handler := relaybox.NewIdempotentHandler(
		store,
		func(ctx context.Context, data []byte) error {
			fmt.Printf("processing payload: %s\n", string(data))
			return nil
		},
	)

	msg := &nats.Msg{
		Subject: "payments.captured",
		Data:    []byte(`{"id":"evt-42","event_type":"payment_captured"}`),
	}

	_ = handler.Handle(msg)
	_ = handler.Handle(msg)

	fmt.Println("done: duplicate message was safely skipped")
}
