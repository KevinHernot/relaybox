# Relaybox

[![CI](https://github.com/KevinHernot/relaybox/actions/workflows/ci.yml/badge.svg)](https://github.com/KevinHernot/relaybox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/tag/KevinHernot/relaybox?label=release)](https://github.com/KevinHernot/relaybox/releases)

Reliable event delivery primitives for Go services.

`relaybox` is an open-source extraction from Hopen's backend reliability layer. It focuses on the small pieces teams keep rebuilding around event-driven services: idempotent consumers, transactional outbox processing, delayed delivery, and broker-facing publish adapters.

## Status

Experimental, but real and usable.

## What Is Included Today

- stable event ID extraction from NATS headers and JSON payloads
- canonical JSON hashing fallback for unordered payloads
- an idempotent NATS handler
- an in-memory claim store for tests, examples, and local tools
- portable transactional outbox interfaces
- an in-memory outbox repository
- an outbox processor with retry/backoff handling
- delayed message scheduling built on the outbox lifecycle
- a Core NATS outbox publisher with `Nats-Msg-Id` dedup headers

## Quick Start

### Idempotent NATS Handler

```go
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
			fmt.Printf("processing: %s\n", string(data))
			return nil
		},
	)

	msg := &nats.Msg{
		Subject: "orders.created",
		Data:    []byte(`{"id":"evt-123","event_type":"order_created"}`),
	}

	_ = handler.Handle(msg)
	_ = handler.Handle(msg) // Duplicate: skipped.
}
```

### Outbox Processor

```go
repo := relaybox.NewMemoryOutboxRepository()
publisher := relaybox.NewNATSOutboxPublisher(natsConn)
processor := relaybox.NewOutboxProcessor(repo, publisher)

message := relaybox.NewOutboxMessage(
	"evt-123",
	"orders.created",
	[]byte(`{"order_id":"order-1"}`),
	relaybox.WithOutboxAggregate("order", "order-1"),
)

_ = repo.Add(context.Background(), message)
_, _ = processor.ProcessBatch(context.Background())
```

### Delayed Queue

```go
queue := relaybox.NewDelayedQueue(repo)

_ = queue.ScheduleAfter(
	context.Background(),
	"evt-124",
	"orders.reminder",
	[]byte(`{"order_id":"order-1"}`),
	15*time.Minute,
)
```

## Package Focus

`relaybox` keeps storage and broker integrations behind small interfaces. The built-in memory stores are intentionally simple; production services should provide durable implementations for `Store` and `OutboxRepository`.

The extraction roadmap is documented in [docs/EXTRACTION_PLAN.md](docs/EXTRACTION_PLAN.md).

## Development

```bash
go test ./...
```

## Examples

- [example/basic_consumer](example/basic_consumer)
- [docs/API.md](docs/API.md)

## License

[MIT](LICENSE)
