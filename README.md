# Relaybox

[![CI](https://github.com/KevinHernot/relaybox/actions/workflows/ci.yml/badge.svg)](https://github.com/KevinHernot/relaybox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/tag/KevinHernot/relaybox?label=release)](https://github.com/KevinHernot/relaybox/releases)

Reliable event delivery primitives for Go services.

`relaybox` is an early open-source extraction from Hopen's backend reliability layer. The initial release focuses on one narrow, useful slice: building idempotent NATS consumers that can survive retries, redeliveries, and reordered JSON payloads without reprocessing the same event twice.

## Status

Experimental, but real and usable.

## What Is Included Today

- stable event ID extraction from NATS headers and JSON payloads
- canonical JSON hashing fallback for unordered payloads
- an idempotent NATS handler
- an in-memory claim store for tests, examples, and local tools

## Quick Start

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

## Package Focus

`relaybox` is intentionally small for now. It does not yet include Hopen's full outbox, delayed-queue, or persistence integrations.

The next extraction candidates are documented in [docs/EXTRACTION_PLAN.md](docs/EXTRACTION_PLAN.md).

## Development

```bash
go test ./...
```

## Examples

- [example/basic_consumer](example/basic_consumer)
- [docs/API.md](docs/API.md)

## License

[MIT](LICENSE)
