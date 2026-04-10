# API Overview

## Core Types

### `Store`

The `Store` interface tracks whether an event has already been claimed or completed.

Methods:

- `Claim(ctx, key, ttl)` reports whether the key was acquired, is still in progress, or is already done
- `MarkDone(ctx, key, ttl)` keeps duplicates suppressed after success
- `Release(ctx, key)` frees the key after a failed attempt

### `MemoryStore`

`MemoryStore` is the built-in in-process `Store` implementation.

Use it for:

- tests
- examples
- local CLIs
- single-process consumers

Avoid it for:

- horizontally scaled consumers
- multi-process deployment
- long-lived durable dedup requirements

### `IdempotentHandler`

`IdempotentHandler` wraps a `func(context.Context, []byte) error` and makes it safe to use with NATS redelivery.

Creation:

```go
handler := relaybox.NewIdempotentHandler(store, func(ctx context.Context, data []byte) error {
	return nil
})
```

Execution:

```go
err := handler.Handle(msg)
```

## Event ID Extraction

`ExtractStableEventID` resolves identifiers in this order:

1. `Nats-Msg-Id`
2. `Ce-Id` / `ce-id`
3. JSON `id`
4. JSON `message_id`
5. deterministic hash of normalized JSON bytes

That last fallback is important because it means these payloads resolve to the same ID:

```json
{"a":1,"b":2}
{"b":2,"a":1}
```

## Handler Options

`HandlerOptions` supports:

- `KeyPrefix`
- `ClaimTTL`
- `ProcessedTTL`
- `AckWait`
- `SafetyMargin`

Use `AckWait` when you are consuming from JetStream and want each delivery attempt to time out slightly before the configured redelivery window.
