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

## Outbox

### `OutboxMessage`

`OutboxMessage` is a portable event envelope for transactional outbox storage.

Required fields:

- `ID`
- `Subject`
- `Payload`

Common optional fields:

- `Headers`
- `AggregateID`
- `AggregateType`
- `Metadata`
- `MaxAttempts`
- `AvailableAt`

Creation:

```go
message := relaybox.NewOutboxMessage(
	"evt-123",
	"orders.created",
	[]byte(`{"order_id":"order-1"}`),
	relaybox.WithOutboxAggregate("order", "order-1"),
	relaybox.WithOutboxHeaders(map[string]string{
		"traceparent": traceparent,
	}),
)
```

### `OutboxRepository`

`OutboxRepository` stores and updates outbox messages.

Methods:

- `Add(ctx, message)` stores a pending message
- `ClaimDue(ctx, limit, now)` atomically claims messages ready to publish
- `MarkPublished(ctx, id)` records a successful publish
- `MarkFailed(ctx, id, retryAt, cause)` records a failed publish and schedules a retry when attempts remain
- `Release(ctx, id, availableAt)` returns a claimed message to the pending state

### `MemoryOutboxRepository`

`MemoryOutboxRepository` is the built-in in-process implementation.

Use it for:

- tests
- examples
- local CLIs
- single-process demos

Avoid it for:

- transactional durability
- horizontally scaled processors
- cross-process delayed delivery

### `OutboxPublisher`

`OutboxPublisher` publishes a claimed message.

```go
type OutboxPublisher interface {
	Publish(ctx context.Context, message relaybox.OutboxMessage) error
}
```

The built-in `NATSOutboxPublisher` publishes to Core NATS and sets `Nats-Msg-Id` from the outbox message ID when the header is not already present.

### `OutboxProcessor`

`OutboxProcessor` claims due messages, publishes them, marks success, and records retry state after failures.

```go
processor := relaybox.NewOutboxProcessor(repo, publisher)

result, err := processor.ProcessBatch(ctx)
```

`OutboxBatchResult` reports:

- `Claimed`
- `Published`
- `Failed`

Use `Run(ctx)` for a simple polling loop.

## Delayed Queue

`DelayedQueue` schedules messages by setting `OutboxMessage.AvailableAt`. When a message becomes due, the normal outbox processor publishes it.

Schedule for a fixed time:

```go
err := queue.ScheduleAt(ctx, "evt-124", "orders.reminder", payload, runAt)
```

Schedule after a duration:

```go
err := queue.ScheduleAfter(ctx, "evt-125", "orders.reminder", payload, 15*time.Minute)
```
