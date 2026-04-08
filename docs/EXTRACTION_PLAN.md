# Relaybox Extraction Plan

## Goal

Extract Hopen's backend event reliability layer into a standalone Go module with clean boundaries and minimal product coupling.

## First Migration Slice

- `hopenbackend/pkg/outbox/outbox.go`
- `hopenbackend/pkg/outbox/processor.go`
- `hopenbackend/pkg/events/delayed_queue.go`
- `hopenbackend/pkg/events/idempotent_handler.go`

## Expected Refactors

- replace `hopenbackend/...` imports with local package boundaries
- split storage-specific code from package-level interfaces
- decouple logging and metrics from Hopen-specific naming
- isolate NATS and Valkey integrations behind portable interfaces where helpful

## Nice Follow-Ups

- example app with PostgreSQL + NATS
- migration guide from in-house outbox processors
- benchmarks and failure-mode documentation

