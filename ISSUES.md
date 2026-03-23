# Codebase Issues Audit

## Audits

| #   | Date       | Description                                                        | New Issues                   | False Positives |
| --- | ---------- | ------------------------------------------------------------------ | ---------------------------- | --------------- |
| 1   | 2026-03-23 | Full codebase audit — all source files, tests, types, build config | 1 CRIT, 3 HIGH, 4 MED, 3 LOW | 0               |

---

## Fixed Issues

0 issues fixed across 0 audit sessions:

| ID  | Issue | Commit |
| --- | ----- | ------ |

## False Positives Removed

0 issues removed after verification:

| Original ID | Why Removed |
| ----------- | ----------- |

---

## Table of Contents

- [Audits](#audits)
- [Fixed Issues](#fixed-issues)
- [False Positives Removed](#false-positives-removed)
- [CRITICAL Issues (1 remaining)](#critical-issues)
- [HIGH Issues (3 remaining)](#high-issues)
- [MEDIUM Issues (4 remaining)](#medium-issues)
- [LOW Issues (3 remaining)](#low-issues)
- [Summary](#summary)

---

## CRITICAL Issues

### CRIT-01: Publish route has no authentication — allows arbitrary event injection

**File:** `src/event-bus/index.ts:32-78`
**Severity:** CRITICAL (any unauthenticated HTTP client can publish arbitrary events into the bus, triggering any registered handler with any payload)

The `/event-bus/publish/:event` POST endpoint is registered without any authentication, authorization, or rate limiting. Any client that can reach the server can inject events. The only guard is the `disableEventPublishRoute` option, which defaults to `undefined` (falsy), meaning the route is **enabled by default**.

This is dangerous because event handlers typically perform business-critical operations (order processing, file handling, etc.), and an attacker can trigger them with crafted payloads.

**Suggested fix:** Either: (1) Default `disableEventPublishRoute` to `true` and require explicit opt-in, or (2) Add a required authentication hook/middleware to this route, or (3) At minimum, document the security implications prominently and add a warning log on registration when the route is enabled.

---

## HIGH Issues

### HIGH-01: Weak truthiness check in `getHandlerMap` silently swallows falsy handlers

**File:** `src/event-bus/commons.ts:37`
**Severity:** HIGH (if a handler function is accidentally set to `undefined`, `null`, `0`, or `""` in a handler map, the `if (handler as any)` check silently skips it instead of throwing an error, making handler registration bugs invisible)

```typescript
if (handler as any) {
  handlerMap.get(key)!.push({ file, handler });
}
```

The `as any` cast defeats TypeScript's type checking. A handler that is `undefined` (e.g., due to a typo in a re-export) would be silently ignored. The developer would see no error but the event would never be processed.

**Suggested fix:** Either remove the check entirely (TypeScript enforces the type) or throw an explicit error:

```typescript
if (!handler) {
  throw new Error(`Handler for event "${key}" in file "${file}" is ${handler}`);
}
```

---

### HIGH-02: `processError` return value `err` is used but may lack `.message` property

**File:** `src/event-bus/commons.ts:110-118`
**Severity:** HIGH (if `processError` returns an `err` without a `.message` property, the `ErrorWithStatus` message becomes `"file-handler failed with undefined"`, making production debugging very difficult)

```typescript
({ err, status } = options.processError(err, ctx));
if (!ctx.specifiedFile) {
  ctx.publishToPubSub(ctx.eventMsg.event, ctx.eventMsg.data, ctx.file);
} else {
  throw new ErrorWithStatus(
    status,
    `${ctx.file}-${ctx.handler.name} failed with ${err.message}`,
  );
}
```

The `processError` interface returns `{ err: any; status: number }`. Since `err` is typed as `any`, accessing `.message` is unsafe. If the caller's `processError` returns a plain string or object without `.message`, the error message is misleading.

**Suggested fix:** Guard the message construction: `err?.message ?? String(err)`.

---

### HIGH-03: FastifyInstance type augmentation missing for `EventBus` and `FileStore`

**File:** `src/types.ts:3-12`
**Severity:** HIGH (TypeScript does not know about `fastify.EventBus` or `fastify.FileStore` on the FastifyInstance — only `FastifyRequest.EventBus` is augmented. This forces users to use `(fastify as any).EventBus` or get type errors)

The `declare module "fastify"` block only augments `FastifyRequest` with `EventBus`. It does not augment `FastifyInstance` with either `EventBus` or `FileStore`. The test files confirm this: `integration.spec.ts:156-157` uses `(fastify1 as any).FileStore`.

**Suggested fix:** Add to the module augmentation:

```typescript
export interface FastifyInstance {
  EventBus: EventBus;
  FileStore: import("./file-store").FileStore;
  _hasEventHandlers: boolean;
}
```

---

## MEDIUM Issues

### MED-01: Prometheus metrics created without checking for duplicate registration

**File:** `src/event-bus/commons.ts:137-154`
**Severity:** MEDIUM (if `CreateHandlerRunner` is called multiple times with the same registry, `prom-client` will throw "A metric with the name event_handler_latency_ms has already been registered", crashing the application)

The `Histogram` and `Counter` are created with fixed names `event_handler_latency_ms` and `event_handler_latency_total`. If the event bus plugin is registered in multiple Fastify encapsulation contexts sharing the same `registry`, the second registration will throw.

**Suggested fix:** Either use `prom.register.getSingleMetric()` to check if the metric already exists, or catch the duplicate registration error and reuse the existing metric.

---

### MED-02: NATS JetStream publisher drops messages silently under backpressure

**File:** `src/event-bus/nats-jetstream.ts:119-126`
**Severity:** MEDIUM (when `inflight.size >= MAX_INFLIGHT`, the publish function logs an error and returns without publishing — the message is permanently lost with no retry or dead-letter mechanism)

```typescript
if (inflight.size >= MAX_INFLIGHT) {
  f.log.error({
    tag: "NATS_JETSTREAM_PUBLISH_BACKPRESSURE",
    event,
    inflightCount: inflight.size,
  });
  return; // message is dropped
}
```

While MAX_INFLIGHT is 10,000, under sustained load this could drop events. Unlike the error path (which retries 3 times), the backpressure path has no recovery.

**Suggested fix:** Either throw an error so callers know the publish failed, or implement a bounded queue with backpressure signaling. At minimum, add a Prometheus counter for dropped messages so operators can alert on it.

---

### MED-03: GCP Pub/Sub consumer mutates `process.env.EVENT_SUBSCRIPTION` at runtime

**File:** `src/event-bus/event-consumer/gcp-pubsub.ts:17-31`
**Severity:** MEDIUM (the consumer builder writes to `process.env.EVENT_SUBSCRIPTION` during auto-discovery, which is a global side-effect that can cause race conditions if multiple consumers are created concurrently or if other code reads this env var)

```typescript
if (
  process.env.EVENT_TOPIC &&
  !process.env.EVENT_SUBSCRIPTION &&
  process.env.APP
) {
  // ...
  for (const sub of subs[0]) {
    if (sub.name.includes(process.env.APP)) {
      process.env.EVENT_SUBSCRIPTION = sub.name; // global mutation
    }
  }
}
```

**Suggested fix:** Use a local variable instead of mutating `process.env`. Pass the discovered subscription name through the function's scope rather than the global environment.

---

### MED-04: Counter metric name `event_handler_latency_total` is misleading

**File:** `src/event-bus/commons.ts:149-153`
**Severity:** MEDIUM (the counter is named `event_handler_latency_total` but it counts handler invocations, not latency — this violates Prometheus naming conventions and will confuse operators)

The histogram already tracks latency (`event_handler_latency_ms`). The counter should be named something like `event_handler_invocations_total` to reflect what it actually measures.

**Suggested fix:** Rename to `event_handler_invocations_total` with `help: "Total number of event handler invocations"`.

---

## LOW Issues

### LOW-01: `RABBITMQ_TAG` uses weak randomness for consumer tag

**File:** `src/event-bus/rabbitmq-utils.ts:3`
**Severity:** LOW (using `Math.random()` for the consumer tag could theoretically produce collisions across instances, though the probability is very low with 1e9 range)

```typescript
export const RABBITMQ_TAG = "" + Math.floor(Math.random() * 1e9);
```

**Suggested fix:** Use `crypto.randomUUID()` or `crypto.randomInt()` for stronger uniqueness guarantees.

---

### LOW-02: Event consumer module has very low test coverage (18.6%)

**File:** `src/event-bus/event-consumer/` (all files except `utils.ts`)
**Severity:** LOW (the consumer implementations for RabbitMQ, GCP Pub/Sub, Azure ServiceBus, and NATS JetStream have 0% branch coverage and ~10-13% line coverage — only covered by Docker-based integration tests)

While the integration tests in `event-bus.integration.spec.ts` cover these with real brokers, the unit test suite has no coverage for consumer logic (retry behavior, DLQ routing, error handling).

**Suggested fix:** Add unit tests using `fastify.inject()` mocking patterns similar to the publisher route tests, or at least document that coverage relies on integration tests that require Docker.

---

### LOW-03: Duplicate test description in integration spec

**File:** `src/integration.spec.ts:41` and `src/integration.spec.ts:64`
**Severity:** LOW (two tests have the identical description "should register both plugins successfully", making test reports ambiguous)

**Suggested fix:** Rename the second test to something like "should register both plugins and perform basic operations".

---

## Summary

| Severity                    | Remaining   |
| --------------------------- | ----------- |
| **CRITICAL**                | 1           |
| **HIGH**                    | 3           |
| **MEDIUM**                  | 4           |
| **LOW**                     | 3           |
| **TOTAL**                   | **11 open** |
| **Fixed**                   | 0           |
| **False Positives Removed** | 0           |
