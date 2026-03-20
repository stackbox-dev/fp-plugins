# Event Bus: NATS Driver, Bug Fixes & Comprehensive Integration Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add NATS JetStream driver to the event-bus plugin, fix the pre-existing `convert()` bug, and add comprehensive Docker-based integration tests for all drivers (RabbitMQ, NATS, GCP Pub/Sub emulator, Azure Service Bus emulator).

**Architecture:** NATS driver follows the same plugin pattern as existing brokers — Fastify plugin for publishing + EventConsumerBuilder for consuming. Integration tests are self-contained: they start Docker containers, run tests, and clean up (including on Ctrl+C / crash). Each driver gets a full test suite: single publish, batch, round-trip with handler, delayed message, error handling.

**Tech Stack:** TypeScript, Fastify, @nats-io/transport-node, @nats-io/jetstream, @nats-io/nats-core, rabbitmq-client, @google-cloud/pubsub, @azure/service-bus, Jest, Docker (testcontainers not used — direct `docker run` for more control)

**Constraints:**
- `EventBus.publish()` stays `void`
- New `busType: "nats-jetstream"` added to union type
- All existing tests must continue to pass

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/event-bus/interfaces.ts` | **Modify** | Add `"nats-jetstream"` to busType union |
| `src/event-bus/nats-jetstream.ts` | **Create** | NATS JetStream publisher plugin |
| `src/event-bus/event-consumer/nats-jetstream.ts` | **Create** | NATS JetStream consumer |
| `src/event-bus/index.ts` | **Modify** | Add nats-jetstream case to switch |
| `src/event-bus/event-consumer/index.ts` | **Modify** | Add nats-jetstream case to switch |
| `src/event-bus/rabbitmq.ts` | **Modify** | Fix convert() outside try-catch |
| `src/event-bus/azure-servicebus.ts` | **Modify** | Fix convert() outside try-catch, add JSON parse error handling |
| `src/event-bus/event-bus.integration.spec.ts` | **Create** | Docker-based integration tests for all drivers |
| `package.json` | **Modify** | Add NATS dependencies, test:integration script |

---

### Task 1: Fix convert() bug in RabbitMQ and Azure ServiceBus publishers

**Files:**
- Modify: `src/event-bus/rabbitmq.ts`
- Modify: `src/event-bus/azure-servicebus.ts`

The `convert(rawMsg)` call is outside the try-catch block in both message processing routes. A JSON parse error crashes the route handler. Additionally, Azure's `convert()` doesn't wrap `JSON.parse` in try-catch like RabbitMQ's does.

- [ ] **Step 1: Fix RabbitMQ — move convert() inside try-catch**

In `src/event-bus/rabbitmq.ts`, the route handler currently does:
```ts
      const msg = convert(rawMsg);      // line ~38 — OUTSIDE try-catch
      options.validateMsg(msg.event, msg.data, req);
      // ...
      try {
        await selectAndRunHandlers(...)  // line ~48 — INSIDE try-catch
```

Move `convert()` and `validateMsg()` inside the try-catch, and handle `ErrorWithStatus` from convert:

Find the route handler's body (after the `if (!rawMsg)` check) and restructure so `convert` is inside the try block. The entire processing block after the null check should be wrapped in try-catch.

- [ ] **Step 2: Fix Azure — add JSON parse error handling to convert() and move inside try-catch**

In `src/event-bus/azure-servicebus.ts`, the `convert()` function does `JSON.parse(msg.body)` without try-catch. Add the same pattern as RabbitMQ's convert:

```ts
function convert(msg: IncomingServiceBusMessage): EventMessage {
  let body: MessageBody;
  try {
    body = JSON.parse(msg.body);
  } catch {
    throw new ErrorWithStatus(
      400,
      `Invalid JSON in message body: ${typeof msg.body === 'string' ? msg.body.substring(0, 100) : 'non-string'}`,
    );
  }
  return {
    id: "" + msg.messageId,
    // ... rest unchanged
  };
}
```

Also move the `convert()` call inside the try-catch in the route handler, same as RabbitMQ.

- [ ] **Step 3: Run tests**

Run: `npx jest --testPathIgnorePatterns=rabbitmq.spec.ts`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/event-bus/rabbitmq.ts src/event-bus/azure-servicebus.ts
git commit -m "fix(event-bus): move convert() inside try-catch, add JSON parse error handling"
```

---

### Task 2: Add NATS dependencies

**Files:**
- Modify: `package.json`

- [ ] **Step 1: Install NATS packages**

```bash
pnpm add @nats-io/transport-node @nats-io/jetstream @nats-io/nats-core
```

- [ ] **Step 2: Commit**

```bash
git add package.json pnpm-lock.yaml
git commit -m "chore: add NATS JetStream dependencies"
```

---

### Task 3: Add "nats-jetstream" to busType and wire up factory

**Files:**
- Modify: `src/event-bus/interfaces.ts`
- Modify: `src/event-bus/index.ts`
- Modify: `src/event-bus/event-consumer/index.ts`

- [ ] **Step 1: Add busType**

In `src/event-bus/interfaces.ts`, change:
```ts
busType: "rabbitmq" | "gcp-pubsub" | "azure-servicebus" | "in-process";
```
To:
```ts
busType: "rabbitmq" | "gcp-pubsub" | "azure-servicebus" | "nats-jetstream" | "in-process";
```

- [ ] **Step 2: Add case to publisher factory**

In `src/event-bus/index.ts`, add a case before `default`:
```ts
    case "nats-jetstream":
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      await f.register(require("./nats-jetstream"), options);
      break;
```

- [ ] **Step 3: Add case to consumer factory**

In `src/event-bus/event-consumer/index.ts`, add import:
```ts
import { NatsJetStreamConsumerBuilder } from "./nats-jetstream";
```

Add case:
```ts
    case "nats-jetstream":
      return NatsJetStreamConsumerBuilder(instance);
```

- [ ] **Step 4: Commit**

```bash
git add src/event-bus/interfaces.ts src/event-bus/index.ts src/event-bus/event-consumer/index.ts
git commit -m "feat(event-bus): add nats-jetstream to busType union and factory wiring"
```

---

### Task 4: Create NATS JetStream publisher plugin

**Files:**
- Create: `src/event-bus/nats-jetstream.ts`

This follows the exact same pattern as the other publishers. Key design decisions:
- Publishes to subject `{topic}.{event}` (topic from options, event from publish call)
- Message body is JSON `{ event, payload, file, processAfterDelayMs, publishTimestamp }`
- Headers carry metadata: `event`, `file`, `processAfterDelayMs`
- Fire-and-forget to preserve sync `publish()` interface
- Connection error handling via NATS status monitoring
- safeAll for shutdown

The implementer should read `src/event-bus/rabbitmq.ts` as a reference for the exact pattern, then adapt for NATS JetStream. Key differences from RabbitMQ:
- Uses `@nats-io/transport-node` `connect()` instead of `rabbitmq-client` `Connection`
- Uses `jetstream(nc)` to get JetStream client
- Publishes via `js.publish(subject, data, { headers })` instead of `publisher.send()`
- Headers use `headers()` from `@nats-io/nats-core`
- Connection env var: `NATS_SERVERS` (comma-separated)
- Topic env var: reuses `options.topic` from EventBusOptions
- Drain connection on close instead of close

Required env vars: `NATS_SERVERS`
Required options: `topic` (used as the JetStream subject prefix)

The message processing endpoint should be `/nats-jetstream/process-message` following the existing naming convention.

- [ ] **Step 1: Create the publisher plugin**

Create `src/event-bus/nats-jetstream.ts` following the rabbitmq.ts pattern but adapted for NATS JetStream. The file should:
- Import from `@nats-io/transport-node`, `@nats-io/jetstream`, `@nats-io/nats-core`
- Import safeAll from ./utils
- Use the same `IncomingMessage` / `MessageBody` / `convert()` pattern
- Register `/nats-jetstream/process-message` endpoint
- Fire-and-forget publish with `.catch()` error logging
- safeAll shutdown (drain nc)
- Connection error handling

- [ ] **Step 2: Verify build compiles**

Run: `pnpm run build`
Expected: Clean build

- [ ] **Step 3: Commit**

```bash
git add src/event-bus/nats-jetstream.ts
git commit -m "feat(event-bus): add NATS JetStream publisher plugin"
```

---

### Task 5: Create NATS JetStream consumer

**Files:**
- Create: `src/event-bus/event-consumer/nats-jetstream.ts`

Follows the same pattern as the RabbitMQ consumer. Key design:
- Connects to NATS, gets JetStream consumer by stream + durable name
- Pull-based consume loop with auto-reconnect
- Injects messages to `/nats-jetstream/process-message` endpoint
- Handles status codes: 2xx=ack, 425=nak+delay, 429/409=nak+randomDelay, 5xx=nak
- Abort-aware consume loop with exponential backoff on reconnect
- safeAll shutdown

Required env vars: `NATS_SERVERS`, `NATS_STREAM`, `NATS_CONSUMER`

The implementer should read `src/event-bus/event-consumer/rabbitmq.ts` and the NATS driver from the PR diff as references.

- [ ] **Step 1: Create the consumer**

Create `src/event-bus/event-consumer/nats-jetstream.ts` following the rabbitmq consumer pattern but adapted for NATS JetStream.

- [ ] **Step 2: Verify build compiles**

Run: `pnpm run build`
Expected: Clean build

- [ ] **Step 3: Run existing tests**

Run: `npx jest --testPathIgnorePatterns=rabbitmq.spec.ts`
Expected: PASS (all 77 tests)

- [ ] **Step 4: Commit**

```bash
git add src/event-bus/event-consumer/nats-jetstream.ts
git commit -m "feat(event-bus): add NATS JetStream consumer"
```

---

### Task 6: Create comprehensive Docker-based integration tests

**Files:**
- Create: `src/event-bus/event-bus.integration.spec.ts`

This is the big one. Model it after the PR's `drivers.integration.spec.ts` but adapted for the event-bus architecture. The tests are self-contained:
- Start Docker containers for each broker
- Clean up on exit (including Ctrl+C, crash)
- Test the full round-trip: register plugin → publish event → consumer picks up → handler fires

**Test structure per driver:**
1. Single publish + consume round-trip
2. Publish with delay (processAfterDelayMs) — verify 425 then process
3. Handler error retry — verify re-publish with file attribute
4. Multiple handlers for same event
5. Graceful shutdown (close without hanging)

**Docker images:**
- RabbitMQ: `rabbitmq:4-alpine` (port 5672)
- NATS: `nats:latest -js` (port 4222)
- GCP Pub/Sub: `google/cloud-sdk:emulators` with `gcloud beta emulators pubsub start`
- Azure Service Bus: `mcr.microsoft.com/azure-messaging/servicebus-emulator` + `mcr.microsoft.com/mssql/server:2022-latest`

**Infrastructure helpers** (same pattern as PR):
- `docker()` — execFileSync wrapper
- `dockerRun()` — run container with port mapping
- `dockerStop()` — stop container
- `waitForPort()` — poll until port is ready
- `cleanup()` — stop all containers, restore env vars
- Signal handlers for SIGINT/SIGTERM
- Orphan container cleanup

The implementer should read the PR's test file thoroughly (ask for it if needed) and adapt for the event-bus architecture. Key differences from the PR:
- Instead of a `Driver` class with `init()/send()/startReceiving()/close()`, we have Fastify plugins + EventConsumerBuilder
- Tests should use `fastify.register(Plugins.EventBus, options)` + `CreateEventConsumer(fastify, type)`
- Handlers are registered via `options.handlers`
- Events flow through the handler runner (commons.ts)

- [ ] **Step 1: Create the integration test file**

The test file should be comprehensive. Add a new npm script:
```json
"test:integration": "jest event-bus.integration.spec.ts --testTimeout=180000 --forceExit"
```

- [ ] **Step 2: Test RabbitMQ (Docker available locally)**

Run: `npx jest src/event-bus/event-bus.integration.spec.ts --testTimeout=180000 --forceExit -t "RabbitMQ"`
Expected: PASS

- [ ] **Step 3: Test NATS (Docker available locally)**

Run: `npx jest src/event-bus/event-bus.integration.spec.ts --testTimeout=180000 --forceExit -t "NATS"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/event-bus/event-bus.integration.spec.ts package.json
git commit -m "test(event-bus): add comprehensive Docker-based integration tests

Self-contained: starts brokers via Docker, runs tests, cleans up everything.
Handles crashes, Ctrl+C, and leftover containers from failed runs.
Covers: RabbitMQ, NATS JetStream, GCP Pub/Sub emulator, Azure SB emulator."
```

---

### Task 7: Verify build and all tests

- [ ] **Step 1: Run build**
Run: `pnpm run build`

- [ ] **Step 2: Run unit tests**
Run: `pnpm test`

- [ ] **Step 3: Run integration tests**
Run: `npx jest src/event-bus/event-bus.integration.spec.ts --testTimeout=180000 --forceExit`

- [ ] **Step 4: Run full RabbitMQ integration tests (existing)**
Run: `npx jest rabbitmq.spec.ts --testTimeout=120000`
