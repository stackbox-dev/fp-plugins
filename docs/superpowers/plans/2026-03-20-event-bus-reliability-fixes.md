# Event Bus Reliability Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix message loss, shutdown leaks, and receiver fragility in the event-bus plugin — mirroring the improvements from falcon-wms PR #12730 but adapted for this shared library's architecture.

**Architecture:** Remove the 20ms flush loops from RabbitMQ and Azure ServiceBus publishers (publish directly, fire-and-forget to preserve the sync `publish()` interface). Fix shutdown ordering (abort before close, `safeAll` for cleanup). Fix receiver bugs (abort-aware sleeps, safe `abandonMessage`, distinct error tags). Add missing RabbitMQ connection error handlers. Guard against resource leaks on init failure.

**Tech Stack:** TypeScript, Fastify, rabbitmq-client, @azure/service-bus, @google-cloud/pubsub, Jest, testcontainers

**Constraints:**

- `EventBus.publish()` signature stays `void` — no breaking API changes
- `mnemonist` `Queue` import can be removed from files that no longer need it
- All existing tests must continue to pass

---

## File Structure

| File                                               | Action     | Responsibility                                                                               |
| -------------------------------------------------- | ---------- | -------------------------------------------------------------------------------------------- |
| `src/event-bus/utils.ts`                           | **Create** | Shared `safeAll()` utility                                                                   |
| `src/event-bus/utils.spec.ts`                      | **Create** | Unit tests for `safeAll()`                                                                   |
| `src/event-bus/rabbitmq.ts`                        | **Modify** | Remove flush loop, publish directly, add connection error handler, use `safeAll` in shutdown |
| `src/event-bus/azure-servicebus.ts`                | **Modify** | Remove flush loop, publish directly, guard init, use `safeAll` in shutdown                   |
| `src/event-bus/event-consumer/utils.ts`            | **Modify** | Add abort signal support to delay functions                                                  |
| `src/event-bus/event-consumer/rabbitmq.ts`         | **Modify** | Add connection error handler, use `safeAll` in shutdown                                      |
| `src/event-bus/event-consumer/azure-servicebus.ts` | **Modify** | Fix shutdown-during-receive, distinct error tags, safe `abandonMessage`, guard init          |
| `src/event-bus/event-consumer/gcp-pubsub.ts`       | **Modify** | Abort-safe reconnect timer, pass abort signal to delay calls                                 |
| `src/event-bus/rabbitmq.spec.ts`                   | **Modify** | Update tests for direct publish (no more flush wait)                                         |

---

### Task 1: Create shared `safeAll` utility

**Files:**

- Create: `src/event-bus/utils.ts`
- Create: `src/event-bus/utils.spec.ts`

- [ ] **Step 1: Write failing tests for `safeAll`**

```ts
// src/event-bus/utils.spec.ts
import { safeAll } from "./utils";

describe("safeAll", () => {
  it("should run all functions even if earlier ones throw", async () => {
    const calls: number[] = [];
    const errors = await safeAll(
      async () => {
        calls.push(1);
        throw new Error("fail-1");
      },
      async () => {
        calls.push(2);
      },
      async () => {
        calls.push(3);
        throw new Error("fail-3");
      },
    );
    expect(calls).toEqual([1, 2, 3]);
    expect(errors).toHaveLength(2);
    expect((errors[0] as Error).message).toBe("fail-1");
    expect((errors[1] as Error).message).toBe("fail-3");
  });

  it("should return empty array when all succeed", async () => {
    const errors = await safeAll(
      async () => {},
      async () => {},
    );
    expect(errors).toEqual([]);
  });

  it("should handle sync functions", async () => {
    const calls: number[] = [];
    const errors = await safeAll(
      () => {
        calls.push(1);
      },
      () => {
        calls.push(2);
        throw new Error("sync-fail");
      },
      () => {
        calls.push(3);
      },
    );
    expect(calls).toEqual([1, 2, 3]);
    expect(errors).toHaveLength(1);
  });

  it("should handle empty arguments", async () => {
    const errors = await safeAll();
    expect(errors).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx jest src/event-bus/utils.spec.ts -v`
Expected: FAIL — module not found

- [ ] **Step 3: Write implementation**

```ts
// src/event-bus/utils.ts

/**
 * Run cleanup functions sequentially, collecting errors without stopping.
 * Sequential (not parallel) because close operations often have ordering
 * dependencies (e.g., close sender before client). Every function runs
 * even if earlier ones fail.
 */
export async function safeAll(
  ...fns: (() => Promise<void> | void)[]
): Promise<unknown[]> {
  const errors: unknown[] = [];
  for (const fn of fns) {
    try {
      await fn();
    } catch (err) {
      errors.push(err);
    }
  }
  return errors;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx jest src/event-bus/utils.spec.ts -v`
Expected: PASS — all 4 tests green

- [ ] **Step 5: Commit**

```bash
git add src/event-bus/utils.ts src/event-bus/utils.spec.ts
git commit -m "feat(event-bus): add safeAll utility for safe sequential cleanup"
```

---

### Task 2: Add abort signal support to consumer delay utilities

**Files:**

- Modify: `src/event-bus/event-consumer/utils.ts`

Currently `exponentialDelay` and `randomDelay` use `timers.setTimeout` without an abort signal. During shutdown, these block until the delay completes. Add an optional `signal` parameter.

- [ ] **Step 1: Modify delay functions to accept abort signal**

In `src/event-bus/event-consumer/utils.ts`, change both functions:

```ts
// Replace existing exponentialDelay signature and body
export async function exponentialDelay(
  attempt: number,
  baseDelayMs: number = BASE_DELAY,
  maxDelayMs: number = MAX_DELAY,
  randomizationFactor: number = 0.5,
  signal?: AbortSignal,
): Promise<void> {
  let delay = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
  if (randomizationFactor > 0) {
    const randomFactor = (Math.random() - 0.5) * randomizationFactor * delay;
    delay = Math.max(0, delay + randomFactor);
  }
  try {
    await timers.setTimeout(delay, undefined, signal ? { signal } : undefined);
  } catch (err) {
    if ((err as Error).name === "AbortError") return;
    throw err;
  }
}

// Replace existing randomDelay signature and body
export async function randomDelay(
  max: number = RANDOMIZED_DELAY_MAX,
  signal?: AbortSignal,
) {
  const delay = Math.ceil(Math.random() * max);
  try {
    await timers.setTimeout(delay, undefined, signal ? { signal } : undefined);
  } catch (err) {
    if ((err as Error).name === "AbortError") return;
    throw err;
  }
}
```

- [ ] **Step 2: Run existing tests to verify nothing breaks**

Run: `npx jest --testPathIgnorePatterns=rabbitmq.spec.ts -v`
Expected: PASS — existing tests still pass (the signal parameter is optional, so existing callers are unaffected)

- [ ] **Step 3: Commit**

```bash
git add src/event-bus/event-consumer/utils.ts
git commit -m "feat(event-bus): add abort signal support to delay utilities"
```

---

### Task 3: Remove flush loop from RabbitMQ publisher — publish directly

**Files:**

- Modify: `src/event-bus/rabbitmq.ts`

This is the core change. Remove `Queue`, `MessageWithAttempts`, `createMessageFlusher`, `flushBatch` — replace with direct publish. The `publish()` method stays sync (fire-and-forget: the async broker call runs but errors are logged, not propagated).

- [ ] **Step 1: Rewrite `rabbitmq.ts`**

Replace the entire file content. Key changes:

1. Remove `mnemonist` import, `MessageWithAttempts` interface, `createMessageFlusher`, `flushBatch`
2. `publishToExchange` now calls `publisher.send(...)` directly (fire-and-forget with `.catch()` for error logging)
3. Add `connection.on("error", ...)` handler to prevent unhandled ECONNRESET crashes
4. Shutdown uses `safeAll` — no more flush step needed since we publish directly
5. Remove the `setInterval` flush loop entirely

```ts
import { randomUUID } from "crypto";
import { FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import { Connection, Publisher } from "rabbitmq-client";
import {
  CreateHandlerRunner,
  ErrorWithStatus,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";
import { EventBus, EventBusOptions, EventMessage } from "./interfaces";
import {
  ensureRabbitMqExchangesAndQueues,
  getServicePrefix,
} from "./rabbitmq-utils";
import { safeAll } from "./utils";

interface IncomingRabbitMqMessage {
  messageId: number;
  body: string;
}

interface MessageBody {
  event: string;
  payload: any;
  file: string | null;
  processAfterDelayMs: number | undefined;
  publishTimestamp: number;
}

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  const handlerMap = getHandlerMap(options);
  f.decorate("_hasEventHandlers", handlerMap.size > 0);

  if (!process.env.RABBITMQ_URL) {
    throw new Error("RabbitMq requires RABBITMQ_URL");
  }
  if (!process.env.K_SERVICE) {
    throw new Error("RabbitMq requires K_SERVICE");
  }
  const connection = new Connection(process.env.RABBITMQ_URL);
  connection.on("error", (err) => {
    f.log.error({ tag: "RABBITMQ_CONNECTION_ERROR", err });
  });

  const service = process.env.K_SERVICE;
  if (options.ensureExchangesAndQueues) {
    await ensureRabbitMqExchangesAndQueues(connection, service);
  }

  const publisher = connection.createPublisher({ maxAttempts: 3 });
  const prefix = getServicePrefix(service);

  f.addHook("onClose", async () => {
    f.log.info({ tag: "RABBITMQ_CLOSING" });
    const errors = await safeAll(
      () => publisher.close(),
      () => connection.close(),
    );
    if (errors.length > 0) {
      f.log.error({ tag: "RABBITMQ_CLOSE_ERRORS", errors });
    }
  });

  function publishToExchange(
    event: string,
    payload: any,
    file: string | null,
    processAfterDelayMs: number,
    req?: FastifyRequest,
  ) {
    options.validateMsg(event, payload, req);
    const messageBody: MessageBody = {
      event,
      payload,
      file: file ?? null,
      processAfterDelayMs:
        processAfterDelayMs > 0 ? processAfterDelayMs : undefined,
      publishTimestamp: Date.now(),
    };

    // Fire-and-forget: publish directly, log errors.
    // publish() must remain sync (void return) — callers don't await.
    publisher
      .send(
        {
          messageId: randomUUID(),
          appId: `${prefix}.${service}`,
          contentType: "application/json",
          durable: true,
          exchange: `${prefix}.main-exchange`,
          headers: {
            event: messageBody.event,
            file: messageBody.file,
            processAfterDelayMs: "" + messageBody.processAfterDelayMs,
          },
        },
        JSON.stringify(messageBody, null, 0),
      )
      .catch((err: unknown) => {
        f.log.error({
          tag: "RABBITMQ_PUBLISH_ERROR",
          err,
          event,
        });
      });

    req?.log.info({
      tag: "EVENT_PUBLISH",
      event,
      payload,
      processAfterDelayMs,
    });
  }

  const bus: EventBus = {
    publish(event, payload, processAfterDelayMs) {
      publishToExchange(event, payload, null, processAfterDelayMs ?? 0);
    },
  };
  f.decorate("EventBus", {
    getter() {
      return bus;
    },
  });

  f.decorateRequest("EventBus", {
    getter() {
      return {
        publish: (event, payload, processAfterDelayMs) => {
          publishToExchange(
            event,
            payload,
            null,
            processAfterDelayMs ?? 0,
            this,
          );
        },
      };
    },
  });

  const selectAndRunHandlers = CreateHandlerRunner(f, options, handlerMap);

  f.post<{ Body: IncomingRabbitMqMessage }>(
    "/rabbitmq/process-message",
    {
      schema: {
        hide: true,
      } as any,
    },
    async function (req, reply) {
      const rawMsg = req.body;
      if (!rawMsg) {
        reply.send("OK");
        return reply;
      }
      req.log.info({
        tag: "RABBITMQ_MESSAGE_RECEIVED",
        messageId: rawMsg.messageId,
      });
      const msg = convert(rawMsg);
      options.validateMsg(msg.event, msg.data, req);

      if (noMatchingHandlers(handlerMap, msg)) {
        reply.send("OK");
        return reply;
      }

      req.log.info({
        tag: "RABBITMQ_MESSAGE_PROCESSING",
        event: msg,
      });

      if (
        msg.processAfterDelayMs > 0 &&
        Date.now() < msg.publishTime.getTime() + msg.processAfterDelayMs
      ) {
        reply
          .status(425)
          .send({ processAfterDelayMs: msg?.processAfterDelayMs });
        return reply;
      }

      try {
        await selectAndRunHandlers(req, msg, (event, payload, file) =>
          publishToExchange(event, payload, file, 0, req),
        );
        reply.send("OK");
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
          reply.status(err.status).send(err.message);
        } else {
          reply.status(500).send("ERROR");
        }
        return reply;
      }
    },
  );
};

export = fp(plugin, { name: "fp-eventbus-rabbitmq" });

function convert(msg: IncomingRabbitMqMessage): EventMessage {
  let body: MessageBody;
  try {
    body = JSON.parse(msg.body);
  } catch {
    throw new ErrorWithStatus(
      400,
      `Invalid JSON in message body: ${msg.body?.substring(0, 100)}`,
    );
  }
  return {
    id: "" + msg.messageId,
    attributes: {
      event: body.event,
      processAfterDelayMs: "" + (body.processAfterDelayMs ?? 0),
      file: body.file ?? "",
    },
    data: body.payload,
    event: body.event,
    processAfterDelayMs: body.processAfterDelayMs ?? 0,
    publishTime: new Date(body.publishTimestamp),
  };
}
```

- [ ] **Step 2: Run all non-integration tests**

Run: `npx jest --testPathIgnorePatterns=rabbitmq.spec.ts -v`
Expected: PASS

- [ ] **Step 3: Run RabbitMQ integration tests**

Run: `npx jest rabbitmq.spec.ts --testTimeout=120000 -v`
Expected: PASS — note that the test `"should publish messages to RabbitMQ"` previously waited 100ms for the flush. The direct publish should make this test pass faster, but the `setTimeout(100)` is still fine as a safety margin.

- [ ] **Step 4: Commit**

```bash
git add src/event-bus/rabbitmq.ts
git commit -m "refactor(event-bus): remove RabbitMQ flush loop, publish directly

Eliminates the 20ms buffered flush that caused message loss on crash,
gave no delivery confirmation, and added ~200 lines of machinery for
negligible batching benefit. publisher.send() is fire-and-forget to
preserve the sync publish() interface.

Also adds connection error handler to prevent unhandled ECONNRESET
crashes on broker restart, and uses safeAll for shutdown cleanup."
```

---

### Task 4: Remove flush loop from Azure ServiceBus publisher — publish directly

**Files:**

- Modify: `src/event-bus/azure-servicebus.ts`

Same treatment as RabbitMQ. Remove `Queue`, `MessageWithAttempts`, `createMessageFlusher`, the `setInterval` loop. Publish directly with fire-and-forget.

- [ ] **Step 1: Rewrite `azure-servicebus.ts`**

Key changes:

1. Remove `mnemonist` import, `MessageWithAttempts`, `createMessageFlusher`
2. Create the sender **after** all validation (guard init failure resource leak)
3. If sender creation fails, close the client
4. `publishToServiceBus` calls `sender.sendMessages()` directly (fire-and-forget via `.catch()`)
5. Shutdown uses `safeAll`, no flush needed

```ts
import * as AzureIden from "@azure/identity";
import {
  ServiceBusClient,
  ServiceBusMessage,
  ServiceBusSender,
} from "@azure/service-bus";
import { FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import {
  CreateHandlerRunner,
  ErrorWithStatus,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";
import { EventBus, EventBusOptions, EventMessage } from "./interfaces";
import { safeAll } from "./utils";

interface IncomingServiceBusMessage {
  messageId: number;
  body: string;
  scheduledEnqueueTimeUtc?: string;
}

interface MessageBody {
  event: string;
  payload: any;
  file: string | null;
  processAfterDelayMs: number | undefined;
  publishTimestamp: number;
}

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  const handlerMap = getHandlerMap(options);
  if (!options.namespace) {
    throw new Error(
      "Azure ServiceBus needs the namespace specified. Use EVENT_NAMESPACE env var",
    );
  }
  if (!options.topic) {
    throw new Error(
      "Azure ServiceBus needs the topic specified. Use EVENT_TOPIC env var",
    );
  }

  // All validation passed — safe to create resources
  const client = new ServiceBusClient(
    options.namespace,
    new AzureIden.DefaultAzureCredential({}),
    {},
  );

  let sender: ServiceBusSender;
  try {
    sender = client.createSender(options.topic);
  } catch (err) {
    await client.close();
    throw err;
  }

  f.addHook("onClose", async () => {
    f.log.info({ tag: "AZURE_SERVICEBUS_CLOSING" });
    const errors = await safeAll(
      () => sender.close(),
      () => client.close(),
    );
    if (errors.length > 0) {
      f.log.error({ tag: "AZURE_SERVICEBUS_CLOSE_ERRORS", errors });
    }
  });

  function publishToServiceBus(
    event: string,
    payload: any,
    file: string | null,
    processAfterDelayMs: number,
    req?: FastifyRequest,
  ) {
    options.validateMsg(event, payload, req);
    const messageBody: MessageBody = {
      event,
      payload,
      file: file ?? null,
      processAfterDelayMs:
        processAfterDelayMs > 0 ? processAfterDelayMs : undefined,
      publishTimestamp: Date.now(),
    };
    const encoded = JSON.stringify(messageBody);
    const msg: ServiceBusMessage = {
      body: Buffer.from(encoded, "utf8"),
      applicationProperties: {
        event,
        file: file ?? "",
      },
      contentType: "application/json",
      scheduledEnqueueTimeUtc:
        processAfterDelayMs > 0
          ? new Date(messageBody.publishTimestamp + processAfterDelayMs)
          : undefined,
    };

    // Fire-and-forget: publish directly, log errors.
    sender.sendMessages(msg).catch((err: unknown) => {
      f.log.error({
        tag: "AZURE_SERVICEBUS_PUBLISH_ERROR",
        err,
        event,
      });
    });

    req?.log.info({
      tag: "EVENT_PUBLISH",
      event,
      payload,
      processAfterDelayMs,
    });
  }

  const bus: EventBus = {
    publish(event, payload, processAfterDelayMs) {
      publishToServiceBus(event, payload, null, processAfterDelayMs ?? 0);
    },
  };
  f.decorate("EventBus", {
    getter() {
      return bus;
    },
  });

  f.decorateRequest("EventBus", {
    getter() {
      return {
        publish: (event, payload, processAfterDelayMs) => {
          publishToServiceBus(
            event,
            payload,
            null,
            processAfterDelayMs ?? 0,
            this,
          );
        },
      };
    },
  });

  const selectAndRunHandlers = CreateHandlerRunner(f, options, handlerMap);

  f.post<{ Body: IncomingServiceBusMessage }>(
    "/azure-servicebus/process-message",
    {
      schema: {
        hide: true,
      } as any,
    },
    async function (req, reply) {
      const rawMsg = req.body;
      if (!rawMsg) {
        reply.send("OK");
        return reply;
      }
      req.log.info({
        tag: "AZURE_SERVICEBUS_MESSAGE",
        messageId: rawMsg.messageId,
        scheduledEnqueueTimeUtc: rawMsg.scheduledEnqueueTimeUtc,
      });
      const msg = convert(rawMsg);
      options.validateMsg(msg.event, msg.data, req);

      if (noMatchingHandlers(handlerMap, msg)) {
        reply.send("OK");
        return reply;
      }

      req.log.info({
        tag: "AZURE_SERVICEBUS_MESSAGE_HANDLE",
        event: msg,
      });

      if (
        msg.processAfterDelayMs > 0 &&
        Date.now() < msg.publishTime.getTime() + msg.processAfterDelayMs
      ) {
        reply
          .status(425)
          .send({ processAfterDelayMs: msg?.processAfterDelayMs });
        return reply;
      }

      try {
        await selectAndRunHandlers(req, msg, (event, payload, file) =>
          publishToServiceBus(
            event,
            payload,
            file,
            msg.processAfterDelayMs,
            req,
          ),
        );
        reply.send("OK");
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
          reply.status(err.status).send(err.message);
        } else {
          reply.status(500).send("ERROR");
        }
        return reply;
      }
    },
  );
};

export = fp(plugin, { name: "fp-eventbus-azure-servicebus" });

function convert(msg: IncomingServiceBusMessage): EventMessage {
  const body: MessageBody = JSON.parse(msg.body);
  return {
    id: "" + msg.messageId,
    attributes: {
      event: body.event,
      processAfterDelayMs: "" + (body.processAfterDelayMs ?? 0),
      file: body.file ?? "",
    },
    data: body.payload,
    event: body.event,
    processAfterDelayMs: body.processAfterDelayMs ?? 0,
    publishTime: new Date(body.publishTimestamp),
  };
}
```

- [ ] **Step 2: Run tests**

Run: `npx jest --testPathIgnorePatterns=rabbitmq.spec.ts -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/event-bus/azure-servicebus.ts
git commit -m "refactor(event-bus): remove Azure ServiceBus flush loop, publish directly

Same fix as RabbitMQ — eliminates 20ms buffered flush. Adds init
failure guard (close client if sender creation fails) and safeAll
for shutdown cleanup."
```

---

### Task 5: Fix RabbitMQ consumer — connection error handler + safeAll shutdown

**Files:**

- Modify: `src/event-bus/event-consumer/rabbitmq.ts`

Two fixes:

1. The consumer creates its own `Connection` but never attaches an error handler. An `ECONNRESET` during broker restart will crash the process.
2. The `close()` method does sequential awaits without error protection — if `sub.close()` throws, `dlqPublisher` and `connection` are leaked.

- [ ] **Step 1: Add connection error handler**

In `src/event-bus/event-consumer/rabbitmq.ts`, after line 54 (`const connection = new Connection(process.env.RABBITMQ_URL);`), add:

```ts
connection.on("error", (err) => {
  instance.log.error({ tag: "RABBITMQ_CONSUMER_CONNECTION_ERROR", err });
});
```

- [ ] **Step 2: Add `safeAll` import and update `close()` method**

Add import at top of file:

```ts
import { safeAll } from "../utils";
```

Replace the close method (around lines 184-189):

```ts
// Old:
    close: async () => {
      ctrl.abort();
      await sub.close();
      await dlqPublisher.close();
      await connection.close();
    },

// New:
    close: async () => {
      ctrl.abort();
      const errors = await safeAll(
        () => sub.close(),
        () => dlqPublisher.close(),
        () => connection.close(),
      );
      if (errors.length > 0) {
        instance.log.error({ tag: "RABBITMQ_CONSUMER_CLOSE_ERRORS", errors });
      }
    },
```

- [ ] **Step 3: Run tests**

Run: `npx jest rabbitmq.spec.ts --testTimeout=120000 -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/event-bus/event-consumer/rabbitmq.ts
git commit -m "fix(event-bus): add RabbitMQ consumer connection error handler + safeAll shutdown

- Prevents unhandled ECONNRESET on broker restart from crashing the process
- Uses safeAll in close() so dlqPublisher and connection are always cleaned up"
```

---

### Task 6: Fix Azure ServiceBus consumer — shutdown safety, error tags, init guard

**Files:**

- Modify: `src/event-bus/event-consumer/azure-servicebus.ts`

Three fixes:

1. Don't call `receiver.abandonMessage(msg)` during shutdown — receiver may already be closing
2. Wrap `abandonMessage` calls in try/catch — they can throw if the lock expired
3. Distinguish `MSG_PROCESS_ERROR` from `TRANSPORT_ERROR`
4. Guard init: if `createReceiver` throws, close the `client`
5. Use `safeAll` for shutdown cleanup
6. Pass abort signal to `exponentialDelay` and `randomDelay`

- [ ] **Step 1: Rewrite the consumer**

```ts
import * as AzureIden from "@azure/identity";
import { RetryMode, ServiceBusClient } from "@azure/service-bus";
import { safeAll } from "../utils";
import { EventConsumerBuilder } from "./interface";
import { exponentialDelay, randomDelay } from "./utils";

export const AzureServiceBusConsumerBuilder: EventConsumerBuilder = async (
  instance,
) => {
  if (!process.env.EVENT_NAMESPACE) {
    throw new Error("Azure ServiceBus needs EVENT_NAMESPACE");
  }
  if (!process.env.EVENT_TOPIC) {
    throw new Error("Azure ServiceBus needs EVENT_TOPIC");
  }
  if (!process.env.EVENT_SUBSCRIPTION) {
    throw new Error("Azure ServiceBus needs EVENT_SUBSCRIPTION");
  }

  // All validation passed — safe to create resources
  const client = new ServiceBusClient(
    process.env.EVENT_NAMESPACE,
    new AzureIden.DefaultAzureCredential({}),
    {
      retryOptions: {
        mode: RetryMode.Fixed,
        retryDelayInMs: 10_000,
        timeoutInMs: 5_000,
        maxRetries: 5,
      },
    },
  );

  let receiver;
  try {
    receiver = client.createReceiver(
      process.env.EVENT_TOPIC,
      process.env.EVENT_SUBSCRIPTION,
      {
        skipParsingBodyAsJson: true,
        skipConvertingDate: true,
        receiveMode: "peekLock",
      },
    );
  } catch (err) {
    await client.close();
    throw err;
  }

  const ctrl = new AbortController();

  const handler = receiver.subscribe(
    {
      async processMessage(msg) {
        if (ctrl.signal.aborted) {
          // Don't attempt abandonMessage during shutdown — receiver may already
          // be closing. The broker will redeliver unacked messages after the
          // connection drops.
          return;
        }
        if (msg.deliveryCount && msg.deliveryCount > 1) {
          instance.log.warn({
            tag: "AZURE_SERVICE_BUS_RECEIVER_RETRY",
            deliveryCount: msg.deliveryCount,
            messageId: msg.messageId,
          });
          await exponentialDelay(
            Math.max(msg.deliveryCount - 2, 0),
            undefined,
            undefined,
            undefined,
            ctrl.signal,
          );
        }
        try {
          const payload = {
            messageId: msg.messageId,
            body: msg.body,
            scheduledEnqueueTimeUtc: msg.scheduledEnqueueTimeUtc?.toISOString(),
          };
          if (Buffer.isBuffer(payload.body)) {
            payload.body = payload.body.toString("utf8");
          }
          const resp = await instance.inject({
            method: "POST",
            url: "/azure-servicebus/process-message",
            payload,
          });
          if (resp.statusCode >= 200 && resp.statusCode < 300) {
            await receiver.completeMessage(msg);
          } else if (resp.statusCode === 429 || resp.statusCode === 409) {
            await randomDelay(undefined, ctrl.signal);
            await receiver.abandonMessage(msg).catch((abandonErr) => {
              instance.log.warn({
                tag: "AZURE_SERVICE_BUS_ABANDON_FAILED",
                err: abandonErr,
                messageId: msg.messageId,
              });
            });
          } else if (resp.statusCode === 425) {
            instance.log.error({
              tag: "AZURE_SERVICE_BUS_DELAYED_MESSAGE",
              payload,
            });
            await receiver.abandonMessage(msg).catch((abandonErr) => {
              instance.log.warn({
                tag: "AZURE_SERVICE_BUS_ABANDON_FAILED",
                err: abandonErr,
                messageId: msg.messageId,
              });
            });
          } else {
            await receiver.abandonMessage(msg).catch((abandonErr) => {
              instance.log.warn({
                tag: "AZURE_SERVICE_BUS_ABANDON_FAILED",
                err: abandonErr,
                messageId: msg.messageId,
              });
            });
          }
        } catch (err) {
          if ((err as Error).name !== "AbortError") {
            instance.log.error({
              tag: "AZURE_SERVICE_BUS_MSG_PROCESS_ERROR",
              err,
            });
          }
          // Skip abandonMessage during shutdown
          if (!ctrl.signal.aborted) {
            await receiver.abandonMessage(msg).catch((abandonErr) => {
              instance.log.warn({
                tag: "AZURE_SERVICE_BUS_ABANDON_FAILED",
                err: abandonErr,
                messageId: msg.messageId,
              });
            });
          }
        }
      },
      async processError(args) {
        instance.log.error({
          tag: "AZURE_SERVICE_BUS_TRANSPORT_ERROR",
          err: args.error,
          entityPath: args.entityPath,
        });
      },
    },
    {
      abortSignal: ctrl.signal,
      autoCompleteMessages: false,
      maxConcurrentCalls:
        parseInt(
          process.env.EVENT_SUBSCRIPTION_MAX_CONCURRENT_CALLS ?? "10",
          10,
        ) || 10,
    },
  );
  instance.log.info(
    "Attached to Azure ServiceBus Subscription=" +
      process.env.EVENT_SUBSCRIPTION,
  );
  return {
    close: async () => {
      // Abort BEFORE closing: prevents shutdown from blocking on broker
      // round-trips. In-flight messages get redelivered by the broker.
      ctrl.abort();
      const errors = await safeAll(
        () => handler.close(),
        () => receiver.close(),
        () => client.close(),
      );
      if (errors.length > 0) {
        instance.log.error({ tag: "AZURE_SERVICE_BUS_CLOSE_ERRORS", errors });
      }
    },
  };
};
```

- [ ] **Step 2: Run tests**

Run: `npx jest --testPathIgnorePatterns=rabbitmq.spec.ts -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/event-bus/event-consumer/azure-servicebus.ts
git commit -m "fix(event-bus): fix Azure ServiceBus consumer shutdown and error handling

- Don't abandonMessage during shutdown (receiver may already be closing)
- Wrap abandonMessage calls in .catch() (lock may have expired)
- Distinct error tags: MSG_PROCESS_ERROR vs TRANSPORT_ERROR
- Guard init: close client if createReceiver throws
- Use safeAll for shutdown cleanup
- Pass abort signal to delay functions"
```

---

### Task 7: Fix GCP Pub/Sub consumer — abort-safe reconnect, abort-aware delays

**Files:**

- Modify: `src/event-bus/event-consumer/gcp-pubsub.ts`

Three fixes:

1. The reconnect timer's `setTimeout` callback should check abort signal before calling `init()` to prevent a race with `close()`
2. The `randomDelay()` call on 429/409 responses should pass the abort signal (otherwise blocks shutdown up to 10s)
3. The `timers.setTimeout(processAfterDelayMs)` for delayed messages should be abort-aware
4. Use `safeAll` for close

- [ ] **Step 1: Add abort check before reconnect init**

In `src/event-bus/event-consumer/gcp-pubsub.ts`, in the `init()` method's error handler (around line 90-93), the reconnect `setTimeout` callback should check `ctrl.signal.aborted` before calling `this.init()`:

Replace:

```ts
this.timerRef = setTimeout(() => {
  this.init();
}, 10000);
```

With:

```ts
this.timerRef = setTimeout(() => {
  if (!this.ctrl.signal.aborted) {
    this.init();
  }
}, 10000);
```

- [ ] **Step 2: Pass abort signal to delay calls in `processMsg`**

In the `processMsg` method, update the `randomDelay` call (around line 124) and the delayed message sleep (around line 130):

Replace:

```ts
      } else if (resp.statusCode === 429 || resp.statusCode === 409) {
        // rate-limited or lock-conflict
        await randomDelay();
        msg.nack();
      } else if (resp.statusCode === 425 && attempt < 2) {
        const parsed = JSON.parse(resp.body);
        const processAfterDelayMs = parsed?.processAfterDelayMs ?? 0;
        if (processAfterDelayMs > 0) {
          await timers.setTimeout(processAfterDelayMs);
        }
```

With:

```ts
      } else if (resp.statusCode === 429 || resp.statusCode === 409) {
        // rate-limited or lock-conflict
        await randomDelay(undefined, this.ctrl.signal);
        msg.nack();
      } else if (resp.statusCode === 425 && attempt < 2) {
        const parsed = JSON.parse(resp.body);
        const processAfterDelayMs = parsed?.processAfterDelayMs ?? 0;
        if (processAfterDelayMs > 0) {
          try {
            await timers.setTimeout(processAfterDelayMs, undefined, { signal: this.ctrl.signal });
          } catch (err) {
            if ((err as Error).name === "AbortError") {
              msg.nack();
              return;
            }
            throw err;
          }
        }
```

- [ ] **Step 3: Use `safeAll` for close**

Add import at the top:

```ts
import { safeAll } from "../utils";
```

In the factory function's returned `close` method, use `safeAll`:

Replace:

```ts
return {
  close: async () => {
    await runner.close();
    await pubsub.close();
  },
};
```

With:

```ts
return {
  close: async () => {
    const errors = await safeAll(
      () => runner.close(),
      () => pubsub.close(),
    );
    if (errors.length > 0) {
      instance.log.error({ tag: "GCP_PUBSUB_CLOSE_ERRORS", errors });
    }
  },
};
```

- [ ] **Step 4: Run tests**

Run: `npx jest --testPathIgnorePatterns=rabbitmq.spec.ts -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/event-bus/event-consumer/gcp-pubsub.ts
git commit -m "fix(event-bus): abort-safe reconnect and delays for GCP Pub/Sub consumer

- Abort check before reconnect init to prevent race with close()
- Pass abort signal to randomDelay and delayed message sleep
- Use safeAll for shutdown cleanup"
```

---

### Task 8: Update RabbitMQ integration tests

**Files:**

- Modify: `src/event-bus/rabbitmq.spec.ts`

The existing test `"should publish messages to RabbitMQ"` waits 100ms for the flush interval. With direct publish, this still works (the `setTimeout(100)` just adds safety margin), but we should verify the test passes reliably and potentially reduce the wait time.

- [ ] **Step 1: Run full integration test suite**

Run: `npx jest rabbitmq.spec.ts --testTimeout=120000 -v`
Expected: PASS — all tests green. The 100ms sleep in `"should publish messages to RabbitMQ"` is still fine as publisher.send() is async fire-and-forget.

- [ ] **Step 2: Run all tests together**

Run: `npx jest --testTimeout=120000 -v`
Expected: PASS — all tests pass, including unit tests and integration tests

- [ ] **Step 3: Commit (if any test adjustments were needed)**

```bash
# Only if tests needed modification
git add src/event-bus/rabbitmq.spec.ts
git commit -m "test(event-bus): update integration tests for direct publish"
```

---

### Task 9: Verify build and final checks

**Files:** None — verification only

- [ ] **Step 1: Run build**

Run: `pnpm run build`
Expected: Clean build, no TypeScript errors

- [ ] **Step 2: Run all tests**

Run: `pnpm test`
Expected: PASS

- [ ] **Step 3: Run RabbitMQ integration tests**

Run: `npx jest rabbitmq.spec.ts --testTimeout=120000 -v`
Expected: PASS

- [ ] **Step 4: Check that `mnemonist` is no longer imported in modified files**

Run: `grep -r "mnemonist" src/event-bus/rabbitmq.ts src/event-bus/azure-servicebus.ts`
Expected: No matches. `mnemonist` is still a dependency (used elsewhere or kept for compatibility), but no longer imported in these two files.

- [ ] **Step 5: Verify no leftover dead code**

Run: `grep -rn "createMessageFlusher\|flushBatch\|MessageWithAttempts\|msgQueue" src/event-bus/`
Expected: No matches in `rabbitmq.ts` or `azure-servicebus.ts`. May still appear in test files if they reference old behavior — update if found.

---

## Summary of Changes

| File                                               | Lines Removed (approx) | Lines Added (approx) | What Changed                                                                  |
| -------------------------------------------------- | ---------------------- | -------------------- | ----------------------------------------------------------------------------- |
| `src/event-bus/utils.ts`                           | 0                      | 15                   | New `safeAll` utility                                                         |
| `src/event-bus/utils.spec.ts`                      | 0                      | 40                   | Tests for `safeAll`                                                           |
| `src/event-bus/rabbitmq.ts`                        | ~150                   | ~100                 | Remove flush loop, direct publish, connection error handler, safeAll shutdown |
| `src/event-bus/azure-servicebus.ts`                | ~100                   | ~70                  | Remove flush loop, direct publish, init guard, safeAll shutdown               |
| `src/event-bus/event-consumer/utils.ts`            | 0                      | ~10                  | Abort signal support on delay functions                                       |
| `src/event-bus/event-consumer/rabbitmq.ts`         | ~5                     | ~15                  | Connection error handler, safeAll shutdown                                    |
| `src/event-bus/event-consumer/azure-servicebus.ts` | ~30                    | ~60                  | Shutdown safety, error tags, init guard, safeAll                              |
| `src/event-bus/event-consumer/gcp-pubsub.ts`       | ~5                     | ~20                  | Abort-safe reconnect, abort-aware delays, safeAll close                       |

**Net effect:** ~280 lines removed, ~310 lines added (most additions are in the new utility + tests + more robust error handling replacing the flush machinery).
