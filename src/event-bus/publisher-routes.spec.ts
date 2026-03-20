/**
 * Unit tests for the process-message endpoints across all publisher plugins.
 * These test the route handler logic (convert, validate, handler dispatch,
 * error paths) via fastify.inject() — no broker connection or Docker needed.
 *
 * Each driver's plugin is tested through the in-process local driver for
 * handler dispatch, while the process-message endpoints are tested directly
 * by injecting payloads that mimic what the consumer would send.
 */
import Fastify, { FastifyInstance } from "fastify";
import { Plugins } from "../index";
import { EventBusOptions, EventHandler, EventMessage } from "./interfaces";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeOptions(
  overrides: Partial<EventBusOptions> = {},
): EventBusOptions {
  return {
    busType: "in-process",
    handlers: [],
    validateMsg: jest.fn(),
    processError: jest
      .fn()
      .mockReturnValue({ err: new Error("test"), status: 500 }),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// In-process (local) driver — route handler tests
// ---------------------------------------------------------------------------

describe("Local (in-process) driver", () => {
  let fastify: FastifyInstance;

  afterEach(async () => {
    if (fastify) await fastify.close();
  });

  it("should process a valid message through handler", async () => {
    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOptions({
        handlers: [{ file: "test.ts", handlers: { "order.created": handler } }],
      }),
    );
    await fastify.ready();

    const resp = await fastify.inject({
      method: "POST",
      url: "/local-servicebus/process-message",
      payload: {
        id: "msg-1",
        event: "order.created",
        data: { orderId: 123 },
        attributes: {},
        publishTime: new Date().toISOString(),
        processAfterDelayMs: 0,
      },
    });

    expect(resp.statusCode).toBe(200);
    expect(handler).toHaveBeenCalled();
    expect(received[0].data).toEqual({ orderId: 123 });
  });

  it("should return OK for events with no matching handler", async () => {
    fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOptions({
        handlers: [
          { file: "test.ts", handlers: { "order.created": jest.fn() } },
        ],
      }),
    );
    await fastify.ready();

    const resp = await fastify.inject({
      method: "POST",
      url: "/local-servicebus/process-message",
      payload: {
        id: "msg-1",
        event: "unknown.event",
        data: {},
        attributes: {},
        publishTime: new Date().toISOString(),
        processAfterDelayMs: 0,
      },
    });

    expect(resp.statusCode).toBe(200);
    expect(resp.body).toContain("BAIL_OUT");
  });

  it("should handle empty body", async () => {
    fastify = Fastify({ logger: false });
    await fastify.register(Plugins.EventBus, makeOptions());
    await fastify.ready();

    const resp = await fastify.inject({
      method: "POST",
      url: "/local-servicebus/process-message",
      payload: null as any,
    });

    // Fastify may parse null differently, but should not crash
    expect(resp.statusCode).toBeLessThan(500);
  });

  it("should call validateMsg on the event", async () => {
    const validateMsg = jest.fn();
    fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOptions({
        validateMsg,
        handlers: [
          { file: "test.ts", handlers: { "validate.event": jest.fn() } },
        ],
      }),
    );
    await fastify.ready();

    await fastify.inject({
      method: "POST",
      url: "/local-servicebus/process-message",
      payload: {
        id: "msg-1",
        event: "validate.event",
        data: { key: "value" },
        attributes: {},
        publishTime: new Date().toISOString(),
        processAfterDelayMs: 0,
      },
    });

    expect(validateMsg).toHaveBeenCalledWith(
      "validate.event",
      { key: "value" },
      expect.anything(),
    );
  });

  it("should call validateMsg synchronously on publish", async () => {
    const validateMsg = jest.fn();
    // Use a separate instance to avoid the local driver's onClose flush race.
    // We never call close() — Jest will GC it. The test only verifies the
    // synchronous validateMsg call, not message delivery.
    const f = Fastify({ logger: false });
    await f.register(
      Plugins.EventBus,
      makeOptions({
        validateMsg,
        handlers: [{ file: "test.ts", handlers: { "some.event": jest.fn() } }],
      }),
    );
    await f.ready();

    f.EventBus.publish("some.event", { data: 1 });
    f.EventBus.publish("some.event", { data: 2 }, 5000);
    expect(validateMsg).toHaveBeenCalledTimes(2);
    expect(validateMsg).toHaveBeenCalledWith(
      "some.event",
      { data: 1 },
      undefined,
    );
    expect(validateMsg).toHaveBeenCalledWith(
      "some.event",
      { data: 2 },
      undefined,
    );
    // Intentionally not closing — local driver's onClose flush + inject race
    // is a pre-existing issue tracked separately.
  });

  it("should dispatch file-targeted retry to correct handler only", async () => {
    const handler1 = jest.fn();
    const handler2 = jest.fn();

    fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOptions({
        handlers: [
          { file: "handler-a.ts", handlers: { "retry.event": handler1 } },
          { file: "handler-b.ts", handlers: { "retry.event": handler2 } },
        ],
      }),
    );
    await fastify.ready();

    // Target only handler-b.ts via file attribute
    const resp = await fastify.inject({
      method: "POST",
      url: "/local-servicebus/process-message",
      payload: {
        id: "msg-retry",
        event: "retry.event",
        data: { retried: true },
        attributes: { file: "handler-b.ts" },
        publishTime: new Date().toISOString(),
        processAfterDelayMs: 0,
      },
    });

    expect(resp.statusCode).toBe(200);
    expect(handler1).not.toHaveBeenCalled();
    expect(handler2).toHaveBeenCalled();
  });

  it("should handle handler errors via processError callback", async () => {
    const processError = jest.fn().mockReturnValue({
      err: new Error("processed"),
      status: 500,
    });

    const handler: EventHandler = jest.fn(async () => {
      throw new Error("handler failed");
    });

    fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOptions({
        processError,
        handlers: [{ file: "fail.ts", handlers: { "fail.event": handler } }],
      }),
    );
    await fastify.ready();

    const resp = await fastify.inject({
      method: "POST",
      url: "/local-servicebus/process-message",
      payload: {
        id: "msg-fail",
        event: "fail.event",
        data: {},
        attributes: { file: "fail.ts" },
        publishTime: new Date().toISOString(),
        processAfterDelayMs: 0,
      },
    });

    // processError should have been called
    expect(processError).toHaveBeenCalled();
    // With file specified, it re-throws as ErrorWithStatus
    expect(resp.statusCode).toBe(500);
  });

  it("should handle multiple handlers for same event", async () => {
    const calls: string[] = [];
    const handler1: EventHandler = jest.fn(async () => {
      calls.push("h1");
    });
    const handler2: EventHandler = jest.fn(async () => {
      calls.push("h2");
    });

    fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOptions({
        handlers: [
          { file: "a.ts", handlers: { "multi.event": handler1 } },
          { file: "b.ts", handlers: { "multi.event": handler2 } },
        ],
      }),
    );
    await fastify.ready();

    const resp = await fastify.inject({
      method: "POST",
      url: "/local-servicebus/process-message",
      payload: {
        id: "msg-multi",
        event: "multi.event",
        data: { multi: true },
        attributes: {},
        publishTime: new Date().toISOString(),
        processAfterDelayMs: 0,
      },
    });

    expect(resp.statusCode).toBe(200);
    expect(calls).toEqual(["h1", "h2"]);
  });
});

// ---------------------------------------------------------------------------
// RabbitMQ process-message endpoint (publisher route only, no connection)
// ---------------------------------------------------------------------------

describe("RabbitMQ process-message endpoint", () => {
  let fastify: FastifyInstance;

  afterEach(async () => {
    if (fastify) await fastify.close();
  });

  // We can't register the real rabbitmq plugin without RABBITMQ_URL, but we
  // can test the convert/route logic by building a minimal Fastify app that
  // mimics the endpoint shape. Instead, we test via the integration tests for
  // the full round-trip. Here we test error handling via inject on the real plugin.

  it("should reject invalid JSON in message body", async () => {
    // Set up env for RabbitMQ
    const origUrl = process.env.RABBITMQ_URL;
    const origService = process.env.K_SERVICE;
    process.env.RABBITMQ_URL = "amqp://fake:5672";
    process.env.K_SERVICE = "test-service";

    try {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "rabbitmq",
          ensureExchangesAndQueues: false,
          handlers: [
            { file: "test.ts", handlers: { "test.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const resp = await fastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: 1,
          body: "not-valid-json{{{",
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(400);
      expect(resp.body).toContain("Invalid JSON");
    } finally {
      if (origUrl !== undefined) process.env.RABBITMQ_URL = origUrl;
      else delete process.env.RABBITMQ_URL;
      if (origService !== undefined) process.env.K_SERVICE = origService;
      else delete process.env.K_SERVICE;
    }
  });

  it("should return OK for valid message with no matching handlers", async () => {
    const origUrl = process.env.RABBITMQ_URL;
    const origService = process.env.K_SERVICE;
    process.env.RABBITMQ_URL = "amqp://fake:5672";
    process.env.K_SERVICE = "test-service";

    try {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "rabbitmq",
          ensureExchangesAndQueues: false,
          handlers: [
            { file: "test.ts", handlers: { "registered.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "unregistered.event",
        payload: { data: 1 },
        file: null,
        processAfterDelayMs: undefined,
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
      expect(resp.body).toBe("OK");
    } finally {
      if (origUrl !== undefined) process.env.RABBITMQ_URL = origUrl;
      else delete process.env.RABBITMQ_URL;
      if (origService !== undefined) process.env.K_SERVICE = origService;
      else delete process.env.K_SERVICE;
    }
  });

  it("should return 425 for delayed messages not yet ready", async () => {
    const origUrl = process.env.RABBITMQ_URL;
    const origService = process.env.K_SERVICE;
    process.env.RABBITMQ_URL = "amqp://fake:5672";
    process.env.K_SERVICE = "test-service";

    try {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "rabbitmq",
          ensureExchangesAndQueues: false,
          handlers: [
            { file: "test.ts", handlers: { "delayed.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "delayed.event",
        payload: { data: 1 },
        file: null,
        processAfterDelayMs: 60_000, // 60 seconds in the future
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(425);
      const body = JSON.parse(resp.body);
      expect(body.processAfterDelayMs).toBe(60_000);
    } finally {
      if (origUrl !== undefined) process.env.RABBITMQ_URL = origUrl;
      else delete process.env.RABBITMQ_URL;
      if (origService !== undefined) process.env.K_SERVICE = origService;
      else delete process.env.K_SERVICE;
    }
  });

  it("should process a valid message through handler", async () => {
    const origUrl = process.env.RABBITMQ_URL;
    const origService = process.env.K_SERVICE;
    process.env.RABBITMQ_URL = "amqp://fake:5672";
    process.env.K_SERVICE = "test-service";

    try {
      const received: EventMessage[] = [];
      const handler: EventHandler = jest.fn(async (msg) => {
        received.push(msg);
      });

      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "rabbitmq",
          ensureExchangesAndQueues: false,
          handlers: [{ file: "test.ts", handlers: { "valid.event": handler } }],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "valid.event",
        payload: { orderId: 42 },
        file: null,
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
      expect(handler).toHaveBeenCalled();
      expect(received[0].data).toEqual({ orderId: 42 });
      expect(received[0].event).toBe("valid.event");
    } finally {
      if (origUrl !== undefined) process.env.RABBITMQ_URL = origUrl;
      else delete process.env.RABBITMQ_URL;
      if (origService !== undefined) process.env.K_SERVICE = origService;
      else delete process.env.K_SERVICE;
    }
  });
});

// ---------------------------------------------------------------------------
// NATS JetStream process-message endpoint
// ---------------------------------------------------------------------------

// NATS JetStream process-message endpoint requires a live NATS connection to
// register the plugin. Route handler coverage comes from the Docker-based
// integration tests in event-bus.integration.spec.ts.

// ---------------------------------------------------------------------------
// GCP Pub/Sub process-message endpoint (no connection needed)
// ---------------------------------------------------------------------------

describe("GCP Pub/Sub process-message endpoint", () => {
  let fastify: FastifyInstance;

  afterEach(async () => {
    if (fastify) await fastify.close();
  });

  // GCP plugin creates a PubSub client on registration, which tries to
  // auto-detect credentials. We can set PUBSUB_EMULATOR_HOST to prevent
  // real credential lookups.

  it("should reject invalid base64/JSON in message data", async () => {
    const origEmulator = process.env.PUBSUB_EMULATOR_HOST;
    process.env.PUBSUB_EMULATOR_HOST = "localhost:9999";

    try {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "gcp-pubsub",
          topic: "test-topic",
          handlers: [
            { file: "test.ts", handlers: { "test.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      // Send a message with invalid base64 data that decodes to invalid JSON
      const resp = await fastify.inject({
        method: "POST",
        url: "/gcp-pubsub/process-message",
        payload: {
          message: {
            attributes: { event: "test.event" },
            data: Buffer.from("not-valid-json{{{").toString("base64"),
            messageId: "msg-1",
            publishTime: new Date().toISOString(),
          },
          subscription: "test-sub",
          attempt: 0,
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(400);
      expect(resp.body).toContain("Invalid JSON");
    } finally {
      if (origEmulator !== undefined)
        process.env.PUBSUB_EMULATOR_HOST = origEmulator;
      else delete process.env.PUBSUB_EMULATOR_HOST;
    }
  });

  it("should return OK for valid message with no matching handlers", async () => {
    const origEmulator = process.env.PUBSUB_EMULATOR_HOST;
    process.env.PUBSUB_EMULATOR_HOST = "localhost:9999";

    try {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "gcp-pubsub",
          topic: "test-topic",
          handlers: [
            { file: "test.ts", handlers: { "registered.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const data = Buffer.from(
        JSON.stringify({
          event: "unregistered.event",
          payload: { x: 1 },
        }),
      ).toString("base64");

      const resp = await fastify.inject({
        method: "POST",
        url: "/gcp-pubsub/process-message",
        payload: {
          message: {
            attributes: { event: "unregistered.event" },
            data,
            messageId: "msg-2",
            publishTime: new Date().toISOString(),
          },
          subscription: "test-sub",
          attempt: 0,
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
    } finally {
      if (origEmulator !== undefined)
        process.env.PUBSUB_EMULATOR_HOST = origEmulator;
      else delete process.env.PUBSUB_EMULATOR_HOST;
    }
  });

  it("should return 425 for delayed messages not yet ready", async () => {
    const origEmulator = process.env.PUBSUB_EMULATOR_HOST;
    process.env.PUBSUB_EMULATOR_HOST = "localhost:9999";

    try {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "gcp-pubsub",
          topic: "test-topic",
          handlers: [
            { file: "test.ts", handlers: { "delayed.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const data = Buffer.from(
        JSON.stringify({
          event: "delayed.event",
          payload: { x: 1 },
        }),
      ).toString("base64");

      const resp = await fastify.inject({
        method: "POST",
        url: "/gcp-pubsub/process-message",
        payload: {
          message: {
            attributes: {
              event: "delayed.event",
              processAfterDelayMs: "60000",
            },
            data,
            messageId: "msg-3",
            publishTime: new Date().toISOString(),
          },
          subscription: "test-sub",
          attempt: 0,
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(425);
    } finally {
      if (origEmulator !== undefined)
        process.env.PUBSUB_EMULATOR_HOST = origEmulator;
      else delete process.env.PUBSUB_EMULATOR_HOST;
    }
  });

  it("should process a valid message through handler", async () => {
    const origEmulator = process.env.PUBSUB_EMULATOR_HOST;
    process.env.PUBSUB_EMULATOR_HOST = "localhost:9999";

    try {
      const received: EventMessage[] = [];
      const handler: EventHandler = jest.fn(async (msg) => {
        received.push(msg);
      });

      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "gcp-pubsub",
          topic: "test-topic",
          handlers: [{ file: "test.ts", handlers: { "valid.event": handler } }],
        }),
      );
      await fastify.ready();

      const data = Buffer.from(
        JSON.stringify({
          event: "valid.event",
          payload: { product: "widget" },
        }),
      ).toString("base64");

      const resp = await fastify.inject({
        method: "POST",
        url: "/gcp-pubsub/process-message",
        payload: {
          message: {
            attributes: { event: "valid.event" },
            data,
            messageId: "msg-4",
            publishTime: new Date().toISOString(),
          },
          subscription: "test-sub",
          attempt: 0,
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
      expect(handler).toHaveBeenCalled();
      expect(received[0].data).toEqual({ product: "widget" });
      expect(received[0].event).toBe("valid.event");
    } finally {
      if (origEmulator !== undefined)
        process.env.PUBSUB_EMULATOR_HOST = origEmulator;
      else delete process.env.PUBSUB_EMULATOR_HOST;
    }
  });

  it("should return OK for empty body", async () => {
    const origEmulator = process.env.PUBSUB_EMULATOR_HOST;
    process.env.PUBSUB_EMULATOR_HOST = "localhost:9999";

    try {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "gcp-pubsub",
          topic: "test-topic",
          handlers: [],
        }),
      );
      await fastify.ready();

      const resp = await fastify.inject({
        method: "POST",
        url: "/gcp-pubsub/process-message",
        headers: { "content-type": "application/json" },
      });

      // Should handle gracefully
      expect(resp.statusCode).toBeLessThan(500);
    } finally {
      if (origEmulator !== undefined)
        process.env.PUBSUB_EMULATOR_HOST = origEmulator;
      else delete process.env.PUBSUB_EMULATOR_HOST;
    }
  });
});

// ---------------------------------------------------------------------------
// Azure ServiceBus process-message endpoint
// Uses a fake emulator connection string — ServiceBusClient is lazy (doesn't
// connect until send/receive), so we can register the plugin and test the
// route handler via inject().
// ---------------------------------------------------------------------------

describe("Azure ServiceBus process-message endpoint", () => {
  let fastify: FastifyInstance;
  const FAKE_CONN_STR =
    "Endpoint=sb://localhost:19999;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fake;UseDevelopmentEmulator=true";

  function withAzureEnv(fn: () => Promise<void>) {
    return async () => {
      const origConn = process.env.AZURE_SERVICEBUS_CONNECTION_STRING;
      process.env.AZURE_SERVICEBUS_CONNECTION_STRING = FAKE_CONN_STR;
      try {
        await fn();
      } finally {
        if (origConn !== undefined)
          process.env.AZURE_SERVICEBUS_CONNECTION_STRING = origConn;
        else delete process.env.AZURE_SERVICEBUS_CONNECTION_STRING;
      }
    };
  }

  afterEach(async () => {
    if (fastify) await fastify.close();
  });

  it(
    "should reject invalid JSON in message body",
    withAzureEnv(async () => {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          handlers: [
            { file: "test.ts", handlers: { "test.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        payload: {
          messageId: 1,
          body: "not-valid-json{{{",
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(400);
      expect(resp.body).toContain("Invalid JSON");
    }),
  );

  it(
    "should return OK for valid message with no matching handlers",
    withAzureEnv(async () => {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          handlers: [
            { file: "test.ts", handlers: { "registered.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "unregistered.event",
        payload: { data: 1 },
        file: null,
        processAfterDelayMs: undefined,
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
      expect(resp.body).toBe("OK");
    }),
  );

  it(
    "should return 425 for delayed messages not yet ready",
    withAzureEnv(async () => {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          handlers: [
            { file: "test.ts", handlers: { "delayed.event": jest.fn() } },
          ],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "delayed.event",
        payload: { data: 1 },
        file: null,
        processAfterDelayMs: 60_000,
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(425);
      const body = JSON.parse(resp.body);
      expect(body.processAfterDelayMs).toBe(60_000);
    }),
  );

  it(
    "should process a valid message through handler",
    withAzureEnv(async () => {
      const received: EventMessage[] = [];
      const handler: EventHandler = jest.fn(async (msg) => {
        received.push(msg);
      });

      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          handlers: [{ file: "test.ts", handlers: { "valid.event": handler } }],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "valid.event",
        payload: { orderId: 99 },
        file: null,
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
      expect(handler).toHaveBeenCalled();
      expect(received[0].data).toEqual({ orderId: 99 });
      expect(received[0].event).toBe("valid.event");
    }),
  );

  it(
    "should handle multiple handlers for same event",
    withAzureEnv(async () => {
      const calls: string[] = [];
      const handler1: EventHandler = jest.fn(async () => {
        calls.push("h1");
      });
      const handler2: EventHandler = jest.fn(async () => {
        calls.push("h2");
      });

      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          handlers: [
            { file: "a.ts", handlers: { "multi.event": handler1 } },
            { file: "b.ts", handlers: { "multi.event": handler2 } },
          ],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "multi.event",
        payload: { multi: true },
        file: null,
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
      expect(calls).toEqual(["h1", "h2"]);
    }),
  );

  it(
    "should dispatch file-targeted retry to correct handler only",
    withAzureEnv(async () => {
      const handler1 = jest.fn();
      const handler2 = jest.fn();

      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          handlers: [
            { file: "handler-a.ts", handlers: { "retry.event": handler1 } },
            { file: "handler-b.ts", handlers: { "retry.event": handler2 } },
          ],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "retry.event",
        payload: { retried: true },
        file: "handler-b.ts",
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBe(200);
      expect(handler1).not.toHaveBeenCalled();
      expect(handler2).toHaveBeenCalled();
    }),
  );

  it(
    "should handle handler errors with ErrorWithStatus",
    withAzureEnv(async () => {
      const processError = jest.fn().mockReturnValue({
        err: new Error("processed"),
        status: 502,
      });

      const handler: EventHandler = jest.fn(async () => {
        throw new Error("handler failed");
      });

      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          processError,
          handlers: [{ file: "fail.ts", handlers: { "fail.event": handler } }],
        }),
      );
      await fastify.ready();

      const messageBody = {
        event: "fail.event",
        payload: {},
        file: "fail.ts",
        publishTimestamp: Date.now(),
      };

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        payload: {
          messageId: 1,
          body: JSON.stringify(messageBody),
        },
        headers: { "content-type": "application/json" },
      });

      expect(processError).toHaveBeenCalled();
      expect(resp.statusCode).toBe(502);
    }),
  );

  it(
    "should return OK for empty/null body",
    withAzureEnv(async () => {
      fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOptions({
          busType: "azure-servicebus",
          topic: "test-topic",
          handlers: [],
        }),
      );
      await fastify.ready();

      const resp = await fastify.inject({
        method: "POST",
        url: "/azure-servicebus/process-message",
        headers: { "content-type": "application/json" },
      });

      expect(resp.statusCode).toBeLessThan(500);
    }),
  );
});
