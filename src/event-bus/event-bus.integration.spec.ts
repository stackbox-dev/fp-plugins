import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { connect as natsConnect } from "@nats-io/transport-node";
import Fastify from "fastify";
import { Connection } from "rabbitmq-client";
import { Plugins } from "../index";
import { CreateEventConsumer } from "./event-consumer";
import { EventBusOptions, EventHandler, EventMessage } from "./interfaces";

const CONTAINER_PREFIX = "fptest";
const SUFFIX = Math.floor(Math.random() * 5_000);

/** Poll until predicate returns true, or throw after timeoutMs. */
async function waitFor(
  predicate: () => boolean,
  timeoutMs = 10_000,
  intervalMs = 100,
): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs)
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

const containers: string[] = [];
const networks: string[] = [];
const envSnapshot: Record<string, string | undefined> = {};

function setEnv(key: string, value: string) {
  if (!(key in envSnapshot)) envSnapshot[key] = process.env[key];
  process.env[key] = value;
}

function restoreEnv() {
  for (const [key, original] of Object.entries(envSnapshot)) {
    if (original === undefined) delete process.env[key];
    else process.env[key] = original;
  }
}

function docker(...args: string[]) {
  execFileSync("docker", args, { stdio: "pipe", timeout: 30_000 });
}

interface DockerRunOpts {
  ports?: [number, number][];
  cmdArgs?: string[];
  envVars?: string[];
  network?: string;
  volumes?: string[];
}

function dockerRun(name: string, image: string, opts: DockerRunOpts = {}) {
  const portFlags = (opts.ports ?? []).flatMap(([host, container]) => [
    "-p",
    `${host}:${container}`,
  ]);
  const envFlags = (opts.envVars ?? []).flatMap((e) => ["-e", e]);
  const netFlags = opts.network ? ["--network", opts.network] : [];
  const volFlags = (opts.volumes ?? []).flatMap((v) => ["-v", v]);
  docker(
    "run",
    "-d",
    "--rm",
    "--name",
    name,
    ...portFlags,
    ...envFlags,
    ...netFlags,
    ...volFlags,
    image,
    ...(opts.cmdArgs ?? []),
  );
  containers.push(name);
}

function dockerCreateNetwork(name: string) {
  try {
    docker("network", "create", name);
  } catch {
    /* already exists */
  }
  networks.push(name);
}

function dockerRemoveNetwork(name: string) {
  try {
    docker("network", "rm", name);
  } catch {
    /* already removed */
  }
}

function dockerStop(name: string) {
  try {
    docker("stop", name);
  } catch {
    /* already stopped */
  }
}

function waitForPort(port: number, maxWaitMs = 30_000): Promise<void> {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      const sock = net.createConnection({ port, host: "127.0.0.1" }, () => {
        sock.destroy();
        resolve();
      });
      sock.on("error", () => {
        if (Date.now() - start > maxWaitMs)
          reject(new Error(`Port ${port} not ready after ${maxWaitMs}ms`));
        else setTimeout(check, 500);
      });
    };
    check();
  });
}

function waitForHttp(
  port: number,
  path: string = "/",
  maxWaitMs = 30_000,
): Promise<void> {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      const http = require("node:http");
      const req = http.get(`http://127.0.0.1:${port}${path}`, (res: any) => {
        res.resume();
        resolve();
      });
      req.on("error", () => {
        if (Date.now() - start > maxWaitMs)
          reject(
            new Error(`HTTP ${port}${path} not ready after ${maxWaitMs}ms`),
          );
        else setTimeout(check, 1000);
      });
      req.setTimeout(2000, () => {
        req.destroy();
        if (Date.now() - start > maxWaitMs)
          reject(
            new Error(`HTTP ${port}${path} timed out after ${maxWaitMs}ms`),
          );
        else setTimeout(check, 1000);
      });
    };
    check();
  });
}

function cleanup() {
  for (const name of containers) dockerStop(name);
  for (const name of networks) dockerRemoveNetwork(name);
  restoreEnv();
}

afterAll(cleanup, 30_000);
process.on("SIGINT", () => {
  cleanup();
  process.exit(130);
});
process.on("SIGTERM", () => {
  cleanup();
  process.exit(143);
});

// Clean up orphaned containers and networks from previous failed runs
beforeAll(() => {
  const prefix = CONTAINER_PREFIX;
  try {
    const running = execFileSync(
      "docker",
      ["ps", "-a", "--filter", `name=^${prefix}-`, "--format", "{{.Names}}"],
      { encoding: "utf8", timeout: 5000 },
    );
    for (const line of running.split("\n")) {
      const name = line.trim();
      if (name) dockerStop(name);
    }
  } catch {
    /* docker not available or no orphans */
  }
  try {
    const nets = execFileSync(
      "docker",
      [
        "network",
        "ls",
        "--filter",
        `name=^${prefix}-`,
        "--format",
        "{{.Name}}",
      ],
      { encoding: "utf8", timeout: 5000 },
    );
    for (const line of nets.split("\n")) {
      const name = line.trim();
      if (name) dockerRemoveNetwork(name);
    }
  } catch {
    /* no orphan networks */
  }
}, 15_000);

// ---------------------------------------------------------------------------
// Helper: create EventBusOptions
// ---------------------------------------------------------------------------

function makeOpts(
  busType: EventBusOptions["busType"],
  handlers: EventBusOptions["handlers"],
  extra: Partial<EventBusOptions> = {},
): EventBusOptions {
  return {
    busType,
    handlers,
    validateMsg: jest.fn(),
    processError: jest
      .fn()
      .mockReturnValue({ err: new Error("test"), status: 500 }),
    ...extra,
  };
}

// ---------------------------------------------------------------------------
// RabbitMQ
// ---------------------------------------------------------------------------

const RABBITMQ_PORT = 25_672 + SUFFIX;

describe("RabbitMQ Integration", () => {
  let rabbitmqUrl: string;

  beforeAll(async () => {
    dockerRun(`${CONTAINER_PREFIX}-rabbitmq-${SUFFIX}`, "rabbitmq:4-alpine", {
      ports: [[RABBITMQ_PORT, 5672]],
    });
    await waitForPort(RABBITMQ_PORT);
    rabbitmqUrl = `amqp://guest:guest@127.0.0.1:${RABBITMQ_PORT}`;
    // RabbitMQ AMQP port opens before the broker is fully initialized.
    // Probe with actual AMQP connection until the broker accepts.
    const readyStart = Date.now();
    while (Date.now() - readyStart < 30_000) {
      try {
        const probe = new Connection(rabbitmqUrl);
        await new Promise<void>((resolve, reject) => {
          probe.on("connection", () => {
            probe.close().then(resolve, resolve);
          });
          probe.on("error", reject);
        });
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }, 60_000);

  it("should publish and consume events round-trip", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "integ-test-service");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "test.ts", handlers: { "test.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      fastify.EventBus.publish("test.event", { hello: "world" });
      await waitFor(() => received.length > 0, 10_000);

      expect(handler).toHaveBeenCalled();
      expect(received[0].data).toEqual({ hello: "world" });
      expect(received[0].event).toBe("test.event");
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should handle delayed messages (processAfterDelayMs)", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "delay-integ-service");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "delay.ts", handlers: { "delay.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      fastify.EventBus.publish("delay.event", { delayed: true }, 2000);

      await new Promise((r) => setTimeout(r, 500));
      expect(received.length).toBe(0);

      await new Promise((r) => setTimeout(r, 15_000));
      expect(received.length).toBeGreaterThan(0);
      expect(received[0].data).toEqual({ delayed: true });
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 30_000);

  it("should handle multiple handlers for same event", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "multi-handler-service");

    const received1: EventMessage[] = [];
    const received2: EventMessage[] = [];
    const handler1: EventHandler = jest.fn(async (msg) => {
      received1.push(msg);
    });
    const handler2: EventHandler = jest.fn(async (msg) => {
      received2.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [
          { file: "handler1.ts", handlers: { "multi.event": handler1 } },
          { file: "handler2.ts", handlers: { "multi.event": handler2 } },
        ],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      fastify.EventBus.publish("multi.event", { multi: true });
      await waitFor(() => received1.length > 0 && received2.length > 0, 10_000);

      expect(handler1).toHaveBeenCalled();
      expect(handler2).toHaveBeenCalled();
      expect(received1[0].data).toEqual({ multi: true });
      expect(received2[0].data).toEqual({ multi: true });
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should handle handler errors with retry via file attribute", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "error-retry-service");

    let callCount = 0;
    const handler: EventHandler = jest.fn(async () => {
      callCount++;
      if (callCount === 1) throw new Error("transient failure");
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "retry.ts", handlers: { "retry.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      fastify.EventBus.publish("retry.event", { retry: true });
      await waitFor(() => callCount >= 2, 10_000);

      expect(callCount).toBeGreaterThanOrEqual(2);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should gracefully shutdown without hanging", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "shutdown-test-service");

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [
          {
            file: "test.ts",
            handlers: { "shutdown.event": jest.fn(async () => {}) },
          },
        ],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    fastify.EventBus.publish("shutdown.event", { data: 1 });
    fastify.EventBus.publish("shutdown.event", { data: 2 });
    await new Promise((r) => setTimeout(r, 500));

    const start = Date.now();
    await consumer.close();
    await fastify.close();
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(5000);
    restoreEnv();
  });

  it("should handle high-throughput batch of messages", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "batch-service");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "batch.ts", handlers: { "batch.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      const COUNT = 20;
      for (let i = 0; i < COUNT; i++) {
        fastify.EventBus.publish("batch.event", { index: i });
      }
      await waitFor(() => received.length >= COUNT, 15_000);

      expect(received.length).toBe(COUNT);
      const indices = received.map((r) => r.data.index).sort((a, b) => a - b);
      expect(indices).toEqual(Array.from({ length: COUNT }, (_, i) => i));
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 15_000);

  it("should preserve payload types (objects, arrays, nested)", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "payload-types-service");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "test.ts", handlers: { "payload.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    const complexPayload = {
      string: "hello",
      number: 42,
      float: 3.14,
      bool: true,
      null_val: null,
      array: [1, "two", { three: 3 }],
      nested: { deep: { deeper: { value: "found" } } },
    };

    try {
      fastify.EventBus.publish("payload.event", complexPayload);
      await waitFor(() => received.length >= 1, 10_000);

      expect(received.length).toBe(1);
      expect(received[0].data).toEqual(complexPayload);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should skip events with no registered handlers", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "no-handler-service");

    const handler: EventHandler = jest.fn(async () => {});

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "test.ts", handlers: { "registered.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      // Publish an event that has no handler
      fastify.EventBus.publish("unregistered.event", { data: 1 });
      // Negative assertion: wait a reasonable time and verify handler was NOT called
      await new Promise((r) => setTimeout(r, 1500));

      expect(handler).not.toHaveBeenCalled();
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should handle 500 handler errors and redeliver via DLX", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "error-500-service");

    let callCount = 0;
    const handler: EventHandler = jest.fn(async () => {
      callCount++;
      if (callCount <= 2) throw new Error("internal failure");
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "err500.ts", handlers: { "err500.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      fastify.EventBus.publish("err500.event", { fail: true });
      // Wait for DLX retry cycles (5s TTL each)
      await waitFor(() => callCount >= 3, 20_000);

      expect(callCount).toBeGreaterThanOrEqual(3);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 25_000);
});

// ---------------------------------------------------------------------------
// NATS JetStream
// ---------------------------------------------------------------------------

const NATS_PORT = 14_222 + SUFFIX;

describe("NATS JetStream Integration", () => {
  let natsUrl: string;

  beforeAll(async () => {
    dockerRun(`${CONTAINER_PREFIX}-nats-${SUFFIX}`, "nats:latest", {
      ports: [[NATS_PORT, 4222]],
      cmdArgs: ["-js"],
    });
    await waitForPort(NATS_PORT);
    await new Promise((r) => setTimeout(r, 1000));
    natsUrl = `127.0.0.1:${NATS_PORT}`;

    // Create stream and consumer for tests
    const nc = await natsConnect({ servers: natsUrl });
    const jsm = await jetstreamManager(nc);
    await jsm.streams.add({ name: "EVENTS", subjects: ["events.>"] });
    await jsm.consumers.add("EVENTS", {
      durable_name: "test-consumer",
      ack_policy: "explicit",
    });
    await nc.drain();
  }, 45_000);

  // Purge stream between tests to prevent message bleed from shared durable consumer
  beforeEach(async () => {
    const nc = await natsConnect({ servers: natsUrl });
    const jsm = await jetstreamManager(nc);
    await jsm.streams.purge("EVENTS");
    await nc.drain();
  });

  it("should publish and consume events round-trip", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "test.ts", handlers: { "test.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      fastify.EventBus.publish("test.event", { hello: "nats" });
      await waitFor(() => received.length > 0, 10_000);

      expect(handler).toHaveBeenCalled();
      expect(received[0].data).toEqual({ hello: "nats" });
      expect(received[0].event).toBe("test.event");
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should handle multiple events in sequence", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "test.ts", handlers: { "seq.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      for (let i = 0; i < 5; i++) {
        fastify.EventBus.publish("seq.event", { index: i });
      }
      await waitFor(() => received.length >= 5, 10_000);

      expect(received.length).toBe(5);
      const indices = received.map((r) => r.data.index).sort();
      expect(indices).toEqual([0, 1, 2, 3, 4]);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should gracefully shutdown without hanging", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [
          {
            file: "test.ts",
            handlers: { "shutdown.event": jest.fn(async () => {}) },
          },
        ],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    fastify.EventBus.publish("shutdown.event", { data: 1 });
    await new Promise((r) => setTimeout(r, 500));

    const start = Date.now();
    await consumer.close();
    await fastify.close();
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(5000);
    restoreEnv();
  });

  it("should handle delayed messages (processAfterDelayMs) via 425 + nak cycle", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "delay.ts", handlers: { "delay.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      fastify.EventBus.publish("delay.event", { delayed: true }, 2000);

      // Should NOT be processed immediately
      await new Promise((r) => setTimeout(r, 500));
      expect(received.length).toBe(0);

      // Wait for delay expiry + randomDelay + nak/redeliver cycle
      await new Promise((r) => setTimeout(r, 15_000));
      expect(received.length).toBeGreaterThan(0);
      expect(received[0].data).toEqual({ delayed: true });
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 30_000);

  it("should handle multiple handlers for same event", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const received1: EventMessage[] = [];
    const received2: EventMessage[] = [];

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [
          {
            file: "h1.ts",
            handlers: {
              "multi.event": jest.fn(async (msg) => {
                received1.push(msg);
              }),
            },
          },
          {
            file: "h2.ts",
            handlers: {
              "multi.event": jest.fn(async (msg) => {
                received2.push(msg);
              }),
            },
          },
        ],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      fastify.EventBus.publish("multi.event", { multi: true });
      await waitFor(
        () => received1.length >= 1 && received2.length >= 1,
        10_000,
      );

      expect(received1.length).toBe(1);
      expect(received2.length).toBe(1);
      expect(received1[0].data).toEqual({ multi: true });
      expect(received2[0].data).toEqual({ multi: true });
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should handle handler errors with retry via nak/redeliver", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    let callCount = 0;
    const handler: EventHandler = jest.fn(async () => {
      callCount++;
      if (callCount === 1) throw new Error("transient failure");
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "retry.ts", handlers: { "retry.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      fastify.EventBus.publish("retry.event", { retry: true });
      await waitFor(() => callCount >= 2, 10_000);

      expect(callCount).toBeGreaterThanOrEqual(2);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 15_000);

  it("should preserve complex payload types", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "test.ts", handlers: { "payload.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    const payload = {
      string: "hello",
      number: 42,
      nested: { a: { b: { c: [1, 2, 3] } } },
      null_val: null,
      bool: false,
    };

    try {
      fastify.EventBus.publish("payload.event", payload);
      await waitFor(() => received.length >= 1, 10_000);

      expect(received.length).toBe(1);
      expect(received[0].data).toEqual(payload);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should handle high-throughput batch", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "batch.ts", handlers: { "batch.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      const COUNT = 20;
      for (let i = 0; i < COUNT; i++) {
        fastify.EventBus.publish("batch.event", { index: i });
      }
      await waitFor(() => received.length >= COUNT, 15_000);

      expect(received.length).toBe(COUNT);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 15_000);

  it("should skip events with no registered handlers", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    const handler: EventHandler = jest.fn(async () => {});

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "test.ts", handlers: { "registered.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      fastify.EventBus.publish("unregistered.event", { data: 1 });
      // Negative assertion: wait a reasonable time and verify handler was NOT called
      await new Promise((r) => setTimeout(r, 2000));

      expect(handler).not.toHaveBeenCalled();
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  });

  it("should handle 500 errors and redeliver via nak", async () => {
    setEnv("NATS_SERVERS", natsUrl);
    setEnv("NATS_STREAM", "EVENTS");
    setEnv("NATS_CONSUMER", "test-consumer");

    let callCount = 0;
    const handler: EventHandler = jest.fn(async () => {
      callCount++;
      if (callCount <= 2) throw new Error("server error");
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "err.ts", handlers: { "err500.event": handler } }],
        { topic: "events" },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "nats-jetstream");

    try {
      fastify.EventBus.publish("err500.event", { fail: true });
      await waitFor(() => callCount >= 3, 10_000);

      expect(callCount).toBeGreaterThanOrEqual(3);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 15_000);

  it("should log error when publish retries are exhausted", async () => {
    setEnv("NATS_SERVERS", natsUrl);

    const logErrors: any[] = [];
    const fastify = Fastify({
      logger: {
        level: "error",
        transport: undefined,
      },
    });
    // Capture log.error calls
    const origError = fastify.log.error.bind(fastify.log);
    fastify.log.error = ((...args: any[]) => {
      logErrors.push(args[0]);
      origError(...args);
    }) as any;

    // Register with a topic prefix that won't match any stream subject,
    // so JetStream publish will fail (no stream matches the subject).
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "nats-jetstream",
        [{ file: "test.ts", handlers: { "test.event": jest.fn() } }],
        { topic: "no-stream-bound" },
      ),
    );
    await fastify.ready();

    try {
      fastify.EventBus.publish("test.event", { will_fail: true });
      // Wait for retry exhaustion (3 attempts * backoff ≈ ~2s)
      await waitFor(
        () => logErrors.some((e) => e?.tag === "NATS_JETSTREAM_PUBLISH_ERROR"),
        10_000,
      );

      const publishError = logErrors.find(
        (e) => e?.tag === "NATS_JETSTREAM_PUBLISH_ERROR",
      );
      expect(publishError).toBeDefined();
      expect(publishError.attempts).toBe(3);
    } finally {
      await fastify.close();
      restoreEnv();
    }
  }, 15_000);
});

// ---------------------------------------------------------------------------
// GCP Pub/Sub (Emulator via Docker)
// ---------------------------------------------------------------------------

const GCP_PUBSUB_PORT = 18_085 + SUFFIX;

describe("GCP Pub/Sub Emulator Integration", () => {
  const projectId = "test-project";
  const topicId = `test-topic-${SUFFIX}`;
  const subscriptionId = `test-sub-${SUFFIX}`;
  let emulatorHost: string;

  beforeAll(async () => {
    emulatorHost = `127.0.0.1:${GCP_PUBSUB_PORT}`;

    dockerRun(
      `${CONTAINER_PREFIX}-gcp-pubsub-${SUFFIX}`,
      "google/cloud-sdk:emulators",
      {
        ports: [[GCP_PUBSUB_PORT, 8085]],
        cmdArgs: [
          "gcloud",
          "beta",
          "emulators",
          "pubsub",
          "start",
          `--project=${projectId}`,
          "--host-port=0.0.0.0:8085",
        ],
      },
    );

    // Wait for emulator REST API to actually respond (not just the port).
    // The emulator takes a while to start up inside the container.
    await waitForHttp(GCP_PUBSUB_PORT, "/", 60_000);
    await new Promise((r) => setTimeout(r, 1000));

    // Create topic and subscription via the emulator's REST API
    const http = require("node:http");

    function httpPut(url: string, body: string): Promise<number> {
      return new Promise((resolve, reject) => {
        const req = http.request(
          url,
          {
            method: "PUT",
            headers: {
              "content-type": "application/json",
              "content-length": Buffer.byteLength(body),
            },
          },
          (res: any) => {
            let data = "";
            res.on("data", (chunk: string) => {
              data += chunk;
            });
            res.on("end", () => {
              if (res.statusCode >= 400 && res.statusCode !== 409) {
                reject(new Error(`HTTP ${res.statusCode}: ${data}`));
              } else {
                resolve(res.statusCode);
              }
            });
          },
        );
        req.on("error", reject);
        req.setTimeout(5000, () => {
          req.destroy();
          reject(new Error("timeout"));
        });
        req.end(body);
      });
    }

    // Create topic (409 = already exists, fine)
    await httpPut(
      `http://${emulatorHost}/v1/projects/${projectId}/topics/${topicId}`,
      "{}",
    );

    // Create subscription (409 = already exists, fine)
    await httpPut(
      `http://${emulatorHost}/v1/projects/${projectId}/subscriptions/${subscriptionId}`,
      JSON.stringify({
        topic: `projects/${projectId}/topics/${topicId}`,
        ackDeadlineSeconds: 30,
      }),
    );
  }, 90_000);

  it("should publish and consume events round-trip", async () => {
    setEnv("PUBSUB_EMULATOR_HOST", emulatorHost);
    setEnv("GCLOUD_PROJECT", projectId);
    setEnv("PUBSUB_PROJECT_ID", projectId);
    setEnv("EVENT_TOPIC", topicId);
    setEnv(
      "EVENT_SUBSCRIPTION",
      `projects/${projectId}/subscriptions/${subscriptionId}`,
    );

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "gcp-pubsub",
        [{ file: "test.ts", handlers: { "test.event": handler } }],
        { topic: topicId },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "gcp-pubsub");

    try {
      fastify.EventBus.publish("test.event", { hello: "gcp" });
      await waitFor(() => received.length > 0, 10_000);

      expect(handler).toHaveBeenCalled();
      expect(received[0].data).toEqual({ hello: "gcp" });
      expect(received[0].event).toBe("test.event");
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 20_000);

  it("should handle multiple messages", async () => {
    setEnv("PUBSUB_EMULATOR_HOST", emulatorHost);
    setEnv("GCLOUD_PROJECT", projectId);
    setEnv("PUBSUB_PROJECT_ID", projectId);
    setEnv("EVENT_TOPIC", topicId);
    setEnv(
      "EVENT_SUBSCRIPTION",
      `projects/${projectId}/subscriptions/${subscriptionId}`,
    );

    const received: EventMessage[] = [];
    const handler: EventHandler = jest.fn(async (msg) => {
      received.push(msg);
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "gcp-pubsub",
        [{ file: "test.ts", handlers: { "multi.event": handler } }],
        { topic: topicId },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "gcp-pubsub");

    try {
      for (let i = 0; i < 5; i++) {
        fastify.EventBus.publish("multi.event", { index: i });
      }
      await waitFor(() => received.length >= 5, 15_000);

      expect(received.length).toBe(5);
      const indices = received.map((r) => r.data.index).sort((a, b) => a - b);
      expect(indices).toEqual([0, 1, 2, 3, 4]);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 20_000);

  it("should handle multiple handlers for same event", async () => {
    setEnv("PUBSUB_EMULATOR_HOST", emulatorHost);
    setEnv("GCLOUD_PROJECT", projectId);
    setEnv("PUBSUB_PROJECT_ID", projectId);
    setEnv("EVENT_TOPIC", topicId);
    setEnv(
      "EVENT_SUBSCRIPTION",
      `projects/${projectId}/subscriptions/${subscriptionId}`,
    );

    const received1: EventMessage[] = [];
    const received2: EventMessage[] = [];

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "gcp-pubsub",
        [
          {
            file: "h1.ts",
            handlers: {
              "dual.event": jest.fn(async (msg) => {
                received1.push(msg);
              }),
            },
          },
          {
            file: "h2.ts",
            handlers: {
              "dual.event": jest.fn(async (msg) => {
                received2.push(msg);
              }),
            },
          },
        ],
        { topic: topicId },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "gcp-pubsub");

    try {
      fastify.EventBus.publish("dual.event", { dual: true });
      await waitFor(
        () => received1.length >= 1 && received2.length >= 1,
        10_000,
      );

      expect(received1.length).toBe(1);
      expect(received2.length).toBe(1);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 20_000);

  it("should gracefully shutdown without hanging", async () => {
    setEnv("PUBSUB_EMULATOR_HOST", emulatorHost);
    setEnv("GCLOUD_PROJECT", projectId);
    setEnv("PUBSUB_PROJECT_ID", projectId);
    setEnv("EVENT_TOPIC", topicId);
    setEnv(
      "EVENT_SUBSCRIPTION",
      `projects/${projectId}/subscriptions/${subscriptionId}`,
    );

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "gcp-pubsub",
        [
          {
            file: "test.ts",
            handlers: { "shutdown.event": jest.fn(async () => {}) },
          },
        ],
        { topic: topicId },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "gcp-pubsub");

    fastify.EventBus.publish("shutdown.event", { data: 1 });
    await new Promise((r) => setTimeout(r, 1000));

    const start = Date.now();
    await consumer.close();
    await fastify.close();
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(10_000);
    restoreEnv();
  }, 20_000);

  it("should skip events with no registered handlers", async () => {
    setEnv("PUBSUB_EMULATOR_HOST", emulatorHost);
    setEnv("GCLOUD_PROJECT", projectId);
    setEnv("PUBSUB_PROJECT_ID", projectId);
    setEnv("EVENT_TOPIC", topicId);
    setEnv(
      "EVENT_SUBSCRIPTION",
      `projects/${projectId}/subscriptions/${subscriptionId}`,
    );

    const handler: EventHandler = jest.fn(async () => {});

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "gcp-pubsub",
        [{ file: "test.ts", handlers: { "registered.event": handler } }],
        { topic: topicId },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "gcp-pubsub");

    try {
      fastify.EventBus.publish("unregistered.event", { data: 1 });
      // Negative assertion: wait a reasonable time and verify handler was NOT called
      await new Promise((r) => setTimeout(r, 3000));

      expect(handler).not.toHaveBeenCalled();
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 20_000);

  it("should handle handler errors and redeliver via nack", async () => {
    setEnv("PUBSUB_EMULATOR_HOST", emulatorHost);
    setEnv("GCLOUD_PROJECT", projectId);
    setEnv("PUBSUB_PROJECT_ID", projectId);
    setEnv("EVENT_TOPIC", topicId);
    setEnv(
      "EVENT_SUBSCRIPTION",
      `projects/${projectId}/subscriptions/${subscriptionId}`,
    );

    let callCount = 0;
    const handler: EventHandler = jest.fn(async () => {
      callCount++;
      if (callCount === 1) throw new Error("transient failure");
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "gcp-pubsub",
        [{ file: "err.ts", handlers: { "err.event": handler } }],
        { topic: topicId },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "gcp-pubsub");

    try {
      fastify.EventBus.publish("err.event", { fail: true });
      await waitFor(() => callCount >= 2, 15_000);

      expect(callCount).toBeGreaterThanOrEqual(2);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 20_000);
});

// ---------------------------------------------------------------------------
// Azure ServiceBus (Emulator via Docker: SQL Server + SB Emulator)
// ---------------------------------------------------------------------------

const AZURE_SB_PORT = 15_672 + SUFFIX;
const AZURE_SQL_PORT = 11_433 + SUFFIX;
const AZURE_NETWORK = `${CONTAINER_PREFIX}-azure-net-${SUFFIX}`;
const AZURE_SQL_NAME = `${CONTAINER_PREFIX}-azure-sql-${SUFFIX}`;
const AZURE_SB_NAME = `${CONTAINER_PREFIX}-azure-sb-${SUFFIX}`;
const AZURE_SA_PASSWORD = "Test@12345678";
const AZURE_TOPIC = "test-topic";
const AZURE_SUBSCRIPTION = "test-sub";

describe("Azure ServiceBus Emulator Integration", () => {
  let connectionString: string;
  let emulatorReady = false;

  beforeAll(async () => {
    // 1. Create a Docker network so SQL + SB emulator can talk
    dockerCreateNetwork(AZURE_NETWORK);

    // 2. Start SQL Server (required by the SB emulator)
    dockerRun(AZURE_SQL_NAME, "mcr.microsoft.com/mssql/server:2022-latest", {
      ports: [[AZURE_SQL_PORT, 1433]],
      envVars: ["ACCEPT_EULA=Y", `MSSQL_SA_PASSWORD=${AZURE_SA_PASSWORD}`],
      network: AZURE_NETWORK,
    });
    await waitForPort(AZURE_SQL_PORT, 60_000);
    // SQL Server takes a while to initialize after port is up
    await new Promise((r) => setTimeout(r, 10_000));

    // 3. Write emulator config to a temp file
    const config = {
      UserConfig: {
        Namespaces: [
          {
            Name: "sbemulatorns",
            Topics: [
              {
                Name: AZURE_TOPIC,
                Properties: {},
                Subscriptions: [
                  {
                    Name: AZURE_SUBSCRIPTION,
                    Properties: {},
                    Rules: [],
                  },
                ],
              },
            ],
          },
        ],
        Logging: { Type: "Console" },
      },
    };
    const configDir = fs.mkdtempSync(path.join(os.tmpdir(), "sb-emu-"));
    const configPath = path.join(configDir, "Config.json");
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

    // 4. Start the ServiceBus emulator
    dockerRun(
      AZURE_SB_NAME,
      "mcr.microsoft.com/azure-messaging/servicebus-emulator:latest",
      {
        ports: [[AZURE_SB_PORT, 5672]],
        envVars: [
          "ACCEPT_EULA=Y",
          `SQL_SERVER=${AZURE_SQL_NAME}`,
          `MSSQL_SA_PASSWORD=${AZURE_SA_PASSWORD}`,
        ],
        network: AZURE_NETWORK,
        volumes: [`${configPath}:/ServiceBus_Emulator/ConfigFiles/Config.json`],
      },
    );

    // 5. Wait for the emulator to be ready. The port opens early but the
    // AMQP protocol layer isn't ready until SQL schema init completes.
    // Repeatedly try a test connection until it stops getting ECONNRESET.
    await waitForPort(AZURE_SB_PORT, 120_000);
    const { ServiceBusClient: SBClient } = require("@azure/service-bus");
    const testConnStr = `Endpoint=sb://localhost:${AZURE_SB_PORT};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`;
    // Probe: repeatedly try to send a test message until the emulator
    // stops returning ECONNRESET. If it doesn't stabilize in 90s, mark
    // tests as skipped rather than failing.
    const readyStart = Date.now();
    while (Date.now() - readyStart < 90_000) {
      try {
        const testClient = new SBClient(testConnStr);
        const testSender = testClient.createSender(AZURE_TOPIC);
        await testSender.sendMessages({ body: "probe" });
        await testSender.close();
        await testClient.close();
        emulatorReady = true;
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 5000));
      }
    }

    connectionString = `Endpoint=sb://localhost:${AZURE_SB_PORT};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`;
  }, 180_000);

  // Shared Fastify + consumer across all Azure tests. The emulator is slow
  // to re-attach subscription consumers after disconnect, so we keep a
  // single long-lived connection.
  let fastify: ReturnType<typeof Fastify>;
  let consumer: { close(): Promise<void> };
  const received: EventMessage[] = [];
  const receivedDual1: EventMessage[] = [];
  const receivedDual2: EventMessage[] = [];

  beforeAll(async () => {
    if (!emulatorReady) return;

    setEnv("AZURE_SERVICEBUS_CONNECTION_STRING", connectionString);
    setEnv("EVENT_TOPIC", AZURE_TOPIC);
    setEnv("EVENT_SUBSCRIPTION", AZURE_SUBSCRIPTION);

    fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "azure-servicebus",
        [
          {
            file: "h1.ts",
            handlers: {
              "test.event": jest.fn(async (msg) => {
                received.push(msg);
              }),
              "dual.event": jest.fn(async (msg) => {
                receivedDual1.push(msg);
              }),
            },
          },
          {
            file: "h2.ts",
            handlers: {
              "dual.event": jest.fn(async (msg) => {
                receivedDual2.push(msg);
              }),
            },
          },
        ],
        { topic: AZURE_TOPIC, namespace: "unused-with-connstr" },
      ),
    );
    await fastify.ready();

    consumer = await CreateEventConsumer(fastify, "azure-servicebus");
    // Wait for consumer to fully attach
    await new Promise((r) => setTimeout(r, 5000));
  }, 30_000);

  afterAll(async () => {
    if (!emulatorReady) return;
    try {
      await consumer?.close();
    } catch {
      /* */
    }
    try {
      await fastify?.close();
    } catch {
      /* */
    }
    restoreEnv();
  }, 20_000);

  function skipIfNotReady() {
    if (!emulatorReady) {
      console.warn("Azure ServiceBus emulator not ready — skipping");
      return true;
    }
    return false;
  }

  it("should publish and consume events round-trip", async () => {
    if (skipIfNotReady()) return;

    const before = received.length;
    fastify.EventBus.publish("test.event", { hello: "azure" });
    await waitFor(() => received.length > before, 15_000);

    expect(received.length).toBeGreaterThan(before);
    const msg = received[received.length - 1];
    expect(msg.data).toEqual({ hello: "azure" });
    expect(msg.event).toBe("test.event");
  }, 30_000);

  it("should handle batch of messages", async () => {
    if (skipIfNotReady()) return;

    const before = received.length;
    for (let i = 0; i < 3; i++) {
      fastify.EventBus.publish("test.event", { batch: i });
    }
    await waitFor(() => received.length >= before + 3, 15_000);

    expect(received.length).toBe(before + 3);
  }, 30_000);

  it("should handle multiple handlers for same event", async () => {
    if (skipIfNotReady()) return;

    const before1 = receivedDual1.length;
    const before2 = receivedDual2.length;
    fastify.EventBus.publish("dual.event", { dual: true });
    await waitFor(
      () => receivedDual1.length > before1 && receivedDual2.length > before2,
      15_000,
    );

    expect(receivedDual1.length).toBe(before1 + 1);
    expect(receivedDual2.length).toBe(before2 + 1);
    expect(receivedDual1[receivedDual1.length - 1].data).toEqual({
      dual: true,
    });
  }, 30_000);

  it("should preserve complex payload types", async () => {
    if (skipIfNotReady()) return;

    const before = received.length;
    const payload = {
      string: "hello",
      number: 42,
      nested: { a: { b: [1, 2, 3] } },
      null_val: null,
    };
    fastify.EventBus.publish("test.event", payload);
    await waitFor(() => received.length > before, 15_000);

    expect(received.length).toBeGreaterThan(before);
    expect(received[received.length - 1].data).toEqual(payload);
  }, 30_000);

  it("should gracefully shutdown without hanging", async () => {
    if (skipIfNotReady()) return;

    fastify.EventBus.publish("test.event", { shutdown: true });
    await new Promise((r) => setTimeout(r, 2000));

    const start = Date.now();
    await consumer.close();
    await fastify.close();
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(15_000);
    // Prevent afterAll from double-closing
    emulatorReady = false;
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Consumer validation & edge cases (no Docker needed for most)
// ---------------------------------------------------------------------------

describe("Consumer factory and validation", () => {
  it("CreateEventConsumer with in-process type returns noop consumer", async () => {
    const fastify = Fastify({ logger: false });
    await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "in-process");
    // Should not throw
    await consumer.close();
    await fastify.close();
  });

  it("NATS consumer should throw without NATS_SERVERS", async () => {
    const orig = process.env.NATS_SERVERS;
    delete process.env.NATS_SERVERS;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
      await fastify.ready();
      await expect(
        CreateEventConsumer(fastify, "nats-jetstream"),
      ).rejects.toThrow("NATS_SERVERS");
      await fastify.close();
    } finally {
      if (orig !== undefined) process.env.NATS_SERVERS = orig;
    }
  });

  it("NATS consumer should throw without NATS_STREAM", async () => {
    const origServers = process.env.NATS_SERVERS;
    const origStream = process.env.NATS_STREAM;
    process.env.NATS_SERVERS = "127.0.0.1:4222";
    delete process.env.NATS_STREAM;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
      await fastify.ready();
      await expect(
        CreateEventConsumer(fastify, "nats-jetstream"),
      ).rejects.toThrow("NATS_STREAM");
      await fastify.close();
    } finally {
      if (origServers !== undefined) process.env.NATS_SERVERS = origServers;
      else delete process.env.NATS_SERVERS;
      if (origStream !== undefined) process.env.NATS_STREAM = origStream;
    }
  });

  it("NATS consumer should throw without NATS_CONSUMER", async () => {
    const origServers = process.env.NATS_SERVERS;
    const origStream = process.env.NATS_STREAM;
    const origConsumer = process.env.NATS_CONSUMER;
    process.env.NATS_SERVERS = "127.0.0.1:4222";
    process.env.NATS_STREAM = "EVENTS";
    delete process.env.NATS_CONSUMER;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
      await fastify.ready();
      await expect(
        CreateEventConsumer(fastify, "nats-jetstream"),
      ).rejects.toThrow("NATS_CONSUMER");
      await fastify.close();
    } finally {
      if (origServers !== undefined) process.env.NATS_SERVERS = origServers;
      else delete process.env.NATS_SERVERS;
      if (origStream !== undefined) process.env.NATS_STREAM = origStream;
      else delete process.env.NATS_STREAM;
      if (origConsumer !== undefined) process.env.NATS_CONSUMER = origConsumer;
    }
  });

  it("Azure consumer should throw without EVENT_TOPIC", async () => {
    const orig = process.env.EVENT_TOPIC;
    delete process.env.EVENT_TOPIC;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
      await fastify.ready();
      await expect(
        CreateEventConsumer(fastify, "azure-servicebus"),
      ).rejects.toThrow("EVENT_TOPIC");
      await fastify.close();
    } finally {
      if (orig !== undefined) process.env.EVENT_TOPIC = orig;
    }
  });

  it("Azure consumer should throw without EVENT_SUBSCRIPTION", async () => {
    const origTopic = process.env.EVENT_TOPIC;
    const origSub = process.env.EVENT_SUBSCRIPTION;
    process.env.EVENT_TOPIC = "test-topic";
    delete process.env.EVENT_SUBSCRIPTION;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
      await fastify.ready();
      await expect(
        CreateEventConsumer(fastify, "azure-servicebus"),
      ).rejects.toThrow("EVENT_SUBSCRIPTION");
      await fastify.close();
    } finally {
      if (origTopic !== undefined) process.env.EVENT_TOPIC = origTopic;
      else delete process.env.EVENT_TOPIC;
      if (origSub !== undefined) process.env.EVENT_SUBSCRIPTION = origSub;
    }
  });

  it("Azure consumer should throw without connection string or namespace", async () => {
    const origTopic = process.env.EVENT_TOPIC;
    const origSub = process.env.EVENT_SUBSCRIPTION;
    const origConn = process.env.AZURE_SERVICEBUS_CONNECTION_STRING;
    const origNs = process.env.EVENT_NAMESPACE;
    process.env.EVENT_TOPIC = "test-topic";
    process.env.EVENT_SUBSCRIPTION = "test-sub";
    delete process.env.AZURE_SERVICEBUS_CONNECTION_STRING;
    delete process.env.EVENT_NAMESPACE;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
      await fastify.ready();
      await expect(
        CreateEventConsumer(fastify, "azure-servicebus"),
      ).rejects.toThrow("AZURE_SERVICEBUS_CONNECTION_STRING");
      await fastify.close();
    } finally {
      if (origTopic !== undefined) process.env.EVENT_TOPIC = origTopic;
      else delete process.env.EVENT_TOPIC;
      if (origSub !== undefined) process.env.EVENT_SUBSCRIPTION = origSub;
      else delete process.env.EVENT_SUBSCRIPTION;
      if (origConn !== undefined)
        process.env.AZURE_SERVICEBUS_CONNECTION_STRING = origConn;
      if (origNs !== undefined) process.env.EVENT_NAMESPACE = origNs;
    }
  });

  it("GCP consumer should throw without EVENT_SUBSCRIPTION", async () => {
    const origSub = process.env.EVENT_SUBSCRIPTION;
    const origApp = process.env.APP;
    const origTopic = process.env.EVENT_TOPIC;
    delete process.env.EVENT_SUBSCRIPTION;
    delete process.env.APP;
    delete process.env.EVENT_TOPIC;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
      await fastify.ready();
      await expect(CreateEventConsumer(fastify, "gcp-pubsub")).rejects.toThrow(
        "EVENT_SUBSCRIPTION",
      );
      await fastify.close();
    } finally {
      if (origSub !== undefined) process.env.EVENT_SUBSCRIPTION = origSub;
      if (origApp !== undefined) process.env.APP = origApp;
      if (origTopic !== undefined) process.env.EVENT_TOPIC = origTopic;
    }
  });

  it("RabbitMQ consumer should throw without RABBITMQ_URL", async () => {
    const origUrl = process.env.RABBITMQ_URL;
    const origK = process.env.K_SERVICE;
    delete process.env.RABBITMQ_URL;
    process.env.K_SERVICE = "test-service";
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOpts("in-process", [
          { file: "test.ts", handlers: { x: jest.fn() } },
        ]),
      );
      // Manually set _hasEventHandlers to bypass the skip check
      (fastify as any)._hasEventHandlers = true;
      await fastify.ready();
      await expect(CreateEventConsumer(fastify, "rabbitmq")).rejects.toThrow(
        "RABBITMQ_URL",
      );
      await fastify.close();
    } finally {
      if (origUrl !== undefined) process.env.RABBITMQ_URL = origUrl;
      else delete process.env.RABBITMQ_URL;
      if (origK !== undefined) process.env.K_SERVICE = origK;
      else delete process.env.K_SERVICE;
    }
  });

  it("RabbitMQ consumer should throw without K_SERVICE", async () => {
    const origUrl = process.env.RABBITMQ_URL;
    const origK = process.env.K_SERVICE;
    process.env.RABBITMQ_URL = "amqp://localhost";
    delete process.env.K_SERVICE;
    try {
      const fastify = Fastify({ logger: false });
      await fastify.register(
        Plugins.EventBus,
        makeOpts("in-process", [
          { file: "test.ts", handlers: { x: jest.fn() } },
        ]),
      );
      (fastify as any)._hasEventHandlers = true;
      await fastify.ready();
      await expect(CreateEventConsumer(fastify, "rabbitmq")).rejects.toThrow(
        "K_SERVICE",
      );
      await fastify.close();
    } finally {
      if (origUrl !== undefined) process.env.RABBITMQ_URL = origUrl;
      else delete process.env.RABBITMQ_URL;
      if (origK !== undefined) process.env.K_SERVICE = origK;
    }
  });

  it("RabbitMQ consumer should skip when _hasEventHandlers is false", async () => {
    const fastify = Fastify({ logger: false });
    await fastify.register(Plugins.EventBus, makeOpts("in-process", []));
    await fastify.ready();
    (fastify as any)._hasEventHandlers = false;

    // Should NOT throw (no RABBITMQ_URL needed — skips entirely)
    const consumer = await CreateEventConsumer(fastify, "rabbitmq");
    await consumer.close();
    await fastify.close();
  });
});

// ---------------------------------------------------------------------------
// RabbitMQ DLQ max-retry path
// ---------------------------------------------------------------------------

describe("RabbitMQ DLQ max-retry", () => {
  let rabbitmqUrl: string;

  beforeAll(async () => {
    const port = 25_672 + SUFFIX;
    const name = `${CONTAINER_PREFIX}-rabbitmq-${SUFFIX}`;
    // Reuse existing container if already running from the RabbitMQ suite
    try {
      const running = execFileSync(
        "docker",
        ["inspect", "-f", "{{.State.Running}}", name],
        { encoding: "utf8", timeout: 5000 },
      ).trim();
      if (running === "true") {
        await waitForPort(port);
        rabbitmqUrl = `amqp://guest:guest@127.0.0.1:${port}`;
        return;
      }
    } catch {
      /* container doesn't exist */
    }

    dockerRun(name, "rabbitmq:4-management", {
      ports: [[port, 5672]],
    });
    await waitForPort(port);
    rabbitmqUrl = `amqp://guest:guest@127.0.0.1:${port}`;
    // Probe with actual AMQP connection until the broker accepts
    const readyStart = Date.now();
    while (Date.now() - readyStart < 30_000) {
      try {
        const probe = new Connection(rabbitmqUrl);
        await new Promise<void>((resolve, reject) => {
          probe.on("connection", () => {
            probe.close().then(resolve, resolve);
          });
          probe.on("error", reject);
        });
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }, 60_000);

  it("should route to DLQ after exceeding max retry count", async () => {
    setEnv("RABBITMQ_URL", rabbitmqUrl);
    setEnv("K_SERVICE", "dlq-max-retry-svc");

    let callCount = 0;
    const handler: EventHandler = jest.fn(async () => {
      callCount++;
      // Always fail — forces continuous retries via DLX
      throw new Error("permanent failure");
    });

    const fastify = Fastify({ logger: false });
    await fastify.register(
      Plugins.EventBus,
      makeOpts(
        "rabbitmq",
        [{ file: "dlq.ts", handlers: { "dlq.event": handler } }],
        { ensureExchangesAndQueues: true },
      ),
    );
    await fastify.ready();

    const consumer = await CreateEventConsumer(fastify, "rabbitmq");

    try {
      fastify.EventBus.publish("dlq.event", { permanent_fail: true });
      // DLX retry cycles are 5s TTL each. After MAX_RETRY_COUNT (10) retries
      // the message should be moved to DLQ and stop being redelivered.
      await waitFor(() => callCount >= 5, 120_000, 1000);

      expect(callCount).toBeGreaterThanOrEqual(5);
      // Verify it eventually stopped retrying (no infinite loop)
      const countAtCheck = callCount;
      await new Promise((r) => setTimeout(r, 10_000));
      // May get 1-2 more due to in-flight, but shouldn't keep growing
      expect(callCount).toBeLessThanOrEqual(countAtCheck + 2);
    } finally {
      await consumer.close();
      await fastify.close();
      restoreEnv();
    }
  }, 180_000);
});
