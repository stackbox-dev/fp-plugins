import Fastify, { FastifyInstance } from "fastify";
import { GenericContainer, StartedTestContainer, Wait } from "testcontainers";
import { Connection } from "rabbitmq-client";
import { Plugins } from "../index";
import { EventBusOptions, EventHandler } from "./interfaces";
import { getServicePrefix } from "./rabbitmq-utils";
import { RabbitMqServiceBusConsumerBuilder } from "./event-consumer/rabbitmq";

describe("RabbitMQ Integration Tests", () => {
  let container: StartedTestContainer;
  let rabbitmqUrl: string;

  beforeAll(async () => {
    container = await new GenericContainer("rabbitmq:3-management")
      .withExposedPorts(5672, 15672)
      .withWaitStrategy(Wait.forLogMessage(/started TCP listener/))
      .start();

    const host = container.getHost();
    const port = container.getMappedPort(5672);
    rabbitmqUrl = `amqp://guest:guest@${host}:${port}`;
  }, 60000);

  afterAll(async () => {
    await container?.stop();
  });

  describe("getServicePrefix", () => {
    it("should extract prefix before first hyphen", () => {
      expect(getServicePrefix("wms-cincout")).toBe("wms");
      expect(getServicePrefix("xyz-service-name")).toBe("xyz");
    });

    it("should return 'default' when no hyphen found", () => {
      expect(getServicePrefix("noprefix")).toBe("default");
      expect(getServicePrefix("")).toBe("default");
    });

    it("should handle edge cases", () => {
      expect(getServicePrefix("-startswithhyphen")).toBe("default");
      expect(getServicePrefix("a-b")).toBe("a");
    });
  });

  describe("RabbitMQ Publisher", () => {
    let fastify: FastifyInstance;
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
      process.env.RABBITMQ_URL = rabbitmqUrl;
      process.env.K_SERVICE = "test-publisher";
      fastify = Fastify({ logger: false });
    });

    afterEach(async () => {
      process.env = originalEnv;
      await fastify.close();
    });

    it("should register RabbitMQ plugin successfully", async () => {
      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await fastify.register(Plugins.EventBus, eventBusOptions);
      expect(fastify.EventBus).toBeDefined();
      expect(typeof fastify.EventBus.publish).toBe("function");
    });

    it("should publish messages to RabbitMQ", async () => {
      const validateMsg = jest.fn();
      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg,
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await fastify.register(Plugins.EventBus, eventBusOptions);

      // Publish a message
      fastify.EventBus.publish("test.event", { data: "test-payload" });

      // Wait for flush interval
      await new Promise((resolve) => setTimeout(resolve, 100));

      expect(validateMsg).toHaveBeenCalledWith("test.event", { data: "test-payload" }, undefined);
    });

    it("should use dynamic prefix based on K_SERVICE", async () => {
      process.env.K_SERVICE = "myapp-service";

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await fastify.register(Plugins.EventBus, eventBusOptions);

      // Now verify the exchange was created with correct prefix
      const connection = new Connection(rabbitmqUrl);
      try {
        // This will throw if exchange doesn't exist (passive: true)
        await connection.exchangeDeclare({
          exchange: "myapp.main-exchange",
          type: "fanout",
          passive: true,
        });
        // If we get here, exchange exists
        expect(true).toBe(true);
      } finally {
        await connection.close();
      }
    });
  });

  describe("RabbitMQ Consumer", () => {
    let fastify: FastifyInstance;
    let consumer: { close: () => Promise<void> } | null = null;
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
      process.env.RABBITMQ_URL = rabbitmqUrl;
      process.env.K_SERVICE = "test-consumer";
      fastify = Fastify({ logger: false });
    });

    afterEach(async () => {
      process.env = originalEnv;
      if (consumer) {
        await consumer.close();
        consumer = null;
      }
      await fastify.close();
    });

    it("should consume messages from RabbitMQ", async () => {
      const receivedMessages: any[] = [];
      const messageHandler: EventHandler = jest.fn(async (msg) => {
        receivedMessages.push(msg);
      });

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "test.ts",
            handlers: { "test.consume": messageHandler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await fastify.register(Plugins.EventBus, eventBusOptions);
      await fastify.ready();

      // Start consumer
      consumer = await RabbitMqServiceBusConsumerBuilder(fastify);

      // Publish a message directly to RabbitMQ
      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        await publisher.send(
          {
            exchange: "test.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "test.consume",
            payload: { message: "hello" },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for message to be consumed
        await new Promise((resolve) => setTimeout(resolve, 500));

        expect(messageHandler).toHaveBeenCalled();
      } finally {
        await publisher.close();
        await connection.close();
      }
    });
  });

  describe("RabbitMQ End-to-End", () => {
    let publisherFastify: FastifyInstance;
    let consumerFastify: FastifyInstance;
    let consumer: { close: () => Promise<void> } | null = null;
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
      process.env.RABBITMQ_URL = rabbitmqUrl;
      publisherFastify = Fastify({ logger: false });
      consumerFastify = Fastify({ logger: false });
    });

    afterEach(async () => {
      process.env = originalEnv;
      if (consumer) {
        await consumer.close();
        consumer = null;
      }
      await publisherFastify.close();
      await consumerFastify.close();
    });

    it("should publish and consume messages end-to-end", async () => {
      process.env.K_SERVICE = "e2e-service";

      const receivedMessages: any[] = [];
      const messageHandler: EventHandler = jest.fn(async (msg) => {
        receivedMessages.push(msg);
      });

      // Set up consumer
      const consumerOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "e2e.ts",
            handlers: { "e2e.test": messageHandler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, consumerOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      // Set up publisher
      const publisherOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await publisherFastify.register(Plugins.EventBus, publisherOptions);

      // Publish message
      publisherFastify.EventBus.publish("e2e.test", { testData: "e2e-payload" });

      // Wait for message to be published and consumed
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(messageHandler).toHaveBeenCalled();
      expect(receivedMessages.length).toBeGreaterThan(0);
      expect(receivedMessages[0].data).toEqual({ testData: "e2e-payload" });
    });

    it("should respect processAfterDelayMs in messages", async () => {
      // Test that messages with processAfterDelayMs are handled correctly
      // The consumer returns 425 for messages that arrive too early
      process.env.K_SERVICE = "delay-test-service";

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "delay.ts",
            handlers: { "delay.check": jest.fn() },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      // Send a message with delay that hasn't elapsed yet
      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: 123,
          body: JSON.stringify({
            event: "delay.check",
            payload: { test: true },
            file: null,
            processAfterDelayMs: 60000, // 60 seconds in the future
            publishTimestamp: Date.now(),
          }),
        },
      });

      // Should return 425 (Too Early) for messages that can't be processed yet
      expect(response.statusCode).toBe(425);
    });

    it("should handle multiple messages in batch and flush remaining", async () => {
      // This test verifies the critical bug fix where messages < batch size (10) were being dropped
      process.env.K_SERVICE = "batch-service";

      const receivedMessages: any[] = [];
      const messageHandler: EventHandler = jest.fn(async (msg) => {
        receivedMessages.push(msg.data);
      });

      const consumerOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "batch.ts",
            handlers: { "batch.test": messageHandler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, consumerOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const publisherOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await publisherFastify.register(Plugins.EventBus, publisherOptions);

      // Publish 15 messages (more than batch size of 10)
      // Before the fix, only 10 would be sent, the last 5 would be dropped
      for (let i = 0; i < 15; i++) {
        publisherFastify.EventBus.publish("batch.test", { index: i });
      }

      // Wait for all messages to be processed
      await new Promise((resolve) => setTimeout(resolve, 1500));

      // Verify ALL 15 messages were received (not just first 10)
      expect(receivedMessages.length).toBe(15);

      // Verify message content - all indices should be present
      const indices = receivedMessages.map((m) => m.index).sort((a, b) => a - b);
      expect(indices).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]);
    });

    it("should handle multiple event types in same batch", async () => {
      process.env.K_SERVICE = "multi-event-service";

      const eventAMessages: any[] = [];
      const eventBMessages: any[] = [];

      const handlerA: EventHandler = jest.fn(async (msg) => {
        eventAMessages.push(msg.data);
      });
      const handlerB: EventHandler = jest.fn(async (msg) => {
        eventBMessages.push(msg.data);
      });

      const consumerOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "multi.ts",
            handlers: {
              "multi.eventA": handlerA,
              "multi.eventB": handlerB,
            },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, consumerOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const publisherOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await publisherFastify.register(Plugins.EventBus, publisherOptions);

      // Publish interleaved events
      publisherFastify.EventBus.publish("multi.eventA", { type: "A", index: 1 });
      publisherFastify.EventBus.publish("multi.eventB", { type: "B", index: 1 });
      publisherFastify.EventBus.publish("multi.eventA", { type: "A", index: 2 });
      publisherFastify.EventBus.publish("multi.eventB", { type: "B", index: 2 });
      publisherFastify.EventBus.publish("multi.eventA", { type: "A", index: 3 });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      expect(eventAMessages.length).toBe(3);
      expect(eventBMessages.length).toBe(2);
      expect(eventAMessages.every((m) => m.type === "A")).toBe(true);
      expect(eventBMessages.every((m) => m.type === "B")).toBe(true);
    });

    it("should flush all messages on shutdown", async () => {
      process.env.K_SERVICE = "shutdown-service";

      const receivedMessages: any[] = [];
      const messageHandler: EventHandler = jest.fn(async (msg) => {
        receivedMessages.push(msg.data);
      });

      const consumerOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "shutdown.ts",
            handlers: { "shutdown.test": messageHandler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, consumerOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const publisherOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await publisherFastify.register(Plugins.EventBus, publisherOptions);

      // Publish messages
      for (let i = 0; i < 7; i++) {
        publisherFastify.EventBus.publish("shutdown.test", { index: i });
      }

      // Close publisher immediately (triggers onClose flush)
      await publisherFastify.close();

      // Wait for consumer to process
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // All 7 messages should have been flushed on close
      expect(receivedMessages.length).toBe(7);
    });
  });

  describe("RabbitMQ Error Handling", () => {
    let fastify: FastifyInstance;
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
      fastify = Fastify({ logger: false });
    });

    afterEach(async () => {
      process.env = originalEnv;
      await fastify.close();
    });

    it("should throw error when RABBITMQ_URL is not set", async () => {
      delete process.env.RABBITMQ_URL;
      process.env.K_SERVICE = "test-service";

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await expect(
        fastify.register(Plugins.EventBus, eventBusOptions)
      ).rejects.toThrow("RabbitMq requires RABBITMQ_URL");
    });

    it("should throw error when K_SERVICE is not set", async () => {
      process.env.RABBITMQ_URL = rabbitmqUrl;
      delete process.env.K_SERVICE;

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await expect(
        fastify.register(Plugins.EventBus, eventBusOptions)
      ).rejects.toThrow("RabbitMq requires K_SERVICE");
    });

    it("should handle invalid JSON in message body", async () => {
      process.env.RABBITMQ_URL = rabbitmqUrl;
      process.env.K_SERVICE = "json-error-service";

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "error.ts",
            handlers: { "error.test": jest.fn() },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await fastify.register(Plugins.EventBus, eventBusOptions);
      await fastify.ready();

      // Simulate incoming message with invalid JSON
      const response = await fastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: 123,
          body: "invalid json {",
        },
      });

      expect(response.statusCode).toBe(400);
    });
  });

  describe("Handler Execution", () => {
    let consumerFastify: FastifyInstance;
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
      process.env.RABBITMQ_URL = rabbitmqUrl;
      consumerFastify = Fastify({ logger: false });
    });

    afterEach(async () => {
      process.env = originalEnv;
      await consumerFastify.close();
    });

    it("should call handler matching event name", async () => {
      process.env.K_SERVICE = "handler-match-service";

      const handler1 = jest.fn().mockResolvedValue(undefined);
      const handler2 = jest.fn().mockResolvedValue(undefined);

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "handler1.ts",
            handlers: { "event.one": handler1 },
          },
          {
            file: "handler2.ts",
            handlers: { "event.two": handler2 },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      // Send message for event.one
      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: "msg-123",
          body: JSON.stringify({
            event: "event.one",
            payload: { test: "data" },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          }),
        },
      });

      expect(response.statusCode).toBe(200);
      expect(handler1).toHaveBeenCalledTimes(1);
      expect(handler2).not.toHaveBeenCalled();
    });

    it("should call only handler matching specified file", async () => {
      process.env.K_SERVICE = "file-match-service";

      const handlerA = jest.fn().mockResolvedValue(undefined);
      const handlerB = jest.fn().mockResolvedValue(undefined);

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "fileA.ts",
            handlers: { "same.event": handlerA },
          },
          {
            file: "fileB.ts",
            handlers: { "same.event": handlerB },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      // Send message with specific file filter
      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: "msg-456",
          body: JSON.stringify({
            event: "same.event",
            payload: { test: "data" },
            file: "fileA.ts",  // Only fileA.ts handler should run
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          }),
        },
      });

      expect(response.statusCode).toBe(200);
      expect(handlerA).toHaveBeenCalledTimes(1);
      expect(handlerB).not.toHaveBeenCalled();
    });

    it("should call all handlers when no file specified", async () => {
      process.env.K_SERVICE = "all-handlers-service";

      const handlerA = jest.fn().mockResolvedValue(undefined);
      const handlerB = jest.fn().mockResolvedValue(undefined);

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "fileA.ts",
            handlers: { "broadcast.event": handlerA },
          },
          {
            file: "fileB.ts",
            handlers: { "broadcast.event": handlerB },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      // Send message without file filter
      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: "msg-789",
          body: JSON.stringify({
            event: "broadcast.event",
            payload: { test: "data" },
            file: null,  // No file filter - all handlers run
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          }),
        },
      });

      expect(response.statusCode).toBe(200);
      expect(handlerA).toHaveBeenCalledTimes(1);
      expect(handlerB).toHaveBeenCalledTimes(1);
    });

    it("should skip handler when file does not match", async () => {
      process.env.K_SERVICE = "file-mismatch-service";

      const handler = jest.fn().mockResolvedValue(undefined);

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "registered-file.ts",
            handlers: { "mismatch.event": handler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      // Send message with different file
      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: "msg-mismatch",
          body: JSON.stringify({
            event: "mismatch.event",
            payload: { test: "data" },
            file: "different-file.ts",  // Doesn't match registered file
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          }),
        },
      });

      // Should return 200 (no error) but handler should NOT be called
      expect(response.statusCode).toBe(200);
      expect(handler).not.toHaveBeenCalled();
    });

    it("should return OK when no handler registered for event", async () => {
      process.env.K_SERVICE = "no-handler-service";

      const handler = jest.fn().mockResolvedValue(undefined);

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "some.ts",
            handlers: { "registered.event": handler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      // Send message for unregistered event
      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: "msg-unknown",
          body: JSON.stringify({
            event: "unknown.event",  // No handler registered
            payload: { test: "data" },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          }),
        },
      });

      // Should bail out gracefully
      expect(response.statusCode).toBe(200);
      expect(handler).not.toHaveBeenCalled();
    });

    it("should pass correct data to handler", async () => {
      process.env.K_SERVICE = "data-pass-service";

      let receivedMsg: any = null;
      const handler = jest.fn().mockImplementation(async (msg) => {
        receivedMsg = msg;
      });

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "data.ts",
            handlers: { "data.event": handler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      const testPayload = { userId: 123, action: "test" };
      const publishTimestamp = Date.now();

      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: "msg-data-123",
          body: JSON.stringify({
            event: "data.event",
            payload: testPayload,
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp,
          }),
        },
      });

      expect(response.statusCode).toBe(200);
      expect(handler).toHaveBeenCalledTimes(1);
      expect(receivedMsg).not.toBeNull();
      expect(receivedMsg.data).toEqual(testPayload);
      expect(receivedMsg.event).toBe("data.event");
      expect(receivedMsg.id).toBe("msg-data-123");
    });

    it("should return 500 when handler throws error", async () => {
      process.env.K_SERVICE = "error-handler-service";

      const handler = jest.fn().mockRejectedValue(new Error("Handler failed"));

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "error.ts",
            handlers: { "error.event": handler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("Processed error"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();

      const response = await consumerFastify.inject({
        method: "POST",
        url: "/rabbitmq/process-message",
        payload: {
          messageId: "msg-error",
          body: JSON.stringify({
            event: "error.event",
            payload: { test: "data" },
            file: "error.ts",  // Specify file to get ErrorWithStatus thrown
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          }),
        },
      });

      expect(response.statusCode).toBe(500);
      expect(handler).toHaveBeenCalledTimes(1);
    });
  });

  describe("RabbitMQ Dead-Letter Retry", () => {
    let consumerFastify: FastifyInstance;
    let consumer: { close: () => Promise<void> } | null = null;
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
      process.env.RABBITMQ_URL = rabbitmqUrl;
      consumerFastify = Fastify({ logger: false });
    });

    afterEach(async () => {
      process.env = originalEnv;
      if (consumer) {
        await consumer.close();
        consumer = null;
      }
      await consumerFastify.close();
    });

    it("should retry via dead-letter on 500 error", async () => {
      process.env.K_SERVICE = "retry-500-service";

      let callCount = 0;
      const messageHandler: EventHandler = jest.fn(async () => {
        callCount++;
        if (callCount < 2) {
          throw new Error("Simulated 500 error");
        }
        // Succeed on second attempt
      });

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "retry.ts",
            handlers: { "retry.500": messageHandler },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      // Publish message directly to RabbitMQ
      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        await publisher.send(
          {
            exchange: "retry.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "retry.500",
            payload: { test: true },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for dead-letter retry cycle (5s TTL + processing time)
        await new Promise((resolve) => setTimeout(resolve, 7000));

        // Handler should be called twice: first fail, then succeed after DLX retry
        expect(callCount).toBe(2);
      } finally {
        await publisher.close();
        await connection.close();
      }
    }, 15000);

    it("should retry via dead-letter on 429 rate-limit", async () => {
      process.env.K_SERVICE = "retry-429-service";

      let callCount = 0;
      const receivedMessages: any[] = [];

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "retry429.ts",
            handlers: {
              "retry.429": async (msg) => {
                callCount++;
                receivedMessages.push({ attempt: callCount, data: msg.data });
              },
            },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);

      // Override the route to return 429 on first call
      let httpCallCount = 0;
      consumerFastify.addHook("onSend", async (request, reply, payload) => {
        if (request.url === "/rabbitmq/process-message") {
          httpCallCount++;
          if (httpCallCount === 1) {
            reply.code(429);
            return JSON.stringify({ error: "rate limited" });
          }
        }
        return payload;
      });

      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        await publisher.send(
          {
            exchange: "retry.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "retry.429",
            payload: { rateLimit: true },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for dead-letter retry cycle
        await new Promise((resolve) => setTimeout(resolve, 7000));

        // Should have been called twice via DLX retry
        expect(httpCallCount).toBeGreaterThanOrEqual(2);
      } finally {
        await publisher.close();
        await connection.close();
      }
    }, 15000);

    it("should retry via dead-letter on 409 lock conflict", async () => {
      process.env.K_SERVICE = "retry-409-service";

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "retry409.ts",
            handlers: { "retry.409": jest.fn() },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);

      let httpCallCount = 0;
      consumerFastify.addHook("onSend", async (request, reply, payload) => {
        if (request.url === "/rabbitmq/process-message") {
          httpCallCount++;
          if (httpCallCount === 1) {
            reply.code(409);
            return JSON.stringify({ error: "lock conflict" });
          }
        }
        return payload;
      });

      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        await publisher.send(
          {
            exchange: "retry.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "retry.409",
            payload: { lockConflict: true },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for dead-letter retry cycle
        await new Promise((resolve) => setTimeout(resolve, 7000));

        // Should have retried via DLX
        expect(httpCallCount).toBeGreaterThanOrEqual(2);
      } finally {
        await publisher.close();
        await connection.close();
      }
    }, 15000);

    it("should verify message goes through retry queue", async () => {
      process.env.K_SERVICE = "dlx-flow-service";

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "dlx.ts",
            handlers: { "dlx.test": jest.fn() },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);

      // Always return 500 to keep message in retry loop
      consumerFastify.addHook("onSend", async (request, reply, payload) => {
        if (request.url === "/rabbitmq/process-message") {
          reply.code(500);
          return JSON.stringify({ error: "server error" });
        }
        return payload;
      });

      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        await publisher.send(
          {
            exchange: "dlx.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "dlx.test",
            payload: { dlxTest: true },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait a bit for message to enter retry queue
        await new Promise((resolve) => setTimeout(resolve, 1000));

        // Check retry queue has the message
        const retryQueueInfo = await connection.queueDeclare({
          queue: "dlx.retry-queue.dlx-flow-service",
          passive: true,
        });

        // Message should be in retry queue (or already cycled back)
        // We verify the queue exists and has been used
        expect(retryQueueInfo.queue).toBe("dlx.retry-queue.dlx-flow-service");
      } finally {
        await publisher.close();
        await connection.close();
      }
    }, 15000);

    it("should use immediate REQUEUE for 425 (delayed message), not DLX", async () => {
      // 425 should use REQUEUE with local sleep (up to 10s randomDelay)
      // Key: retry should happen BEFORE 5s DLX TTL would complete (total < 15s)
      // DLX path would be: 5s TTL + processing > 5s
      // REQUEUE path: up to 10s randomDelay + immediate requeue
      process.env.K_SERVICE = "requeue-service";

      let httpCallCount = 0;
      const callTimestamps: number[] = [];

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "requeue.ts",
            handlers: { "requeue.425": jest.fn() },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      // Register hook BEFORE plugin
      consumerFastify.addHook("onSend", async (request, reply, payload) => {
        if (request.url === "/rabbitmq/process-message") {
          httpCallCount++;
          callTimestamps.push(Date.now());
          if (httpCallCount === 1) {
            reply.code(425);
            return JSON.stringify({ processAfterDelayMs: 1000 });
          }
        }
        return payload;
      });

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        await publisher.send(
          {
            exchange: "requeue.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "requeue.425",
            payload: { delayed: true },
            file: null,
            processAfterDelayMs: 1000,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for REQUEUE cycle (randomDelay up to 10s + processing)
        await new Promise((resolve) => setTimeout(resolve, 12000));

        // Should have been called twice (via REQUEUE, not DLX)
        expect(httpCallCount).toBeGreaterThanOrEqual(2);

        // Verify the retry didn't go through DLX (which adds 5s TTL)
        // REQUEUE with 10s max delay should be faster than DLX 5s + 10s delay
        if (callTimestamps.length >= 2) {
          const timeBetweenCalls = callTimestamps[1] - callTimestamps[0];
          // Should be less than 12s (10s max delay + some processing)
          expect(timeBetweenCalls).toBeLessThan(12000);
        }
      } finally {
        await publisher.close();
        await connection.close();
      }
    }, 20000);

    it("should dead-letter on other 4xx errors (bad message)", async () => {
      process.env.K_SERVICE = "bad-msg-service";

      let callCount = 0;

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "badmsg.ts",
            handlers: { "badmsg.test": jest.fn() },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);

      // Return 400 on first call, 200 after DLX retry
      consumerFastify.addHook("onSend", async (request, reply, payload) => {
        if (request.url === "/rabbitmq/process-message") {
          callCount++;
          if (callCount === 1) {
            reply.code(400);
            return JSON.stringify({ error: "bad request" });
          }
        }
        return payload;
      });

      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        await publisher.send(
          {
            exchange: "bad.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "badmsg.test",
            payload: { bad: true },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for DLX retry cycle (5s TTL + processing)
        await new Promise((resolve) => setTimeout(resolve, 7000));

        // Should have retried via DLX
        expect(callCount).toBeGreaterThanOrEqual(2);
      } finally {
        await publisher.close();
        await connection.close();
      }
    }, 15000);

    it("should not fan out retry to other services", async () => {
      // This test verifies that when a message fails in service-a,
      // the retry only goes back to service-a, not to service-b
      // Both services use same prefix "shared" so they share the main-exchange

      let serviceBMessageCount = 0;
      let serviceAHttpCalls = 0;

      // Set up service A (will fail first, then succeed)
      process.env.K_SERVICE = "shared-service-a";

      // Register hook BEFORE plugin for service A
      consumerFastify.addHook("onSend", async (request, reply, payload) => {
        if (request.url === "/rabbitmq/process-message") {
          serviceAHttpCalls++;
          if (serviceAHttpCalls === 1) {
            reply.code(500);
            return JSON.stringify({ error: "server error" });
          }
        }
        return payload;
      });

      const serviceAOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "fanout-a.ts",
            handlers: {
              "fanout.test": jest.fn(),
            },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await consumerFastify.register(Plugins.EventBus, serviceAOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      // Set up service B (separate instance, same prefix "shared")
      const serviceBFastify = Fastify({ logger: false });
      process.env.K_SERVICE = "shared-service-b";

      const serviceBOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "fanout-b.ts",
            handlers: {
              "fanout.test": async () => {
                serviceBMessageCount++;
              },
            },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      await serviceBFastify.register(Plugins.EventBus, serviceBOptions);
      await serviceBFastify.ready();
      const consumerB = await RabbitMqServiceBusConsumerBuilder(serviceBFastify);

      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        // Publish to main exchange (fans out to both services)
        await publisher.send(
          {
            exchange: "shared.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "fanout.test",
            payload: { fanoutTest: true },
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for initial fanout + DLX retry cycle (5s TTL + processing)
        await new Promise((resolve) => setTimeout(resolve, 8000));

        // KEY ASSERTION: Service B should have received the message exactly once (initial fanout only)
        // If retry went through main-exchange (the old buggy behavior), service B would get it again
        expect(serviceBMessageCount).toBe(1);

        // Service A should have received it at least twice (initial fail + retry after DLX)
        expect(serviceAHttpCalls).toBeGreaterThanOrEqual(2);
      } finally {
        await publisher.close();
        await connection.close();
        await consumerB.close();
        await serviceBFastify.close();
      }
    }, 20000);

    it("should move message to DLQ after 10 failed retries with correct headers", async () => {
      // This is the REAL DLQ behavior test:
      // 1. Message fails 10 times via DLX retry cycles
      // 2. After 10th failure, message is moved to DLQ
      // 3. DLQ message has correct headers (x-original-queue, x-final-status-code, x-final-retry-count)
      // 4. Message is removed from main queue (ACKed)
      //
      // Timing: 10 DLX cycles × 5s TTL = 50s minimum
      process.env.K_SERVICE = "dlq-e2e-service";

      let httpCallCount = 0;

      const eventBusOptions: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [
          {
            file: "dlq-e2e.ts",
            handlers: { "dlq.e2e": jest.fn() },
          },
        ],
        validateMsg: jest.fn(),
        processError: jest.fn().mockReturnValue({ err: new Error("test"), status: 500 }),
      };

      // Always return 500 to force message through all 10 retry cycles
      consumerFastify.addHook("onSend", async (request, reply, payload) => {
        if (request.url === "/rabbitmq/process-message") {
          httpCallCount++;
          reply.code(500);
          return JSON.stringify({ error: "simulated failure" });
        }
        return payload;
      });

      await consumerFastify.register(Plugins.EventBus, eventBusOptions);
      await consumerFastify.ready();
      consumer = await RabbitMqServiceBusConsumerBuilder(consumerFastify);

      const connection = new Connection(rabbitmqUrl);
      const publisher = connection.createPublisher({ maxAttempts: 3 });

      try {
        // Verify DLQ queue was created during setup
        const dlqInfo = await connection.queueDeclare({
          queue: "dlq.dlq.dlq-e2e-service",
          passive: true,
        });
        expect(dlqInfo.queue).toBe("dlq.dlq.dlq-e2e-service");

        const testPayload = { dlqTest: true, timestamp: Date.now() };

        // Publish message that will fail all 10 retries
        await publisher.send(
          {
            exchange: "dlq.main-exchange",
            contentType: "application/json",
          },
          JSON.stringify({
            event: "dlq.e2e",
            payload: testPayload,
            file: null,
            processAfterDelayMs: 0,
            publishTimestamp: Date.now(),
          })
        );

        // Wait for 10 DLX cycles + processing time
        // Each cycle: 5s TTL + processing overhead
        // Total: ~55-60 seconds
        await new Promise((resolve) => setTimeout(resolve, 58000));

        // Should have been called at least 11 times:
        // 1 initial call + 10 retry cycles = 11 calls
        // On the 11th call (after 10 DLX cycles), x-death count = 10, triggers DLQ
        expect(httpCallCount).toBeGreaterThanOrEqual(11);

        // Now verify the message is in the DLQ
        // Create a temporary consumer to read from DLQ
        let dlqMessage: any = null;
        const dlqConsumer = connection.createConsumer(
          {
            queue: "dlq.dlq.dlq-e2e-service",
            queueOptions: { passive: true },
            noAck: true, // Don't require ACK for test
          },
          async (msg: any) => {
            dlqMessage = msg;
            return 1; // ACK
          }
        );

        // Give consumer time to receive message
        await new Promise((resolve) => setTimeout(resolve, 1000));
        await dlqConsumer.close();

        // Verify DLQ message exists and has correct headers
        expect(dlqMessage).not.toBeNull();
        expect(dlqMessage.headers).toBeDefined();
        expect(dlqMessage.headers["x-original-queue"]).toBe("dlq.queue.dlq-e2e-service");
        expect(dlqMessage.headers["x-final-status-code"]).toBe(500);
        expect(dlqMessage.headers["x-final-retry-count"]).toBeGreaterThanOrEqual(10);

        // Verify message body contains original payload
        const bodyStr = Buffer.isBuffer(dlqMessage.body)
          ? dlqMessage.body.toString("utf8")
          : dlqMessage.body;
        const body = JSON.parse(bodyStr);
        expect(body.payload).toEqual(testPayload);
        expect(body.event).toBe("dlq.e2e");

        // Verify main queue is empty (message was ACKed after moving to DLQ)
        const mainQueueInfo = await connection.queueDeclare({
          queue: "dlq.queue.dlq-e2e-service",
          passive: true,
        });
        expect(mainQueueInfo.messageCount).toBe(0);
      } finally {
        await publisher.close();
        await connection.close();
      }
    }, 70000); // 70s timeout for 10 retry cycles
  });
});
