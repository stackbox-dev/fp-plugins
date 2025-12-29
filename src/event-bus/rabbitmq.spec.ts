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
});
