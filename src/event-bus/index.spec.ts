import Fastify from "fastify";
import EventBusPlugin from "./index";
import { EventBusOptions, EventHandler } from "./interfaces";

describe("EventBus Plugin", () => {
  let fastify: ReturnType<typeof Fastify>;
  let mockValidateMsg: jest.Mock;
  let mockProcessError: jest.Mock;
  let mockHandler: EventHandler;

  beforeEach(() => {
    fastify = Fastify({ logger: false });
    mockValidateMsg = jest.fn();
    mockProcessError = jest.fn().mockReturnValue({ err: new Error("test"), status: 500 });
    mockHandler = jest.fn().mockResolvedValue(undefined);
  });

  afterEach(async () => {
    try {
      await fastify.close();
    } catch (error) {
      // Ignore errors from fastify already being closed
      if (!error.message.includes('already been closed')) {
        throw error;
      }
    }
  });

  describe("Plugin Registration", () => {
    it("should register with in-process bus type (default)", async () => {
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
      expect(fastify.EventBus).toBeDefined();
    });

    it("should require proper environment variables for rabbitmq", async () => {
      // Test that rabbitmq registration fails without required env vars
      const options: EventBusOptions = {
        busType: "rabbitmq",
        handlers: [],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await expect(fastify.register(EventBusPlugin, options)).rejects.toThrow();
    });

    it("should register with gcp-pubsub bus type", async () => {
      process.env.EVENT_TOPIC = "test-topic";
      
      const options: EventBusOptions = {
        busType: "gcp-pubsub",
        topic: "test-topic",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
      expect(fastify.EventBus).toBeDefined();
      
      delete process.env.EVENT_TOPIC;
    });

    it("should register with azure-servicebus bus type", async () => {
      process.env.EVENT_TOPIC = "test-topic";
      
      const options: EventBusOptions = {
        busType: "azure-servicebus",
        namespace: "test-namespace",
        topic: "test-topic",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
      expect(fastify.EventBus).toBeDefined();
      
      delete process.env.EVENT_TOPIC;
    });

    it("should throw error for azure-servicebus without namespace", async () => {
      const options: EventBusOptions = {
        busType: "azure-servicebus",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await expect(fastify.register(EventBusPlugin, options)).rejects.toThrow(
        "Azure ServiceBus needs the namespace specified"
      );
    });
  });

  describe("Event Publish Route", () => {
    beforeEach(async () => {
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
    });

    it("should register publish route by default", async () => {
      const response = await fastify.inject({
        method: "POST",
        url: "/event-bus/publish/testEvent",
        payload: { test: "data" }
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("OK");
    });

    it("should handle string payload via query param", async () => {
      const response = await fastify.inject({
        method: "POST",
        url: "/event-bus/publish/testEvent?stringPayload=test-string",
        payload: {}
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("OK");
    });

    it("should handle integer payload via query param", async () => {
      const response = await fastify.inject({
        method: "POST",
        url: "/event-bus/publish/testEvent?integerPayload=123",
        payload: {}
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("OK");
    });

    it("should not register publish route when disabled", async () => {
      const fastifyDisabled = Fastify({ logger: false });
      
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
        disableEventPublishRoute: true,
      };

      await fastifyDisabled.register(EventBusPlugin, options);

      const response = await fastifyDisabled.inject({
        method: "POST",
        url: "/event-bus/publish/testEvent",
        payload: { test: "data" }
      });

      expect(response.statusCode).toBe(404);
      await fastifyDisabled.close();
    });
  });

  describe("EventBus Decorator", () => {
    beforeEach(async () => {
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
    });

    it("should provide EventBus on fastify instance", () => {
      expect(fastify.EventBus).toBeDefined();
      expect(typeof fastify.EventBus.publish).toBe("function");
    });

    it("should allow publishing events programmatically", () => {
      expect(() => {
        fastify.EventBus.publish("testEvent", { test: "data" });
      }).not.toThrow();
    });

    it("should allow publishing events with delay", () => {
      expect(() => {
        fastify.EventBus.publish("testEvent", { test: "data" }, 1000);
      }).not.toThrow();
    });
  });

  describe("Configuration Options", () => {
    it("should handle empty handlers array", async () => {
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
      expect(fastify.EventBus).toBeDefined();
    });

    it("should handle multiple handler files", async () => {
      const handler1: EventHandler = jest.fn().mockResolvedValue(undefined);
      const handler2: EventHandler = jest.fn().mockResolvedValue(undefined);

      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [
          {
            file: "handlers1.ts",
            handlers: { event1: handler1 }
          },
          {
            file: "handlers2.ts", 
            handlers: { event2: handler2 }
          }
        ],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
      expect(fastify.EventBus).toBeDefined();
    });

    it("should handle topic and namespace options", async () => {
      const options: EventBusOptions = {
        busType: "in-process",
        topic: "test-topic",
        namespace: "test-namespace",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(EventBusPlugin, options);
      expect(fastify.EventBus).toBeDefined();
    });

    it("should handle actionConcurrency option", async () => {
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
        actionConcurrency: 5,
      };

      await fastify.register(EventBusPlugin, options);
      expect(fastify.EventBus).toBeDefined();
    });
  });
});