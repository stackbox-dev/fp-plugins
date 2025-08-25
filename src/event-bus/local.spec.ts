import Fastify from "fastify";
import LocalEventBusPlugin from "./local";
import { EventBusOptions, EventHandler, EventMessage } from "./interfaces";

describe("Local EventBus Plugin", () => {
  let mockValidateMsg: jest.Mock;
  let mockProcessError: jest.Mock;
  let mockHandler: EventHandler;

  beforeEach(() => {
    mockValidateMsg = jest.fn();
    mockProcessError = jest.fn().mockReturnValue({ err: new Error("test"), status: 500 });
    mockHandler = jest.fn().mockResolvedValue(undefined);
  });

  describe("Local EventBus Registration", () => {
    it("should register local event bus plugin", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);
      
      expect(fastify.EventBus).toBeDefined();
      expect(typeof fastify.EventBus.publish).toBe("function");
      
      await fastify.close();
    });

    it("should decorate both fastify instance and request", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      // Test fastify instance decoration
      expect(fastify.EventBus).toBeDefined();
      
      // Test request decoration through a route
      fastify.get("/test", async (request) => {
        expect(request.EventBus).toBeDefined();
        expect(typeof request.EventBus.publish).toBe("function");
        return "ok";
      });

      const response = await fastify.inject({
        method: "GET",
        url: "/test"
      });

      expect(response.statusCode).toBe(200);
      
      await fastify.close();
    });
  });

  describe("Event Publishing", () => {
    it("should publish events and validate", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      // Test publishing - this should call validateMsg
      expect(() => {
        fastify.EventBus.publish("testEvent", { test: "data" });
      }).not.toThrow();

      expect(mockValidateMsg).toHaveBeenCalledWith("testEvent", { test: "data" }, undefined);
      
      // Don't close fastify to avoid flush hook issues
    });

    it("should publish events with delay", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      expect(() => {
        fastify.EventBus.publish("testEvent", { test: "data" }, 1000);
      }).not.toThrow();

      expect(mockValidateMsg).toHaveBeenCalledWith("testEvent", { test: "data" }, undefined);
      
      // Don't close fastify to avoid flush hook issues
    });
  });

  describe("Message Processing", () => {
    it("should process messages via internal route", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      const testMessage: EventMessage = {
        id: "test-msg-1",
        event: "testEvent",
        data: { test: "data" },
        attributes: {},
        publishTime: new Date("2023-01-01"),
        processAfterDelayMs: 0,
      };

      const response = await fastify.inject({
        method: "POST",
        url: "/local-servicebus/process-message",
        payload: testMessage
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("OK");
      expect(mockValidateMsg).toHaveBeenCalledWith("testEvent", { test: "data" }, expect.any(Object));
      
      await fastify.close();
    });

    it("should handle messages with no body", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      const response = await fastify.inject({
        method: "POST",
        url: "/local-servicebus/process-message"
        // No payload
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("NO_MESSAGE");
      
      await fastify.close();
    });

    it("should bail out when no matching handlers", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      const testMessage: EventMessage = {
        id: "test-msg-1",
        event: "nonExistentEvent",
        data: { test: "data" },
        attributes: {},
        publishTime: new Date("2023-01-01"),
        processAfterDelayMs: 0,
      };

      const response = await fastify.inject({
        method: "POST",
        url: "/local-servicebus/process-message",
        payload: testMessage
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("BAIL_OUT_NO_MATCHING_HANDLERS");
      
      await fastify.close();
    });

    it("should execute matching event handlers", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      const testMessage: EventMessage = {
        id: "test-msg-1",
        event: "testEvent",
        data: { test: "execution data" },
        attributes: {},
        publishTime: new Date("2023-01-01"),
        processAfterDelayMs: 0,
      };

      const response = await fastify.inject({
        method: "POST",
        url: "/local-servicebus/process-message",
        payload: testMessage
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("OK");
      expect(mockHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          id: "test-msg-1",
          event: "testEvent",
          data: { test: "execution data" },
          attributes: {},
          processAfterDelayMs: 0,
        }),
        expect.any(Object)
      );
      
      await fastify.close();
    });

    it("should handle messages with file attributes", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "specific-file.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      const testMessage: EventMessage = {
        id: "test-msg-1",
        event: "testEvent",
        data: { test: "data" },
        attributes: { file: "specific-file.ts" },
        publishTime: new Date("2023-01-01"),
        processAfterDelayMs: 0,
      };

      const response = await fastify.inject({
        method: "POST",
        url: "/local-servicebus/process-message",
        payload: testMessage
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toBe("OK");
      
      await fastify.close();
    });
  });

  describe("Error Handling", () => {
    it("should handle validation errors", async () => {
      const fastify = Fastify({ logger: false });
      const mockValidateMsgFail = jest.fn().mockImplementation(() => {
        throw new Error("Validation failed");
      });

      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsgFail,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      expect(() => {
        fastify.EventBus.publish("testEvent", { invalid: "data" });
      }).toThrow("Validation failed");
      
      await fastify.close();
    });

    it("should handle handler execution errors gracefully", async () => {
      const fastify = Fastify({ logger: false });
      const failingHandler: EventHandler = jest.fn().mockRejectedValue(new Error("Handler failed"));

      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { failingEvent: failingHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      const testMessage: EventMessage = {
        id: "test-msg-1",
        event: "failingEvent",
        data: { test: "data" },
        attributes: {},
        publishTime: new Date("2023-01-01"),
        processAfterDelayMs: 0,
      };

      // The request should still complete, errors are handled by the handler runner
      const response = await fastify.inject({
        method: "POST",
        url: "/local-servicebus/process-message",
        payload: testMessage
      });

      expect(response.statusCode).toBe(200);
      expect(failingHandler).toHaveBeenCalled();
      
      await fastify.close();
    });
  });

  describe("Message Queue Integration", () => {
    it("should handle basic message flow", async () => {
      const fastify = Fastify({ logger: false });
      const options: EventBusOptions = {
        busType: "in-process",
        handlers: [{
          file: "test.ts",
          handlers: { testEvent: mockHandler }
        }],
        validateMsg: mockValidateMsg,
        processError: mockProcessError,
      };

      await fastify.register(LocalEventBusPlugin, options);

      // Publish an event
      expect(() => {
        fastify.EventBus.publish("testEvent", { test: "basic flow" });
      }).not.toThrow();

      // Verify validation was called
      expect(mockValidateMsg).toHaveBeenCalledWith("testEvent", { test: "basic flow" }, undefined);
      
      // Don't close to avoid flush hook issues
    });
  });
});