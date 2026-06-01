import { PubSub } from "@google-cloud/pubsub";
import { FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import {
  CreateHandlerRunner,
  ErrorWithStatus,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";
import { EventBus, EventBusOptions, EventMessage } from "./interfaces";

interface PubsubMessage {
  message: {
    attributes: Record<string, string>;
    data: string;
    messageId: string;
    publishTime: string;
  };
  subscription: string;
  attempt: number;
}

// Resolves @opentelemetry/api from either NODE_PATH (OTel operator) or the
// pnpm absolute path. All copies share the same global symbol so the
// registered SDK is always reached.
function getOtelApi() {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require("@opentelemetry/api");
  } catch {
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      return require(
        "/app/node_modules/.pnpm/@opentelemetry+api@1.9.0/node_modules/@opentelemetry/api",
      );
    } catch {
      return null;
    }
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const otel: any = getOtelApi();
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const tracer: any = otel
  ? otel.trace.getTracer("fp-eventbus-gcp-pubsub")
  : null;

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  if (!options.topic) {
    throw new Error(
      "Google Cloud PubSub needs the topic specified. Use EVENT_TOPIC env var",
    );
  }

  const handlerMap = getHandlerMap(options);
  const client = new PubSub();
  const topic = client.topic(options.topic, {
    batching: {
      maxMilliseconds: 10,
      maxMessages: 100,
    },
  });

  f.addHook("onClose", async () => {
    await topic.flush();
    f.log.info({ tag: "GCP_PUBSUB_FINAL_FLUSH" });
    await client.close();
  });

  function publishToPubSub(
    event: string,
    payload: any, // eslint-disable-line @typescript-eslint/no-explicit-any
    file: string | null,
    processAfterDelayMs: number,
    req?: FastifyRequest,
  ) {
    options.validateMsg(event, payload, req);
    const attrs: Record<string, string> = {
      event,
    };
    if (file) {
      attrs.file = file;
    }
    if (processAfterDelayMs > 0) {
      attrs.processAfterDelayMs = "" + processAfterDelayMs;
    }

    // Inject the active OTel span context as a W3C traceparent/tracestate into
    // Pub/Sub message attributes.  context.active() reads from AsyncLocalStorage
    // at call-time, so when this is invoked inside a context.with() block (e.g.
    // the outbox worker) the correct parent context is used automatically.
    if (otel) {
      otel.propagation.inject(otel.context.active(), attrs);
      if (attrs.traceparent) {
        (req ?? f).log.info({
          tag: "EVENT_TRACEPARENT_INJECT",
          event,
          traceparent: attrs.traceparent,
        });
      }
    }

    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    topic.publishMessage({
      json: { event, payload },
      attributes: attrs,
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
      publishToPubSub(event, payload, null, processAfterDelayMs ?? 0);
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
        // req.EventBus.publish — called inside an HTTP request handler.
        // context.active() already holds the HTTP server span context here,
        // so propagation.inject works automatically.
        publish: (event: string, payload: any, processAfterDelayMs?: number) => { // eslint-disable-line @typescript-eslint/no-explicit-any
          publishToPubSub(event, payload, null, processAfterDelayMs ?? 0, this);
        },
      };
    },
  });

  const selectAndRunHandlers = CreateHandlerRunner(f, options, handlerMap);

  f.post<{ Body: PubsubMessage }>(
    "/gcp-pubsub/process-message",
    {
      schema: {
        hide: true,
      } as any, // eslint-disable-line @typescript-eslint/no-explicit-any
    },
    async function (req, reply) {
      const body = req.body;
      if (!body) {
        reply.send("OK");
        return reply;
      }
      const eventMsg = convert(body);
      const attrs = body.message.attributes ?? {};

      // Extract the W3C trace context that was injected by the publisher.
      // This reconstructs the parent span context so all handler spans
      // (DB queries, further publishes) are children of the original trace.
      let handlerCtx = otel ? otel.context.active() : null;
      let span = null;
      if (otel && attrs.traceparent) {
        const parentCtx = otel.propagation.extract(
          otel.context.active(),
          attrs,
        );
        span = tracer.startSpan(
          `pubsub.consume.${eventMsg.event}`,
          { kind: otel.SpanKind.CONSUMER },
          parentCtx,
        );
        handlerCtx = otel.trace.setSpan(parentCtx, span);
      }

      req.log.info({
        tag: "PUB_SUB_MSG",
        messageId: body.message.messageId,
        subscription: body.subscription,
        attributes: attrs,
        publishTime: body.message.publishTime,
        attempt: body.attempt,
        traceparent_extracted: attrs.traceparent ?? "MISSING",
      });

      options.validateMsg(eventMsg.event, eventMsg.data, req);

      if (noMatchingHandlers(handlerMap, eventMsg)) {
        span?.end();
        reply.send("OK");
        return reply;
      }

      req.log.info({
        tag: "PUB_SUB_MSG_HANDLE",
        event: eventMsg,
        traceparent: attrs.traceparent ?? "MISSING",
      });

      if (
        eventMsg.processAfterDelayMs > 0 &&
        Date.now() <
          eventMsg.publishTime.getTime() + eventMsg.processAfterDelayMs
      ) {
        req.log.info({
          tag: "PUB_SUB_MSG_DELAYED",
          eventId: eventMsg.id,
        });
        span?.end();
        reply
          .status(425)
          .send({ processAfterDelayMs: eventMsg?.processAfterDelayMs });
        return reply;
      }

      const runHandlers = async () => {
        try {
          await selectAndRunHandlers(req, eventMsg, (event, payload, file) =>
            publishToPubSub(
              event,
              payload,
              file,
              eventMsg.processAfterDelayMs,
              req,
            ),
          );
          reply.send("OK");
        } catch (err) {
          if (err instanceof ErrorWithStatus) {
            reply.status(err.status).send(err.message);
          } else {
            reply.status(500).send("ERROR");
          }
        } finally {
          span?.end();
        }
      };

      // Run handlers inside the extracted parent context so all child spans
      // (DB queries, further publishes) are linked to the original trace tree.
      if (otel && handlerCtx) {
        await otel.context.with(handlerCtx, runHandlers);
      } else {
        await runHandlers();
      }
      return reply;
    },
  );
};

export = fp(plugin, { name: "fp-eventbus-gcp-pubsub" });

function convert(msg: PubsubMessage): EventMessage {
  const buf = Buffer.from(msg.message.data, "base64");
  const json = buf.toString("utf-8");
  const obj = JSON.parse(json);
  return {
    id: msg.message.messageId,
    publishTime: new Date(msg.message.publishTime),
    processAfterDelayMs:
      parseInt(msg.message.attributes.processAfterDelayMs ?? "0", 10) || 0,
    attributes: msg.message.attributes,
    event: obj.event,
    data: obj.payload,
  };
}
