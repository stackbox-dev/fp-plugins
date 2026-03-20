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
import { safeCloseAll } from "./utils";

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
  f.decorate("_hasEventHandlers", handlerMap.size > 0);

  const client = new PubSub();
  const topic = client.topic(options.topic, {
    batching: {
      maxMilliseconds: 10,
      maxMessages: 100,
    },
  });

  f.addHook("onClose", async () => {
    f.log.info({ tag: "GCP_PUBSUB_CLOSING" });
    const errors = await safeCloseAll(
      () => topic.flush(),
      () => client.close(),
    );
    if (errors.length > 0) {
      f.log.error({ tag: "GCP_PUBSUB_CLOSE_ERRORS", errors });
    }
  });

  function publishToPubSub(
    event: string,
    payload: any,
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
    topic
      .publishMessage({
        json: { event, payload },
        attributes: attrs,
      })
      .catch((err: unknown) => {
        f.log.error({
          tag: "GCP_PUBSUB_PUBLISH_ERROR",
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
      publishToPubSub(event, payload, null, processAfterDelayMs ?? 0);
    },
  };
  f.decorate("EventBus", {
    getter() {
      return bus;
    },
  });

  f.decorateRequest("EventBus", {
    getter(this: FastifyRequest) {
      const req = this;
      return {
        publish: (event, payload, processAfterDelayMs) => {
          publishToPubSub(event, payload, null, processAfterDelayMs ?? 0, req);
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
      } as any,
    },
    async function (req, reply) {
      const body = req.body;
      if (!body) {
        reply.send("OK");
        return reply;
      }
      req.log.info({
        tag: "PUB_SUB_MSG",
        messageId: body.message.messageId,
        subscription: body.subscription,
        attributes: body.message.attributes,
        publishTime: body.message.publishTime,
        attempt: body.attempt,
      });

      try {
        const eventMsg = convert(body);
        options.validateMsg(eventMsg.event, eventMsg.data, req);

        if (noMatchingHandlers(handlerMap, eventMsg)) {
          reply.send("OK");
          return reply;
        }

        req.log.info({
          tag: "PUB_SUB_MSG_HANDLE",
          event: eventMsg,
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
          reply
            .status(425)
            .send({ processAfterDelayMs: eventMsg?.processAfterDelayMs });
          return reply;
        }

        await selectAndRunHandlers(req, eventMsg, (event, payload, file) =>
          publishToPubSub(event, payload, file, 0, req),
        );
        reply.send("OK");
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
          if (err.status === 400) {
            req.log.warn({
              tag: "GCP_PUBSUB_BAD_MESSAGE",
              err,
              messageId: body.message?.messageId,
            });
          }
          reply.status(err.status).send(err.message);
        } else {
          reply.status(500).send("ERROR");
        }
        return reply;
      }
    },
  );
};

export = fp(plugin, { name: "fp-eventbus-gcp-pubsub" });

function convert(msg: PubsubMessage): EventMessage {
  const buf = Buffer.from(msg.message.data, "base64");
  const json = buf.toString("utf-8");
  let obj: { event: string; payload: any };
  try {
    obj = JSON.parse(json);
  } catch {
    throw new ErrorWithStatus(400, "Invalid JSON in message body");
  }
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
