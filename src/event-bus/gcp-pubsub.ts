import { FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import { PubSub } from "@google-cloud/pubsub";
import { EventBus, EventBusOptions, EventMessage } from "./interfaces";
import {
  CreateHandlerRunner,
  ErrorWithStatus,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";

interface PubsubMessage {
  message: {
    attributes: Record<string, string>;
    data: string;
    messageId: string;
    publishTime: string;
  };
  subscription: string;
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
  const client = new PubSub({
    projectId: options.credentials?.projectId,
    auth: options.credentials?.GoogleAuth,
  });
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
  f.decorate("EventBus", bus);

  f.decorateRequest("EventBus", bus);
  f.addHook("onRequest", function (req, _reply, done) {
    req.EventBus = {
      publish(event, payload, processAfterDelayMs) {
        publishToPubSub(event, payload, null, processAfterDelayMs ?? 0, req);
      },
    };
    done();
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
      const eventMsg = convert(body);
      req.log.info({
        tag: "PUB_SUB_MSG",
        messageId: body.message.messageId,
        subscription: body.subscription,
        attributes: body.message.attributes,
        publishTime: body.message.publishTime,
      });
      options.validateMsg(eventMsg.event, eventMsg.data, req);

      if (noMatchingHandlers(handlerMap, eventMsg)) {
        // bail-out
        // service has no event-handlers registered
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
        // wait for pub-sub to repush. can't process so early
        reply.send(`ProcessAfterDelayMs=${eventMsg.processAfterDelayMs}`);
        reply.status(425);
        return reply;
      }

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
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
          reply.send(err.message);
          reply.status(err.status);
        } else {
          reply.send("ERROR");
          reply.status(500);
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
