import { FastifyInstance, FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import {
  ServiceBusClient,
  ServiceBusMessage,
  ServiceBusSender,
} from "@azure/service-bus";
import { EventBus, EventBusOptions, EventMessage } from "./interfaces";
import {
  CreateHandlerRunner,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";

// eslint-disable-next-line @typescript-eslint/no-require-imports
import Denque = require("denque");

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

interface MessageWithAttempts {
  msg: ServiceBusMessage;
  attempts: number;
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

  const client = new ServiceBusClient(
    options.namespace,
    options.credentials,
    {},
  );

  const sender = client.createSender(options.topic);

  const msgQueue = new Denque<MessageWithAttempts>();

  const flush = createMessageFlusher(f, sender, msgQueue);
  const ref = setInterval(() => {
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    flush();
  }, 20);

  f.addHook("onClose", async () => {
    f.log.info({ tag: "AZURE_SERVICEBUS_FINAL_FLUSH" });
    clearInterval(ref);
    // final task to ensure all messages are flushed
    await flush(true);
    await sender.close();
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
    const messageBody: MessageBody = {
      event,
      payload,
      file: file ?? null,
      processAfterDelayMs:
        processAfterDelayMs > 0 ? processAfterDelayMs : undefined,
      publishTimestamp: Date.now(),
    };
    const encoded = JSON.stringify(messageBody);
    msgQueue.push({
      msg: {
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
      },
      attempts: 0,
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
        // bail-out
        // service has no event-handlers registered
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
        // wait for pub-sub to repush. can't process so early
        reply.send(`ProcessAfterDelayMs=${msg.processAfterDelayMs}`);
        reply.status(425);
        return reply;
      }

      await selectAndRunHandlers(req, msg, (event, payload, file) =>
        publishToPubSub(event, payload, file, msg.processAfterDelayMs, req),
      );
      return "OK";
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

function createMessageFlusher(
  f: FastifyInstance,
  sender: ServiceBusSender,
  msgQueue: Denque<MessageWithAttempts>,
) {
  let running = false;
  return async function flush(force = false) {
    if (msgQueue.length === 0 || (!force && running)) {
      return;
    }
    running = true;
    const tracker: MessageWithAttempts[] = [];
    let flushed = 0;
    const total = msgQueue.length;
    const start = Date.now();
    try {
      let batch = await sender.createMessageBatch({});
      while (msgQueue.length > 0) {
        const message = msgQueue.shift();
        if (!message) {
          break;
        }
        const added = batch.tryAddMessage(message.msg);
        if (added) {
          tracker.push(message);
        } else {
          if (batch.count > 0) {
            await sender.sendMessages(batch);
            flushed += batch.count;
            tracker.length = 0;
          }
          batch = await sender.createMessageBatch();
          // try adding this message solo to a batch
          if (batch.tryAddMessage(message.msg)) {
            tracker.push(message);
          } else {
            f.log.error({
              tag: "AZURE_SERVICE_BUS_ERROR",
              msg: "Message too big to fit in a batch",
            });
          }
        }
      }
      if (batch.count > 0) {
        await sender.sendMessages(batch);
        flushed += batch.count;
        tracker.length = 0;
      }
    } catch (err) {
      for (const message of tracker) {
        // don't re-attempt indefinitely
        if (message.attempts < 3) {
          msgQueue.push({
            msg: message.msg,
            attempts: message.attempts + 1,
          });
        }
      }
      tracker.length = 0;
      f.log.error({
        tag: "AZURE_SERVICE_BUS_ERROR",
        msg: err.message,
        err,
      });
    } finally {
      running = false;
      const latency = Date.now() - start;
      if (latency > 100) {
        f.log.warn({
          tag: "AZURE_SERVICEBUS_SLOW_FLUSH",
          latency,
          total,
          flushed,
        });
      }
    }
  };
}
