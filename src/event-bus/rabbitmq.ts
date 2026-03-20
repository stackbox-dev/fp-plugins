import { randomUUID } from "crypto";
import { FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import { Connection } from "rabbitmq-client";
import {
  CreateHandlerRunner,
  ErrorWithStatus,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";
import { EventBus, EventBusOptions, EventMessage } from "./interfaces";
import {
  ensureRabbitMqExchangesAndQueues,
  getServicePrefix,
} from "./rabbitmq-utils";
import { safeCloseAll } from "./utils";

interface IncomingRabbitMqMessage {
  messageId: number;
  body: string;
}

interface MessageBody {
  event: string;
  payload: any;
  file: string | null;
  processAfterDelayMs: number | undefined;
  publishTimestamp: number;
}

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  const handlerMap = getHandlerMap(options);
  f.decorate("_hasEventHandlers", handlerMap.size > 0);

  if (!process.env.RABBITMQ_URL) {
    throw new Error("RabbitMq requires RABBITMQ_URL");
  }
  if (!process.env.K_SERVICE) {
    throw new Error("RabbitMq requires K_SERVICE");
  }
  const connection = new Connection(process.env.RABBITMQ_URL);
  connection.on("error", (err) => {
    f.log.error({ tag: "RABBITMQ_CONNECTION_ERROR", err });
  });

  const service = process.env.K_SERVICE;
  if (options.ensureExchangesAndQueues) {
    await ensureRabbitMqExchangesAndQueues(connection, service);
  }

  const publisher = connection.createPublisher({ maxAttempts: 3 });
  const prefix = getServicePrefix(service);

  f.addHook("onClose", async () => {
    f.log.info({ tag: "RABBITMQ_CLOSING" });
    const errors = await safeCloseAll(
      () => publisher.close(),
      () => connection.close(),
    );
    if (errors.length > 0) {
      f.log.error({ tag: "RABBITMQ_CLOSE_ERRORS", errors });
    }
  });

  function publishToExchange(
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

    // Fire-and-forget: publish directly, log errors.
    // publish() must remain sync (void return) — callers don't await.
    publisher
      .send(
        {
          messageId: randomUUID(),
          appId: `${prefix}.${service}`,
          contentType: "application/json",
          durable: true,
          exchange: `${prefix}.main-exchange`,
          headers: {
            event: messageBody.event,
            file: messageBody.file,
            processAfterDelayMs: "" + messageBody.processAfterDelayMs,
          },
        },
        JSON.stringify(messageBody, null, 0),
      )
      .catch((err: unknown) => {
        f.log.error({
          tag: "RABBITMQ_PUBLISH_ERROR",
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
      publishToExchange(event, payload, null, processAfterDelayMs ?? 0);
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
          publishToExchange(
            event,
            payload,
            null,
            processAfterDelayMs ?? 0,
            req,
          );
        },
      };
    },
  });

  const selectAndRunHandlers = CreateHandlerRunner(f, options, handlerMap);

  f.post<{ Body: IncomingRabbitMqMessage }>(
    "/rabbitmq/process-message",
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
        tag: "RABBITMQ_MESSAGE_RECEIVED",
        messageId: rawMsg.messageId,
      });
      try {
        const msg = convert(rawMsg);
        options.validateMsg(msg.event, msg.data, req);

        if (noMatchingHandlers(handlerMap, msg)) {
          reply.send("OK");
          return reply;
        }

        req.log.info({
          tag: "RABBITMQ_MESSAGE_PROCESSING",
          event: msg,
        });

        if (
          msg.processAfterDelayMs > 0 &&
          Date.now() < msg.publishTime.getTime() + msg.processAfterDelayMs
        ) {
          reply
            .status(425)
            .send({ processAfterDelayMs: msg?.processAfterDelayMs });
          return reply;
        }

        await selectAndRunHandlers(req, msg, (event, payload, file) =>
          publishToExchange(event, payload, file, 0, req),
        );
        reply.send("OK");
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
          if (err.status === 400) {
            req.log.warn({
              tag: "RABBITMQ_BAD_MESSAGE",
              err,
              messageId: rawMsg.messageId,
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

export = fp(plugin, { name: "fp-eventbus-rabbitmq" });

function convert(msg: IncomingRabbitMqMessage): EventMessage {
  let body: MessageBody;
  try {
    body = JSON.parse(msg.body);
  } catch {
    throw new ErrorWithStatus(400, "Invalid JSON in message body");
  }
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
