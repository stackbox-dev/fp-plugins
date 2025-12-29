import { FastifyInstance, FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import { Queue } from "mnemonist";
import { Connection, Publisher } from "rabbitmq-client";
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

interface MessageWithAttempts {
  body: MessageBody;
  attempts: number;
}

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  const handlerMap = getHandlerMap(options);

  if (!process.env.RABBITMQ_URL) {
    throw new Error("RabbitMq requires RABBITMQ_URL");
  }
  if (!process.env.K_SERVICE) {
    throw new Error("RabbitMq requires K_SERVICE");
  }
  const connection = new Connection(process.env.RABBITMQ_URL);
  const service = process.env.K_SERVICE;
  await ensureRabbitMqExchangesAndQueues(connection, service);

  const publisher = connection.createPublisher({ maxAttempts: 3 });

  const msgQueue = new Queue<MessageWithAttempts>();

  const flush = createMessageFlusher(f, publisher, msgQueue);
  const ref = setInterval(() => {
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    flush();
  }, 20);

  f.addHook("onClose", async () => {
    f.log.info({ tag: "RABBITMQ_FINAL_FLUSH" });
    clearInterval(ref);
    // final task to ensure all messages are flushed
    await flush(true);
    await publisher.close();
    await connection.close();
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
    msgQueue.enqueue({
      body: messageBody,
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
      publishToExchange(event, payload, null, processAfterDelayMs ?? 0);
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
        publish: (event, payload, processAfterDelayMs) => {
          publishToExchange(
            event,
            payload,
            null,
            processAfterDelayMs ?? 0,
            this,
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
      const msg = convert(rawMsg);
      options.validateMsg(msg.event, msg.data, req);

      if (noMatchingHandlers(handlerMap, msg)) {
        // bail-out
        // service has no event-handlers registered
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
        // wait for pub-sub to repush. can't process so early
        reply
          .status(425)
          .send({ processAfterDelayMs: msg?.processAfterDelayMs });
        return reply;
      }

      try {
        await selectAndRunHandlers(req, msg, (event, payload, file) =>
          publishToExchange(event, payload, file, 0, req),
        );
        reply.send("OK");
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
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
    throw new ErrorWithStatus(
      400,
      `Invalid JSON in message body: ${msg.body?.substring(0, 100)}`,
    );
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

function createMessageFlusher(
  f: FastifyInstance,
  publisher: Publisher,
  msgQueue: Queue<MessageWithAttempts>,
) {
  let running = false;
  let currentFlush: Promise<void> | null = null;
  const CONCURRENCY = 10;
  return async function flush(force = false) {
    if (msgQueue.size === 0) {
      return;
    }
    // If a flush is already running, wait for it to complete (for forced flushes) or skip
    if (running) {
      if (force && currentFlush) {
        await currentFlush;
      } else {
        return;
      }
    }
    running = true;
    const doFlush = async () => {
      let flushed = 0;
      const total = msgQueue.size;
      const start = Date.now();
      try {
        const batch: MessageWithAttempts[] = [];
        while (msgQueue.size > 0) {
          const message = msgQueue.dequeue();
          if (!message) {
            break;
          }
          batch.push(message);

          if (batch.length >= CONCURRENCY) {
            await flushBatch(batch, publisher, f, msgQueue, () => flushed++);
            batch.length = 0;
          }
        }
        // Flush any remaining messages in the batch
        if (batch.length > 0) {
          await flushBatch(batch, publisher, f, msgQueue, () => flushed++);
        }
      } catch (err) {
        const errMsg = err instanceof Error ? err.message : String(err);
        f.log.error({
          tag: "RABBITMQ_FLUSH_ERROR",
          msg: errMsg,
          err,
        });
      } finally {
        running = false;
        currentFlush = null;
        const latency = Date.now() - start;
        if (latency > 100) {
          f.log.warn({
            tag: "RABBITMQ_SLOW_FLUSH",
            latency,
            total,
            flushed,
          });
        }
      }
    };
    currentFlush = doFlush();
    await currentFlush;
  };
}

async function flushBatch(
  batch: MessageWithAttempts[],
  publisher: Publisher,
  f: FastifyInstance,
  msgQueue: Queue<MessageWithAttempts>,
  onSuccess: () => void,
) {
  const service = process.env.K_SERVICE ?? "";
  const prefix = getServicePrefix(service);
  await Promise.all(
    batch.map((msg) =>
      publisher
        .send(
          {
            appId: `${prefix}.${service}`,
            contentType: "application/json",
            durable: true,
            exchange: `${prefix}.main-exchange`,
            headers: {
              event: msg.body.event,
              file: msg.body.file,
              processAfterDelayMs: "" + msg.body.processAfterDelayMs,
            },
          },
          JSON.stringify(msg.body, null, 0),
        )
        .then(() => {
          onSuccess();
        })
        .catch((err: unknown) => {
          const errMsg = err instanceof Error ? err.message : String(err);
          msg.attempts++;
          if (msg.attempts < 3) {
            f.log.error({
              tag: "RABBITMQ_PUBLISH_ERROR",
              msg: errMsg,
              err,
              attempt: msg.attempts,
            });
            msgQueue.enqueue(msg);
          } else {
            f.log.error({
              tag: "RABBITMQ_MESSAGE_PERMANENTLY_DROPPED",
              msg: errMsg,
              err,
              event: msg.body.event,
              payload: msg.body.payload,
            });
          }
        }),
    ),
  );
}
