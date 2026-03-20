import * as timers from "node:timers/promises";
import { JetStreamClient, jetstream } from "@nats-io/jetstream";
import { headers as natsHeaders } from "@nats-io/nats-core";
import { connect } from "@nats-io/transport-node";
import { FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import {
  CreateHandlerRunner,
  ErrorWithStatus,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";
import { EventBus, EventBusOptions, EventMessage } from "./interfaces";

interface IncomingNatsMessage {
  messageId: string;
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

  if (!process.env.NATS_SERVERS) {
    throw new Error("NATS JetStream requires NATS_SERVERS");
  }
  if (!options.topic) {
    throw new Error(
      "NATS JetStream needs the topic specified. Use EVENT_TOPIC env var",
    );
  }

  const nc = await connect({
    servers: process.env.NATS_SERVERS.split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    name: process.env.K_SERVICE ?? "fp-eventbus",
  });

  // Monitor connection status for visibility
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === "error" || s.type === "disconnect") {
        f.log.error({
          tag: "NATS_CONNECTION_STATUS",
          type: s.type,
          data: s.data,
        });
      } else if (s.type === "reconnect") {
        f.log.info({ tag: "NATS_CONNECTION_STATUS", type: s.type });
      }
    }
  })().catch((err) => {
    f.log.error({ tag: "NATS_STATUS_MONITOR_ERROR", err });
  });

  let js: JetStreamClient;
  try {
    js = jetstream(nc);
  } catch (err) {
    await nc.drain();
    throw err;
  }

  const subjectPrefix = options.topic;
  const inflight = new Set<Promise<void>>();
  const MAX_INFLIGHT = 10_000;
  const ctrl = new AbortController();

  f.addHook("onClose", async () => {
    f.log.info({ tag: "NATS_JETSTREAM_CLOSING" });
    ctrl.abort();
    try {
      await Promise.allSettled([...inflight]);
      await nc.drain();
    } catch (err) {
      f.log.error({ tag: "NATS_JETSTREAM_CLOSE_ERROR", err });
    }
  });

  function publishToNats(
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

    const hdrs = natsHeaders();
    hdrs.set("event", event);
    hdrs.set("file", file ?? "");
    hdrs.set(
      "processAfterDelayMs",
      "" + (messageBody.processAfterDelayMs ?? 0),
    );

    // Fire-and-forget with retry: NATS client has no built-in publish
    // retries (unlike RabbitMQ/GCP/Azure SDKs), so we retry up to 3 times.
    if (inflight.size >= MAX_INFLIGHT) {
      f.log.error({
        tag: "NATS_JETSTREAM_PUBLISH_BACKPRESSURE",
        event,
        inflightCount: inflight.size,
      });
      return;
    }
    const subject = `${subjectPrefix}.${event}`;
    const data = Buffer.from(JSON.stringify(messageBody));
    const opts = { headers: hdrs };
    const p = (async () => {
      const MAX_ATTEMPTS = 3;
      for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        try {
          await js.publish(subject, data, opts);
          return;
        } catch (err) {
          if (attempt === MAX_ATTEMPTS) {
            f.log.error({
              tag: "NATS_JETSTREAM_PUBLISH_ERROR",
              err,
              event,
              attempts: attempt,
            });
          } else {
            f.log.warn({
              tag: "NATS_JETSTREAM_PUBLISH_RETRY",
              err,
              event,
              attempt,
            });
            try {
              await timers.setTimeout(250 * 2 ** (attempt - 1), undefined, {
                signal: ctrl.signal,
              });
            } catch (err) {
              if ((err as Error).name === "AbortError") return;
              throw err;
            }
          }
        }
      }
    })();
    inflight.add(p);
    p.finally(() => inflight.delete(p));

    req?.log.info({
      tag: "EVENT_PUBLISH",
      event,
      payload,
      processAfterDelayMs,
    });
  }

  const bus: EventBus = {
    publish(event, payload, processAfterDelayMs) {
      publishToNats(event, payload, null, processAfterDelayMs ?? 0);
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
          publishToNats(event, payload, null, processAfterDelayMs ?? 0, req);
        },
      };
    },
  });

  const selectAndRunHandlers = CreateHandlerRunner(f, options, handlerMap);

  f.post<{ Body: IncomingNatsMessage }>(
    "/nats-jetstream/process-message",
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
        tag: "NATS_JETSTREAM_MESSAGE_RECEIVED",
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
          tag: "NATS_JETSTREAM_MESSAGE_PROCESSING",
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
          publishToNats(event, payload, file, 0, req),
        );
        reply.send("OK");
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
          if (err.status === 400) {
            req.log.warn({
              tag: "NATS_JETSTREAM_BAD_MESSAGE",
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

export = fp(plugin, { name: "fp-eventbus-nats-jetstream" });

function convert(msg: IncomingNatsMessage): EventMessage {
  let body: MessageBody;
  try {
    body = JSON.parse(msg.body);
  } catch {
    throw new ErrorWithStatus(400, "Invalid JSON in message body");
  }
  return {
    id: msg.messageId,
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
