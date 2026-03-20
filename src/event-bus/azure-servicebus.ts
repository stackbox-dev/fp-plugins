import * as AzureIden from "@azure/identity";
import {
  ServiceBusClient,
  ServiceBusMessage,
  ServiceBusSender,
} from "@azure/service-bus";
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

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  const handlerMap = getHandlerMap(options);
  f.decorate("_hasEventHandlers", handlerMap.size > 0);

  if (!options.topic) {
    throw new Error(
      "Azure ServiceBus needs the topic specified. Use EVENT_TOPIC env var",
    );
  }

  // Connection string mode (emulator / local dev) vs namespace + credential
  const connectionString = process.env.AZURE_SERVICEBUS_CONNECTION_STRING;
  if (!connectionString && !options.namespace) {
    throw new Error(
      "Azure ServiceBus needs either AZURE_SERVICEBUS_CONNECTION_STRING or the namespace specified via EVENT_NAMESPACE env var",
    );
  }
  if (connectionString && !connectionString.includes("Endpoint=")) {
    throw new Error(
      "AZURE_SERVICEBUS_CONNECTION_STRING must contain 'Endpoint=' — check the format",
    );
  }

  // All validation passed — safe to create resources
  const client = connectionString
    ? new ServiceBusClient(connectionString)
    : new ServiceBusClient(
        options.namespace!,
        new AzureIden.DefaultAzureCredential({}),
        {},
      );

  let sender: ServiceBusSender;
  try {
    sender = client.createSender(options.topic);
  } catch (err) {
    await client.close();
    throw err;
  }

  f.addHook("onClose", async () => {
    f.log.info({ tag: "AZURE_SERVICEBUS_CLOSING" });
    const errors = await safeCloseAll(
      () => sender.close(),
      () => client.close(),
    );
    if (errors.length > 0) {
      f.log.error({ tag: "AZURE_SERVICEBUS_CLOSE_ERRORS", errors });
    }
  });

  function publishToServiceBus(
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
    const msg: ServiceBusMessage = {
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
    };

    // Fire-and-forget: publish directly, log errors.
    sender.sendMessages(msg).catch((err: unknown) => {
      f.log.error({
        tag: "AZURE_SERVICEBUS_PUBLISH_ERROR",
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
      publishToServiceBus(event, payload, null, processAfterDelayMs ?? 0);
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
          publishToServiceBus(
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
      try {
        const msg = convert(rawMsg);
        options.validateMsg(msg.event, msg.data, req);

        if (noMatchingHandlers(handlerMap, msg)) {
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
          reply
            .status(425)
            .send({ processAfterDelayMs: msg?.processAfterDelayMs });
          return reply;
        }

        await selectAndRunHandlers(req, msg, (event, payload, file) =>
          publishToServiceBus(event, payload, file, 0, req),
        );
        reply.send("OK");
        return reply;
      } catch (err) {
        if (err instanceof ErrorWithStatus) {
          if (err.status === 400) {
            req.log.warn({
              tag: "AZURE_SERVICEBUS_BAD_MESSAGE",
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

export = fp(plugin, { name: "fp-eventbus-azure-servicebus" });

function convert(msg: IncomingServiceBusMessage): EventMessage {
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
