import { FastifyPluginAsync, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import { EventBusOptions, EventMessage } from "./interfaces";
import {
  CreateHandlerRunner,
  getHandlerMap,
  noMatchingHandlers,
} from "./commons";

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  const handlerMap = getHandlerMap(options);

  f.decorateRequest("EventBus", {
    publish(event, payload, processAfterDelayMs) {
      publishToPubSub(event, payload, null, processAfterDelayMs ?? 0);
    },
  });
  f.addHook("onRequest", function (req, _reply, done) {
    req.EventBus = {
      publish(event, payload, processAfterDelayMs) {
        publishToPubSub(event, payload, null, processAfterDelayMs ?? 0, req);
      },
    };
    done();
  });

  const messages: EventMessage[] = [];

  const flush = async () => {
    while (messages.length) {
      const msg = messages.shift();
      if (!msg) {
        break;
      }
      await f.inject({
        method: "POST",
        url: "/local-servicebus/process-message",
        payload: msg,
      });
    }
  };

  f.addHook("onClose", async () => {
    await flush();
    f.log.trace({ tag: "LOCAL_SERVICEBUS_FINAL_FLUSH" });
  });

  f.addHook("onSend", async function (_req, _reply, payload) {
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    flush();
    return payload;
  });

  let msgId = 0;
  function publishToPubSub(
    event: string,
    payload: any,
    file: string | null,
    processAfterDelayMs: number,
    req?: FastifyRequest,
  ) {
    options.validateMsg(event, payload, req);
    const messageBody: EventMessage = {
      id: "in-process" + msgId,
      event,
      attributes: file == null ? {} : { file },
      data: payload,
      publishTime: new Date(),
      processAfterDelayMs: 0,
    };
    msgId++;
    messages.push(messageBody);
    f.log.info(
      "Event pushed --> " +
        event +
        " -- " +
        payload +
        "--" +
        file +
        "--" +
        processAfterDelayMs,
    );
  }

  const selectAndRunHandlers = CreateHandlerRunner(f, options, handlerMap);

  f.post<{ Body: EventMessage }>(
    "/local-servicebus/process-message",
    {
      schema: {
        hide: true,
      } as any,
    },
    async function (req) {
      const msg = req.body;
      if (!msg) {
        return "NO_MESSAGE";
      }
      options.validateMsg(msg.event, msg.data, req);

      if (noMatchingHandlers(handlerMap, msg)) {
        // bail-out
        // service has no event-handlers registered
        return "BAIL_OUT_NO_MATCHING_HANDLERS";
      }

      req.log.info({
        tag: "LOCAL_SERVICEBUS_MESSAGE_HANDLE",
        msg: msg,
      });

      await selectAndRunHandlers(req, msg, (event, payload, file) =>
        publishToPubSub(event, payload, file, 0, req),
      );
      return "OK";
    },
  );
};

export = fp(plugin, { name: "fp-event-bus-local" });
