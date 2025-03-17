import { FastifyPluginAsync } from "fastify";
import fp from "fastify-plugin";
import { EventBusOptions } from "./interfaces";

const plugin: FastifyPluginAsync<EventBusOptions> = async function (
  f,
  options,
) {
  switch (options.busType) {
    case "gcp-pubsub":
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      f.register(require("./gcp-pubsub"), options);
      break;
    case "azure-servicebus":
      if (!options.namespace) {
        throw new Error(
          "Azure ServiceBus needs the namespace specified. Use EVENT_NAMESPACE env var",
        );
      }
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      f.register(require("./azure-servicebus"), options);
      break;
    case "rabbitmq":
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      f.register(require("./rabbitmq"), options);
      break;
    default:
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      f.register(require("./local"), options);
  }

  ///
  if (!options.disableEventPublishRoute) {
    f.post<{
      Params: { event: string };
      Querystring: {
        stringPayload?: string;
        integerPayload?: string;
      };
      Body: any;
    }>(
      "/event-bus/publish/:event",
      {
        schema: {
          description:
            "API to push a event manually. Use the appropriate query-param or request-body to send the payload.",
          operationId: "publishToEventBus",
          params: {
            event: { type: "string" },
          },
          querystring: {
            stringPayload: { type: "string" },
            integerPayload: { type: "string" },
          },
          body: {
            type: "object",
            additionalProperties: true,
          },
        },
      },
      async function (req) {
        if (req.query.stringPayload || req.query.integerPayload) {
          req.EventBus.publish(
            req.params.event,
            req.query.stringPayload || req.query.integerPayload,
          );
        } else {
          req.EventBus.publish(req.params.event, req.body);
        }
        return "OK";
      },
    );
  }
};

export default fp(plugin, {
  name: "fp-event-bus",
  dependencies: ["fp-config"],
});
