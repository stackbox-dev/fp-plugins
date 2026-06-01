import { FastifyInstance } from "fastify";
import { AzureServiceBusConsumerBuilder } from "./azure-servicebus";
import { GcpPubSubConsumerBuilder } from "./gcp-pubsub";
import { EventConsumer } from "./interface";
import { RabbitMqServiceBusConsumerBuilder } from "./rabbitmq";
import { EventBusOptions } from "../interfaces";

export function CreateEventConsumer(
  instance: FastifyInstance,
  type: EventBusOptions["busType"],
): Promise<EventConsumer> {
  switch (type) {
    case "gcp-pubsub":
      return GcpPubSubConsumerBuilder(instance);
    case "azure-servicebus":
      return AzureServiceBusConsumerBuilder(instance);
    case "rabbitmq":
      return RabbitMqServiceBusConsumerBuilder(instance);
    default:
      return Promise.resolve({
        close: async () => {
          // Implement the close logic here
        },
      });
  }
}
