import { FastifyInstance } from "fastify";
import { AzureServiceBusConsumerBuilder } from "./azure-servicebus";
import { GcpPubSubConsumerBuilder } from "./gcp-pubsub";
import { EventConsumer } from "./interface";
import { RabbitMqServiceBusConsumerBuilder } from "./rabbitmq";
import { EventBusOptions } from "../interfaces";

export function CreateEventConsumer(
  instance: FastifyInstance,
  type: EventBusOptions["busType"],
  credentials: any,
): Promise<EventConsumer> {
  switch (type) {
    case "gcp-pubsub":
      return GcpPubSubConsumerBuilder(instance, credentials);
    case "azure-servicebus":
      return AzureServiceBusConsumerBuilder(instance, credentials);
    case "rabbitmq":
      return RabbitMqServiceBusConsumerBuilder(instance, credentials);
    default:
      return Promise.resolve({
        close: async () => {
          // Implement the close logic here
        },
      });
  }
}
