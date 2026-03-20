import { FastifyInstance } from "fastify";
import { EventBusOptions } from "../interfaces";
import { AzureServiceBusConsumerBuilder } from "./azure-servicebus";
import { GcpPubSubConsumerBuilder } from "./gcp-pubsub";
import { EventConsumer } from "./interface";
import { NatsJetStreamConsumerBuilder } from "./nats-jetstream";
import { RabbitMqServiceBusConsumerBuilder } from "./rabbitmq";

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
    case "nats-jetstream":
      return NatsJetStreamConsumerBuilder(instance);
    default:
      return Promise.resolve({
        close: async () => {
          // Implement the close logic here
        },
      });
  }
}
