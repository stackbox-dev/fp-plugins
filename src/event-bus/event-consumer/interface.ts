import { FastifyInstance } from "fastify";

export interface EventConsumer {
  close(): Promise<void>;
}

export type EventConsumerBuilder = (
  instance: FastifyInstance,
) => Promise<EventConsumer>;
