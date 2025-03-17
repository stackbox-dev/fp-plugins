import { FastifyInstance } from "fastify";

export interface EventConsumer {
  close(): Promise<void>;
}

export type EventConsumerBuilder = (
  instance: FastifyInstance,
  credentials?: any,
) => Promise<EventConsumer>;
