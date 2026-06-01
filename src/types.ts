import { EventBus } from "./event-bus/interfaces";

declare module "fastify" {
  export interface FastifyRequest {
    EventBus: EventBus;
  }
  export interface FastifySchema {
    operationId?: string;
    summary?: string;
    description?: string;
  }
}
