import { FastifyInstance, FastifyRequest } from "fastify";
import { Registry } from "prom-client";

export interface EventBusOptions {
  busType: "rabbitmq" | "gcp-pubsub" | "azure-servicebus" | "in-process";
  topic?: string;
  namespace?: string;
  handlers: {
    file: string;
    handlers: EventHandlers;
  }[];
  validateMsg: (event: string, payload: any, req?: FastifyRequest) => void;
  processError(err: any, ctx: ActionContext): { err: any; status: number };
  disableEventPublishRoute?: boolean;
  actionConcurrency?: number;
  credentials?: any;
  registry: Registry;
}

export type PublishToPubSub = (
  event: string,
  payload: any,
  file: string | null,
) => void;

export interface ActionContext {
  req: FastifyRequest;
  publishToPubSub: PublishToPubSub;
  handler: EventHandler;
  ///
  eventMsg: EventMessage;
  file: string;
  specifiedFile: string | undefined;
}

export type EventHandlers = {
  readonly [k: string]: EventHandler<any>;
};

export type EventHandler<T = any> = (
  this: FastifyInstance,
  msg: EventMessage<T>,
  req: FastifyRequest,
) => Promise<void>;

export interface EventMessage<T = any> {
  id: string;
  publishTime: Date;
  processAfterDelayMs: number;
  attributes: Record<string, string>;
  event: string;
  data: T;
}

export interface EventBus {
  // published event should be processed after (Date.now() + processAfterDelayMs)
  publish(event: string, payload: any, processAfterDelayMs?: number): void;
}
