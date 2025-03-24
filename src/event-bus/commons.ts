import * as prom from "prom-client";
import { FastifyInstance, FastifyRequest } from "fastify";
import {
  ActionContext,
  EventHandler,
  EventMessage,
  PublishToPubSub,
} from "./interfaces";
import { EventBusOptions } from "./interfaces";

interface HandlerInfo {
  file: string;
  handler: EventHandler;
}

interface AppContext {
  f: FastifyInstance;
  counter: prom.Counter;
  histogram: prom.Histogram;
}

type Action = () => Promise<void>;

export function getHandlerMap(options: Pick<EventBusOptions, "handlers">) {
  const handlerMap = new Map<
    string,
    {
      file: string;
      handler: EventHandler;
    }[]
  >();
  for (const { file, handlers } of options.handlers) {
    for (const [key, handler] of Object.entries(handlers)) {
      if (!handlerMap.has(key)) {
        handlerMap.set(key, []);
      }
      if (handler as any) {
        handlerMap.get(key)!.push({ file, handler });
      }
    }
  }
  return handlerMap;
}

export function noMatchingHandlers(
  handlerMap: Map<
    string,
    {
      file: string;
      handler: EventHandler;
    }[]
  >,
  eventMsg: EventMessage,
) {
  const handlers = handlerMap.get(eventMsg.event) ?? [];
  const specifiedFile = eventMsg.attributes.file;
  for (const { file } of handlers) {
    if (specifiedFile && file !== specifiedFile) {
      continue;
    }
    return false;
  }
  return true;
}

export class ErrorWithStatus extends Error {
  status: number;
  constructor(status: number, message?: string) {
    super(message);
    this.status = status;
  }
}

const CreateActionFactory =
  (options: EventBusOptions, appCtx: AppContext) =>
  (ctx: ActionContext) =>
  async () => {
    const start = Date.now();
    let status = 200;
    try {
      await ctx.handler.call(appCtx.f, ctx.eventMsg, ctx.req);
    } catch (err) {
      if (ctx.eventMsg.attributes.noRetry === "true") {
        return;
      }

      ({ err, status } = options.processError(err, ctx));
      if (!ctx.specifiedFile) {
        // if no specified file is provided, trigger another message with
        // the specified file
        ctx.publishToPubSub(ctx.eventMsg.event, ctx.eventMsg.data, ctx.file);
      } else {
        throw new ErrorWithStatus(
          status,
          `${ctx.file}-${ctx.handler.name} failed with ${err.message}`,
        );
      }
    } finally {
      const label = {
        event: ctx.eventMsg.event,
        file: ctx.file,
        status,
      };
      appCtx.counter.inc(label);
      appCtx.histogram.observe(label, Date.now() - start);
    }
  };

export function CreateHandlerRunner(
  f: FastifyInstance,
  options: EventBusOptions,
  handlersMap: Map<string, HandlerInfo[]>,
) {
  const histogram = new prom.Histogram({
    help: "event_handler_latency_ms",
    name: "event_handler_latency_ms",
    buckets: [
      1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 500, 750, 1000,
      1500, 2000, 2500, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 12000,
      15000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
      70000, 80000,
    ],
    registers: options.registry ? [options.registry] : [],
    labelNames: ["event", "file", "status"] as const,
  });
  const counter = new prom.Counter({
    help: "event_handler_latency_total",
    name: "event_handler_latency_total",
    registers: options.registry ? [options.registry] : [],
    labelNames: ["event", "file", "status"] as const,
  });

  const appCtx: AppContext = {
    f,
    counter,
    histogram,
  };
  const createAction = CreateActionFactory(options, appCtx);

  const CONCURRENCY = options.actionConcurrency ?? 1;
  f.log.info({
    tag: "HANDLER_ACTION_CONCURRENCY",
    concurrency: CONCURRENCY,
  });

  return async function selectAndRunHandlers(
    req: FastifyRequest,
    eventMsg: EventMessage,
    publishToPubSub: PublishToPubSub,
  ) {
    const handlers = handlersMap.get(eventMsg.event) ?? [];
    const actions: Action[] = [];
    const specifiedFile = eventMsg.attributes.file;
    for (const { file, handler } of handlers) {
      if (specifiedFile && file !== specifiedFile) {
        continue;
      }
      const ctx: ActionContext = {
        req,
        publishToPubSub,
        handler,
        eventMsg,
        file,
        specifiedFile,
      };
      const act = createAction(ctx);
      actions.push(act);
    }

    for (let i = 0; i < actions.length; i += CONCURRENCY) {
      await Promise.all(actions.slice(i, i + CONCURRENCY).map((act) => act()));
    }
  };
}
