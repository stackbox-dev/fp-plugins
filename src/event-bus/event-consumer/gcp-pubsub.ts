import * as timers from "node:timers/promises";
import { Message, PubSub, Subscription } from "@google-cloud/pubsub";
import { FastifyInstance } from "fastify";
import { EventConsumerBuilder } from "./interface";
import { randomDelay } from "./utils";

// Resolves @opentelemetry/api from either NODE_PATH (OTel operator) or the
// pnpm absolute path.  All copies share the same global symbol so the
// registered SDK is always reached.
function getOtelApi() {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require("@opentelemetry/api");
  } catch {
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      return require(
        "/app/node_modules/.pnpm/@opentelemetry+api@1.9.0/node_modules/@opentelemetry/api",
      );
    } catch {
      return null;
    }
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const otel: any = getOtelApi();
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const tracer: any = otel
  ? otel.trace.getTracer("fp-eventbus-gcp-pubsub-consumer")
  : null;

/**
 * GCP Pub/Sub supports
 * 1. flowControl -> this is to support parallel processing
 * 2. need to support delayed message delivery manually (recursive retry for 425)
 */
export const GcpPubSubConsumerBuilder: EventConsumerBuilder = async (
  instance,
) => {
  const pubsub = new PubSub();
  if (
    process.env.EVENT_TOPIC &&
    !process.env.EVENT_SUBSCRIPTION &&
    process.env.APP
  ) {
    instance.log.info(
      "Looking for GCP PubSub Subscription for APP=" + process.env.APP,
    );
    const subs = await pubsub.topic(process.env.EVENT_TOPIC).getSubscriptions();
    instance.log.info("Found Subscriptions=" + subs[0].length);
    for (const sub of subs[0]) {
      if (sub.name.includes(process.env.APP)) {
        process.env.EVENT_SUBSCRIPTION = sub.name;
      }
    }
  }
  if (!process.env.EVENT_SUBSCRIPTION) {
    throw new Error("GCP PubSub needs EVENT_SUBSCRIPTION");
  }
  const subName = process.env.EVENT_SUBSCRIPTION;
  instance.log.info("Attaching to GCP PubSub Subscription=" + subName);

  const runner = new Runner(instance, pubsub, subName);
  runner.init();

  return {
    close: async () => {
      await runner.close();
      await pubsub.close();
    },
  };
};

class Runner {
  public subscription: Subscription | null = null;
  private readonly ctrl = new AbortController();
  private timerRef: NodeJS.Timeout | null = null;
  constructor(
    public readonly instance: FastifyInstance,
    public readonly pubsub: PubSub,
    public readonly subName: string,
  ) {}

  async close(): Promise<void> {
    this.ctrl.abort();
    if (this.timerRef) {
      clearTimeout(this.timerRef);
    }
    if (this.subscription) {
      this.subscription.removeAllListeners();
      return this.subscription.close();
    }
  }

  init() {
    if (this.ctrl.signal.aborted) {
      return;
    }
    this.subscription = this.pubsub.subscription(this.subName, {
      flowControl: {
        allowExcessMessages: false,
        maxMessages:
          parseInt(process.env.EVENT_SUBSCRIPTION_MAX_MESSAGES ?? "10", 10) ||
          10,
      },
    });
    this.subscription.addListener("message", (msg: Message) => {
      this.processMsg(msg, 0);
    });
    this.subscription.on("error", (err) => {
      this.instance.log.error({ tag: "GCP_PUBSUB_RECEIVER_ERROR", err });
      this.subscription?.removeAllListeners();
      this.subscription?.close();
      this.instance.log.warn("waiting for 10 seconds before reconnecting...");
      this.timerRef = setTimeout(() => {
        this.init();
      }, 10000); // retrying after 10 seconds
    });
  }

  async processMsg(msg: Message, attempt: number) {
    if (this.ctrl.signal.aborted) {
      msg.nack();
      return;
    }

    // Extract the W3C trace context from Pub/Sub message attributes and start
    // a CONSUMER span.  Re-inject the context as HTTP headers so the push
    // handler at /gcp-pubsub/process-message continues the same trace tree.
    const attrs = msg.attributes ?? {};
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let ctx: any = null;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let span: any = null;
    if (otel && attrs.traceparent) {
      const parentCtx = otel.propagation.extract(otel.context.active(), attrs);
      span = tracer.startSpan(
        "pubsub.pull.process",
        { kind: otel.SpanKind.CONSUMER },
        parentCtx,
      );
      ctx = otel.trace.setSpan(parentCtx, span);
    }

    const doProcess = async () => {
      const headers: Record<string, string> = {
        "content-type": "application/json",
      };
      // Re-inject the active context as HTTP headers so the Fastify inject()
      // call carries the traceparent into the push handler.
      if (otel && ctx) {
        otel.propagation.inject(otel.context.active(), headers);
      }
      try {
        const resp = await this.instance.inject({
          method: "POST",
          url: "/gcp-pubsub/process-message",
          payload: {
            message: {
              attributes: msg.attributes,
              data: msg.data.toString("base64"),
              messageId: msg.id,
              publishTime: msg.publishTime.toISOString(),
            },
            attempt,
            subscription: this.subName,
          },
          headers,
        });
        if (resp.statusCode >= 200 && resp.statusCode < 300) {
          msg.ack();
        } else if (resp.statusCode === 429 || resp.statusCode === 409) {
          // rate-limited or lock-conflict
          await randomDelay();
          msg.nack();
        } else if (resp.statusCode === 425 && attempt < 2) {
          const parsed = JSON.parse(resp.body);
          const processAfterDelayMs = parsed?.processAfterDelayMs ?? 0;
          if (processAfterDelayMs > 0) {
            await timers.setTimeout(processAfterDelayMs);
          }
          await this.processMsg(msg, attempt + 1);
        } else {
          msg.nack();
        }
      } catch (err) {
        this.instance.log.error({
          tag: "GCP_PUBSUB_RECEIVER_ERROR",
          err,
        });
        msg.nack();
      } finally {
        span?.end();
      }
    };

    if (otel && ctx) {
      await otel.context.with(ctx, doProcess);
    } else {
      await doProcess();
    }
  }
}
