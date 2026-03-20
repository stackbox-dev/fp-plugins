import * as timers from "node:timers/promises";
import { jetstream } from "@nats-io/jetstream";
import { connect } from "@nats-io/transport-node";
import { safeCloseAll } from "../utils";
import { EventConsumerBuilder } from "./interface";
import { randomDelay } from "./utils";

export const NatsJetStreamConsumerBuilder: EventConsumerBuilder = async (
  instance,
) => {
  if (!process.env.NATS_SERVERS) {
    throw new Error("NATS JetStream consumer requires NATS_SERVERS");
  }
  if (!process.env.NATS_STREAM) {
    throw new Error("NATS JetStream consumer requires NATS_STREAM");
  }
  if (!process.env.NATS_CONSUMER) {
    throw new Error("NATS JetStream consumer requires NATS_CONSUMER");
  }

  const nc = await connect({
    servers: process.env.NATS_SERVERS.split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    name: `${process.env.K_SERVICE ?? "fp-eventbus"}-consumer`,
  });

  // Monitor connection status for visibility (matches publisher pattern)
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === "error" || s.type === "disconnect") {
        instance.log.error({
          tag: "NATS_CONSUMER_CONNECTION_STATUS",
          type: s.type,
          data: s.data,
        });
      } else if (s.type === "reconnect") {
        instance.log.info({
          tag: "NATS_CONSUMER_CONNECTION_STATUS",
          type: s.type,
        });
      }
    }
  })().catch((err) => {
    instance.log.error({ tag: "NATS_CONSUMER_STATUS_MONITOR_ERROR", err });
  });

  const js = jetstream(nc);
  const streamName = process.env.NATS_STREAM;
  const consumerName = process.env.NATS_CONSUMER;
  const ctrl = new AbortController();

  let consumeHandle: { stop(): void } | null = null;
  let reconnectAttempt = 0;
  const MAX_RECONNECT_ATTEMPTS = 20;

  // Iterative consume loop — a single promise that close() can await.
  // Replaces the previous recursive consumeLoop() pattern to avoid
  // unbounded promise chain growth on long-lived consumers.
  const loopPromise = (async () => {
    while (!ctrl.signal.aborted) {
      try {
        const consumer = await js.consumers.get(streamName, consumerName);
        const messages = await consumer.consume();
        consumeHandle = messages;
        reconnectAttempt = 0;

        for await (const msg of messages) {
          if (ctrl.signal.aborted) {
            // Don't nak during shutdown — connection may already be draining.
            // JetStream will redeliver unacked messages after the connection drops.
            break;
          }
          await processMessage(msg);
        }

        // Iterator completed without error — stream/consumer may have been deleted.
        // Reconnect with backoff to prevent CPU spinning.
        if (!ctrl.signal.aborted) {
          instance.log.warn({ tag: "NATS_JETSTREAM_CONSUME_ENDED" });
          await timers.setTimeout(2000, undefined, { signal: ctrl.signal });
        }
      } catch (err) {
        if (ctrl.signal.aborted) return;
        if ((err as Error).name === "AbortError") return;

        reconnectAttempt++;
        if (reconnectAttempt > MAX_RECONNECT_ATTEMPTS) {
          instance.log.error({
            tag: "NATS_JETSTREAM_MAX_RECONNECT_EXCEEDED",
            err,
            attempts: reconnectAttempt,
          });
          return;
        }

        const backoff = Math.min(
          5000 * Math.pow(2, reconnectAttempt - 1),
          30_000,
        );
        instance.log.error({
          tag: "NATS_JETSTREAM_CONSUME_ERROR",
          err,
          attempt: reconnectAttempt,
          maxAttempts: MAX_RECONNECT_ATTEMPTS,
          nextRetryMs: backoff,
        });

        try {
          await timers.setTimeout(backoff, undefined, { signal: ctrl.signal });
        } catch (timerErr) {
          if ((timerErr as Error).name === "AbortError") return;
          instance.log.error({
            tag: "NATS_JETSTREAM_RECONNECT_TIMER_ERROR",
            err: timerErr,
          });
          return;
        }
      }
    }
  })();

  async function processMessage(msg: {
    data: Uint8Array;
    ack(): void;
    nak(): void;
    seq: number;
  }) {
    try {
      const bodyStr = Buffer.from(msg.data).toString("utf8");
      const resp = await instance.inject({
        method: "POST",
        url: "/nats-jetstream/process-message",
        payload: {
          messageId: "" + msg.seq,
          body: bodyStr,
        },
        headers: {
          "content-type": "application/json",
        },
      });

      if (resp.statusCode >= 200 && resp.statusCode < 300) {
        if (!ctrl.signal.aborted) msg.ack();
      } else if (resp.statusCode === 429 || resp.statusCode === 409) {
        await randomDelay(undefined, ctrl.signal);
        if (!ctrl.signal.aborted) msg.nak();
      } else if (resp.statusCode === 425) {
        await randomDelay(undefined, ctrl.signal);
        if (!ctrl.signal.aborted) msg.nak();
      } else if (resp.statusCode >= 500 && resp.statusCode < 600) {
        if (!ctrl.signal.aborted) msg.nak();
      } else {
        instance.log.warn({
          tag: "NATS_JETSTREAM_MESSAGE_UNEXPECTED_STATUS",
          seq: msg.seq,
          statusCode: resp.statusCode,
          body: resp.body,
        });
        if (!ctrl.signal.aborted) msg.nak();
      }
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        instance.log.error({
          tag: "NATS_JETSTREAM_MSG_PROCESS_ERROR",
          err,
        });
      }
      // Don't nak during shutdown — connection may already be draining
      if (!ctrl.signal.aborted) msg.nak();
    }
  }

  instance.log.info(
    `Attached to NATS JetStream Stream=${streamName} Consumer=${consumerName}`,
  );

  return {
    close: async () => {
      ctrl.abort();
      consumeHandle?.stop();
      // Wait for in-flight processMessage to complete before draining
      await loopPromise;
      const errors = await safeCloseAll(() => nc.drain());
      if (errors.length > 0) {
        instance.log.error({
          tag: "NATS_JETSTREAM_CONSUMER_CLOSE_ERRORS",
          errors,
        });
      }
    },
  };
};
