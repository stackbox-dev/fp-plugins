import * as timers from "node:timers/promises";
import { RetryMode, ServiceBusClient } from "@azure/service-bus";
import { EventConsumerBuilder } from "./interface";

async function exponentialDelay(
  baseDelayMs: number,
  maxDelayMs: number,
  attempt: number,
): Promise<void> {
  const delay = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
  await timers.setTimeout(delay);
}

export const AzureServiceBusConsumerBuilder: EventConsumerBuilder = async (
  instance,
  credentials: any,
) => {
  if (!process.env.EVENT_NAMESPACE) {
    throw new Error("Azure ServiceBus needs EVENT_NAMESPACE");
  }
  if (!process.env.EVENT_TOPIC) {
    throw new Error("Azure ServiceBus needs EVENT_TOPIC");
  }
  if (!process.env.EVENT_SUBSCRIPTION) {
    throw new Error("Azure ServiceBus needs EVENT_SUBSCRIPTION");
  }
  const client = new ServiceBusClient(
    process.env.EVENT_NAMESPACE,
    credentials,
    {
      retryOptions: {
        mode: RetryMode.Fixed,
        retryDelayInMs: 10_000,
        timeoutInMs: 5_000,
        maxRetries: 5,
      },
    },
  );
  const receiver = client.createReceiver(
    process.env.EVENT_TOPIC,
    process.env.EVENT_SUBSCRIPTION,
    {
      skipParsingBodyAsJson: true,
      skipConvertingDate: true,
      receiveMode: "peekLock",
    },
  );
  const ctrl = new AbortController();

  const baseDelay = Math.max(
    parseInt(process.env.EVENT_RETRY_BASE_DELAY ?? "0", 10) || 0,
    5_000,
  );
  const maxDelay = Math.max(
    parseInt(process.env.EVENT_RETRY_MAX_DELAY ?? "0", 10) || 0,
    60_000,
  );

  const handler = receiver.subscribe(
    {
      async processMessage(msg) {
        if (ctrl.signal.aborted) {
          await receiver.abandonMessage(msg);
          return;
        }
        if (msg.deliveryCount && msg.deliveryCount > 1) {
          instance.log.warn({
            tag: "AZURE_SERVICE_BUS_RECEIVER_RETRY",
            deliveryCount: msg.deliveryCount,
            messageId: msg.messageId,
          });
          await exponentialDelay(
            baseDelay,
            maxDelay,
            Math.max(msg.deliveryCount - 2, 0),
          );
        }
        try {
          const payload = {
            messageId: msg.messageId,
            body: msg.body,
            scheduledEnqueueTimeUtc: msg.scheduledEnqueueTimeUtc?.toISOString(),
          };
          if (Buffer.isBuffer(payload.body)) {
            payload.body = payload.body.toString("utf8");
          }
          const resp = await instance.inject({
            method: "POST",
            url: "/azure-servicebus/process-message",
            payload,
          });
          if (resp.statusCode >= 200 && resp.statusCode < 300) {
            await receiver.completeMessage(msg);
          } else {
            await receiver.abandonMessage(msg);
          }
        } catch (err) {
          instance.log.error({
            tag: "AZURE_SERVICE_BUS_RECEIVER_ERROR",
            err: err,
          });
          await receiver.abandonMessage(msg);
        }
      },
      async processError(args) {
        instance.log.error({
          tag: "AZURE_SERVICE_BUS_RECEIVER_ERROR",
          err: args.error,
          entityPath: args.entityPath,
        });
      },
    },
    {
      abortSignal: ctrl.signal,
      autoCompleteMessages: false,
      maxConcurrentCalls:
        parseInt(
          process.env.EVENT_SUBSCRIPTION_MAX_CONCURRENT_CALLS ?? "10",
          10,
        ) || 10,
    },
  );
  instance.log.info(
    "Attached to Azure ServiceBus Subscription=" +
      process.env.EVENT_SUBSCRIPTION,
  );
  return {
    close: async () => {
      ctrl.abort();
      await handler.close();
      await client.close();
    },
  };
};
