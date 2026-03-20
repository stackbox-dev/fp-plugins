import * as AzureIden from "@azure/identity";
import {
  RetryMode,
  ServiceBusClient,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
} from "@azure/service-bus";
import { safeCloseAll } from "../utils";
import { EventConsumerBuilder } from "./interface";
import { exponentialDelay, randomDelay } from "./utils";

export const AzureServiceBusConsumerBuilder: EventConsumerBuilder = async (
  instance,
) => {
  if (!process.env.EVENT_TOPIC) {
    throw new Error("Azure ServiceBus needs EVENT_TOPIC");
  }
  if (!process.env.EVENT_SUBSCRIPTION) {
    throw new Error("Azure ServiceBus needs EVENT_SUBSCRIPTION");
  }

  const connectionString = process.env.AZURE_SERVICEBUS_CONNECTION_STRING;
  if (!connectionString && !process.env.EVENT_NAMESPACE) {
    throw new Error(
      "Azure ServiceBus needs either AZURE_SERVICEBUS_CONNECTION_STRING or EVENT_NAMESPACE",
    );
  }
  if (connectionString && !connectionString.includes("Endpoint=")) {
    throw new Error(
      "AZURE_SERVICEBUS_CONNECTION_STRING must contain 'Endpoint=' — check the format",
    );
  }

  // All validation passed — safe to create resources
  const retryOptions = {
    mode: RetryMode.Fixed,
    retryDelayInMs: 10_000,
    timeoutInMs: 5_000,
    maxRetries: 5,
  };
  const client = connectionString
    ? new ServiceBusClient(connectionString, { retryOptions })
    : new ServiceBusClient(
        process.env.EVENT_NAMESPACE!,
        new AzureIden.DefaultAzureCredential({}),
        { retryOptions },
      );

  let receiver: ServiceBusReceiver;
  try {
    receiver = client.createReceiver(
      process.env.EVENT_TOPIC,
      process.env.EVENT_SUBSCRIPTION,
      {
        skipParsingBodyAsJson: true,
        skipConvertingDate: true,
        receiveMode: "peekLock",
      },
    );
  } catch (err) {
    await client.close();
    throw err;
  }

  const ctrl = new AbortController();

  function safeAbandon(msg: ServiceBusReceivedMessage) {
    return receiver.abandonMessage(msg).catch((err) => {
      instance.log.warn({
        tag: "AZURE_SERVICE_BUS_ABANDON_FAILED",
        err,
        messageId: msg.messageId,
      });
    });
  }

  const handler = receiver.subscribe(
    {
      async processMessage(msg) {
        if (ctrl.signal.aborted) {
          // Don't attempt abandonMessage during shutdown — receiver may already
          // be closing. The broker will redeliver unacked messages after the
          // connection drops.
          return;
        }
        if (msg.deliveryCount && msg.deliveryCount > 1) {
          instance.log.warn({
            tag: "AZURE_SERVICE_BUS_RECEIVER_RETRY",
            deliveryCount: msg.deliveryCount,
            messageId: msg.messageId,
          });
          await exponentialDelay(Math.max(msg.deliveryCount - 2, 0), {
            signal: ctrl.signal,
          });
          if (ctrl.signal.aborted) return;
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
            headers: {
              "content-type": "application/json",
            },
          });
          if (resp.statusCode >= 200 && resp.statusCode < 300) {
            await receiver.completeMessage(msg);
          } else if (resp.statusCode === 429 || resp.statusCode === 409) {
            await randomDelay(undefined, ctrl.signal);
            await safeAbandon(msg);
          } else if (resp.statusCode === 425) {
            instance.log.error({
              tag: "AZURE_SERVICE_BUS_DELAYED_MESSAGE",
              payload,
            });
            await safeAbandon(msg);
          } else {
            await safeAbandon(msg);
          }
        } catch (err) {
          if ((err as Error).name !== "AbortError") {
            instance.log.error({
              tag: "AZURE_SERVICE_BUS_MSG_PROCESS_ERROR",
              err,
            });
          }
          // Skip abandonMessage during shutdown
          if (!ctrl.signal.aborted) {
            await safeAbandon(msg);
          }
        }
      },
      async processError(args) {
        instance.log.error({
          tag: "AZURE_SERVICE_BUS_TRANSPORT_ERROR",
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
      // Abort BEFORE closing: prevents shutdown from blocking on broker
      // round-trips. In-flight messages get redelivered by the broker.
      ctrl.abort();
      const errors = await safeCloseAll(
        () => handler.close(),
        () => receiver.close(),
        () => client.close(),
      );
      if (errors.length > 0) {
        instance.log.error({ tag: "AZURE_SERVICE_BUS_CLOSE_ERRORS", errors });
      }
    },
  };
};
