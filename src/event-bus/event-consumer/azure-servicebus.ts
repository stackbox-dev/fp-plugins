import * as AzureIden from "@azure/identity";
import { RetryMode, ServiceBusClient } from "@azure/service-bus";
import { EventConsumerBuilder } from "./interface";
import { exponentialDelay, randomDelay } from "./utils";

/**
 * Azure ServiceBus supports
 * 1. scheduledEnqueueTimeUtc -> this is to support delayed message delivery
 * 2. peekLock -> this is to support message processing without contention
 * 3. maxConcurrentCalls -> this is to support parallel processing
 */
export const AzureServiceBusConsumerBuilder: EventConsumerBuilder = async (
  instance,
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
    new AzureIden.DefaultAzureCredential({}),
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
          await exponentialDelay(Math.max(msg.deliveryCount - 2, 0));
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
          } else if (resp.statusCode === 429 || resp.statusCode === 409) {
            // rate-limited or lock-conflict
            await randomDelay();
            await receiver.abandonMessage(msg);
          } else if (resp.statusCode === 425) {
            // delayed message
            // ideally it shouldn't come here because azure service bus already
            // supports delayed message receiving
            instance.log.error({
              tag: "AZURE_SERVICE_BUS_DELAYED_MESSAGE",
              payload,
            });
            await receiver.abandonMessage(msg);
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
