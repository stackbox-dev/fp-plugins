import { AsyncMessage, Connection, ConsumerStatus } from "rabbitmq-client";
import {
  RABBITMQ_TAG,
  ensureRabbitMqExchangesAndQueues,
  getServicePrefix,
} from "../rabbitmq-utils";
import { EventConsumerBuilder } from "./interface";
import { randomDelay } from "./utils";

const MAX_RETRY_COUNT = 10;

/**
 * Extract retry count from x-death header added by RabbitMQ DLX.
 * Only counts rejections (consumer drops), not TTL expirations from retry queue.
 * x-death entries look like: { queue, reason, count, ... }
 * - reason "rejected" = consumer nacked/dropped the message
 * - reason "expired" = TTL expired in retry queue (not a retry attempt)
 */
function getRetryCount(msg: AsyncMessage): number {
  const xDeath = msg.headers?.["x-death"];
  if (!Array.isArray(xDeath) || xDeath.length === 0) {
    return 0;
  }
  // Only count "rejected" entries (actual consumer rejections)
  // Don't count "expired" entries from retry queue TTL
  return xDeath
    .filter((death) => death.reason === "rejected")
    .reduce((total, death) => total + (death.count || 0), 0);
}

/**
 * RabbitMq supports
 * 1. prefetchCount -> this is to support parallel processing
 * 2. concurrency -> this is to support parallel processing
 * 3. ConsumerStatus -> requeue and drop messages appropriately
 */
export const RabbitMqServiceBusConsumerBuilder: EventConsumerBuilder = async (
  instance,
) => {
  // Skip consumer creation if no handlers are registered
  if ((instance as any)._hasEventHandlers === false) {
    instance.log.info("No event handlers registered, skipping RabbitMQ consumer");
    return {
      close: async () => {},
    };
  }

  if (!process.env.RABBITMQ_URL) {
    throw new Error("RabbitMq requires RABBITMQ_URL");
  }
  if (!process.env.K_SERVICE) {
    throw new Error("RabbitMq requires K_SERVICE");
  }
  const connection = new Connection(process.env.RABBITMQ_URL);
  await ensureRabbitMqExchangesAndQueues(connection, process.env.K_SERVICE);

  const ctrl = new AbortController();
  const service = process.env.K_SERVICE;
  const prefix = getServicePrefix(service);

  // Publisher for DLQ messages
  const dlqPublisher = connection.createPublisher({
    confirm: true,
    maxAttempts: 3,
  });

  const sub = connection.createConsumer(
    {
      queue: `${prefix}.queue.${service}`,
      queueOptions: {
        passive: true,
      },
      qos: {
        prefetchCount: 10,
      },
      concurrency: 10,
      consumerTag: `${prefix}.consumer.${service}.${RABBITMQ_TAG}`,
    },
    async (msg: AsyncMessage) => {
      if (ctrl.signal.aborted) {
        return ConsumerStatus.REQUEUE;
      }
      try {
        const payload = {
          messageId: msg.messageId,
          body: msg.body,
        };
        if (Buffer.isBuffer(payload.body)) {
          payload.body = payload.body.toString("utf8");
        }
        const resp = await instance.inject({
          method: "POST",
          url: "/rabbitmq/process-message",
          payload,
        });
        if (resp.statusCode >= 200 && resp.statusCode < 300) {
          return ConsumerStatus.ACK;
        }

        // Check retry count before dead-lettering
        const retryCount = getRetryCount(msg);
        if (retryCount >= MAX_RETRY_COUNT) {
          instance.log.error({
            tag: "RABBITMQ_MESSAGE_MAX_RETRIES_EXCEEDED",
            messageId: msg.messageId,
            retryCount,
            statusCode: resp.statusCode,
            body: resp.body,
          });
          // Store in DLQ for manual retry before ACKing
          await dlqPublisher.send(
            {
              routingKey: `${prefix}.dlq.${service}`,
              contentType: "application/json",
              headers: {
                ...msg.headers,
                "x-original-queue": `${prefix}.queue.${service}`,
                "x-final-status-code": resp.statusCode,
                "x-final-retry-count": retryCount,
              },
            },
            msg.body,
          );
          // ACK to remove from queue permanently (don't dead-letter again)
          return ConsumerStatus.ACK;
        }

        if (resp.statusCode === 429 || resp.statusCode === 409) {
          // rate-limited or lock-conflict, use dead-letter retry
          return ConsumerStatus.DROP;
        } else if (resp.statusCode === 425) {
          // delayed message, use local sleep since delay may exceed DLX TTL
          await randomDelay();
          return ConsumerStatus.REQUEUE;
        } else if (resp.statusCode >= 500 && resp.statusCode < 600) {
          // transient server error, use dead-letter retry
          return ConsumerStatus.DROP;
        } else {
          instance.log.warn({
            tag: "RABBITMQ_MESSAGE_DROPPED",
            messageId: msg.messageId,
            statusCode: resp.statusCode,
            body: resp.body,
          });
          return ConsumerStatus.DROP;
        }
      } catch (err) {
        // Check retry count before dead-lettering on exception
        const retryCount = getRetryCount(msg);
        if (retryCount >= MAX_RETRY_COUNT) {
          instance.log.error({
            tag: "RABBITMQ_MESSAGE_MAX_RETRIES_EXCEEDED",
            messageId: msg.messageId,
            retryCount,
            err,
          });
          // Store in DLQ for manual retry before ACKing
          await dlqPublisher.send(
            {
              routingKey: `${prefix}.dlq.${service}`,
              contentType: "application/json",
              headers: {
                ...msg.headers,
                "x-original-queue": `${prefix}.queue.${service}`,
                "x-final-error": err instanceof Error ? err.message : String(err),
                "x-final-retry-count": retryCount,
              },
            },
            msg.body,
          );
          return ConsumerStatus.ACK;
        }
        instance.log.error({
          tag: "RABBITMQ_CONSUMER_ERROR",
          err: err,
        });
        return ConsumerStatus.DROP;
      }
    },
  );
  instance.log.info(
    "Attached to RabbitMQ for Service=" + process.env.K_SERVICE,
  );
  return {
    close: async () => {
      ctrl.abort();
      await sub.close();
      await dlqPublisher.close();
      await connection.close();
    },
  };
};
