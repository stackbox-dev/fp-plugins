import { Connection, ConsumerStatus } from "rabbitmq-client";
import {
  RABBITMQ_TAG,
  ensureRabbitMqExchangesAndQueues,
  getServicePrefix,
} from "../rabbitmq-utils";
import { EventConsumerBuilder } from "./interface";
import { randomDelay } from "./utils";

/**
 * RabbitMq supports
 * 1. prefetchCount -> this is to support parallel processing
 * 2. concurrency -> this is to support parallel processing
 * 3. ConsumerStatus -> requeue and drop messages appropriately
 */
export const RabbitMqServiceBusConsumerBuilder: EventConsumerBuilder = async (
  instance,
) => {
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
    async (msg) => {
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
        } else if (resp.statusCode === 429 || resp.statusCode === 409) {
          await randomDelay();
          // rate-limited or lock-conflict
          return ConsumerStatus.REQUEUE;
        } else if (resp.statusCode === 425) {
          // delayed message. requeue with delay to avoid tight loop
          await randomDelay();
          return ConsumerStatus.REQUEUE;
        } else if (resp.statusCode >= 500 && resp.statusCode < 600) {
          // transient server error, retry
          await randomDelay();
          return ConsumerStatus.REQUEUE;
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
      await connection.close();
    },
  };
};
