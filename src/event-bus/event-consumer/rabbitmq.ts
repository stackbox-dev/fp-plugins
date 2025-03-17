import { Connection, ConsumerStatus } from "rabbitmq-client";
import {
  RABBITMQ_TAG,
  ensureRabbitMqExchangesAndQueues,
} from "../rabbitmq-utils";
import { EventConsumerBuilder } from "./interface";

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

  const sub = connection.createConsumer(
    {
      queue: `wms.queue.${service}`,
      queueOptions: {
        passive: true,
      },
      qos: {
        prefetchCount: 10,
      },
      concurrency: 10,
      consumerTag: `wms.consumer.${service}.${RABBITMQ_TAG}`,
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
        } else {
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
      await sub.close();
      ctrl.abort();
      await connection.close();
    },
  };
};
