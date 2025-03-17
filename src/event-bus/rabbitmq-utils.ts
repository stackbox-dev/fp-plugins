import Connection from "rabbitmq-client";

export const RABBITMQ_TAG = "" + Math.floor(Math.random() * 1e9);

export async function ensureRabbitMqExchangesAndQueues(
  connection: Connection,
  service: string,
) {
  await connection.exchangeDeclare({
    exchange: `wms.main-exchange`,
    type: "fanout",
    durable: true,
    arguments: {},
    autoDelete: false,
    internal: false,
    passive: false,
  });
  await connection.queueDeclare({
    queue: `wms.queue.${service}`,
    arguments: {
      "x-dead-letter-exchange": `wms.retry-exchange.${service}`,
      "x-queue-type": "classic",
    },
    autoDelete: false,
    durable: true,
    exclusive: false,
    passive: false,
  });
  await connection.queueBind({
    exchange: `wms.main-exchange`,
    queue: `wms.queue.${service}`,
  });
  await connection.exchangeDeclare({
    exchange: `wms.retry-exchange.${service}`,
    type: "direct",
    durable: true,
    arguments: {},
    autoDelete: false,
    internal: false,
    passive: false,
  });
  await connection.queueDeclare({
    queue: `wms.retry-queue.${service}`,
    arguments: {
      "x-message-ttl": 5000,
      "x-dead-letter-exchange": "wms.main-exchange",
      "x-queue-type": "classic",
    },
    autoDelete: false,
    durable: true,
    exclusive: false,
    passive: false,
  });
  await connection.queueBind({
    exchange: `wms.retry-exchange.${service}`,
    queue: `wms.retry-queue.${service}`,
  });
}
