import { Connection } from "rabbitmq-client";

export const RABBITMQ_TAG = "" + Math.floor(Math.random() * 1e9);

/**
 * Extracts the prefix from a service name (part before the first hyphen).
 * e.g., "wms-cincout" → "wms", "xyz-service" → "xyz", "noprefix" → "default"
 */
export function getServicePrefix(service: string): string {
  const hyphenIndex = service.indexOf("-");
  return hyphenIndex > 0 ? service.substring(0, hyphenIndex) : "default";
}

export async function ensureRabbitMqExchangesAndQueues(
  connection: Connection,
  service: string,
) {
  const prefix = getServicePrefix(service);
  await connection.exchangeDeclare({
    exchange: `${prefix}.main-exchange`,
    type: "fanout",
    durable: true,
    arguments: {},
    autoDelete: false,
    internal: false,
    passive: false,
  });
  await connection.queueDeclare({
    queue: `${prefix}.queue.${service}`,
    arguments: {
      "x-dead-letter-exchange": `${prefix}.retry-exchange.${service}`,
      "x-dead-letter-routing-key": "retry", // Fixed routing key for DLX
      "x-queue-type": "classic",
    },
    autoDelete: false,
    durable: true,
    exclusive: false,
    passive: false,
  });
  await connection.queueBind({
    exchange: `${prefix}.main-exchange`,
    queue: `${prefix}.queue.${service}`,
  });
  await connection.exchangeDeclare({
    exchange: `${prefix}.retry-exchange.${service}`,
    type: "direct",
    durable: true,
    arguments: {},
    autoDelete: false,
    internal: false,
    passive: false,
  });
  await connection.queueDeclare({
    queue: `${prefix}.retry-queue.${service}`,
    arguments: {
      "x-message-ttl": 5000,
      "x-dead-letter-exchange": "", // default exchange routes by queue name
      "x-dead-letter-routing-key": `${prefix}.queue.${service}`,
      "x-queue-type": "classic",
    },
    autoDelete: false,
    durable: true,
    exclusive: false,
    passive: false,
  });
  await connection.queueBind({
    exchange: `${prefix}.retry-exchange.${service}`,
    queue: `${prefix}.retry-queue.${service}`,
    routingKey: "retry", // Must match x-dead-letter-routing-key from main queue
  });
  // Dead Letter Queue for messages that exceed max retries (manual retry)
  await connection.queueDeclare({
    queue: `${prefix}.dlq.${service}`,
    arguments: {
      "x-queue-type": "classic",
    },
    autoDelete: false,
    durable: true,
    exclusive: false,
    passive: false,
  });
}
