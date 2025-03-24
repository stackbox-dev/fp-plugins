const fastify = require("fastify");
const { CreateEventConsumer, EventBus, Plugins } = require("../dist");

async function main() {
  process.env.TOPIC = "projects/sbx-stag/topics/test-topic";
  process.env.EVENT_SUBSCRIPTION =
    "projects/sbx-stag/subscriptions/test-topic-sub";

  const app = fastify({ logger: true });
  CreateEventConsumer(app, "gcp-pubsub").catch(console.error);
  app.register(Plugins.FileStore, {
    type: "local",
  });
  app.register(Plugins.EventBus, {
    busType: "gcp-pubsub",
    topic: process.env.TOPIC,
    validateMsg: (event, payload) => {
      console.log(new Date(), "Validating message", event, payload);
    },
    processError: (err, ctx) => {
      console.error(new Date(), "Error processing message", err, ctx);
      return { err, status: 500 };
    },
    handlers: [
      {
        file: "app1",
        handlers: {
          event1: async (msg, req) => {
            console.log(new Date(), "handling event1", msg.data);
          },
        },
      },
    ],
  });

  app.post("/event-bus/publish", async (req) => {
    req.EventBus.publish(
      "event1",
      { message: "hello", time: Date.now() },
      60_000,
    );
    // req.EventBus.publish("event1", { message: "world" });
    return { status: "ok" };
  });

  app.listen({ port: 3000 });
}

main().catch(console.error);
