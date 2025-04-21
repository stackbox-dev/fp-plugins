# @stackbox/fp-plugins

Fastify plugins for Stackbox applications.

## Installation

```bash
npm install @stackbox/fp-plugins
```

## Overview

This package provides a collection of Fastify plugins designed to enhance Stackbox applications. These plugins extend Fastify's functionality while maintaining its performance and developer experience.

## Available Plugins

### Event Bus Plugin

A Fastify plugin that integrates an event bus system into your application, enabling efficient event-driven communication between different parts of your application.

#### Supported Message Brokers

- **RabbitMQ** (`rabbitmq`)
- **Google Cloud Pub/Sub** (`gcp-pubsub`)
- **Azure Service Bus** (`azure-servicebus`)
- **In-process** (`in-process`) - for development and testing

#### Configuration

```typescript
app.register(Plugins.EventBus, {
  // Required: type of message broker to use
  busType: "rabbitmq" | "gcp-pubsub" | "azure-servicebus" | "in-process",

  // Required for gcp-pubsub and azure-servicebus
  topic: "your-topic-name",

  // Required for azure-servicebus
  namespace: "your-namespace",

  // Required: define event handlers
  handlers: [
    {
      file: "module-name",
      handlers: {
        "event-name": async function (msg, req) {
          // Handle the event
        },
      },
    },
  ],

  // Required: message validation function
  validateMsg: (event, payload, req) => {
    // Validate the message
  },

  // Required: error processing function
  processError: (err, ctx) => {
    // Process the error
    return { err, status: 500 };
  },

  // Optional: disable the /event-bus/publish/:event route
  disableEventPublishRoute: false,

  // Optional: control concurrency of event handlers
  actionConcurrency: 1,

  // Optional: Prometheus registry for metrics
  registry: new Registry(),
});
```

#### Environment Variables

##### RabbitMQ Configuration

- `RABBITMQ_URL`: RabbitMQ connection URL
- `K_SERVICE`: Service name for queue naming

##### Google Cloud Pub/Sub Configuration

- `EVENT_TOPIC`: GCP Pub/Sub topic name
- `EVENT_SUBSCRIPTION`: GCP Pub/Sub subscription name
- `EVENT_SUBSCRIPTION_MAX_MESSAGES`: Maximum concurrent messages (default: 10)

##### Azure Service Bus Configuration

- `EVENT_NAMESPACE`: Azure Service Bus namespace
- `EVENT_TOPIC`: Azure Service Bus topic
- `EVENT_SUBSCRIPTION`: Azure Service Bus subscription
- `EVENT_SUBSCRIPTION_MAX_CONCURRENT_CALLS`: Maximum concurrent calls (default: 10)

##### Retry Configuration

- `EVENT_RETRY_BASE_DELAY`: Base delay for exponential backoff in ms (default: 5000)
- `EVENT_RETRY_MAX_DELAY`: Maximum delay for retries in ms (default: 60000)

#### Usage Example

```typescript
import { EventBus, Plugins } from "@stackbox/fp-plugins";

const app = fastify();

// Register event bus
app.register(Plugins.EventBus, {
  busType: "rabbitmq",
  validateMsg: (event, payload) => {
    // Validate event and payload
    console.log(`Validating message: ${event}`);
  },
  processError: (err, ctx) => {
    console.error(`Error processing message: ${err.message}`);
    return { err, status: 500 };
  },
  handlers: [
    {
      file: "orderModule",
      handlers: {
        "order.created": async function (msg, req) {
          // Process order created event
          console.log(`Processing order: ${msg.data.orderId}`);
        },
        "order.cancelled": async function (msg, req) {
          // Process order cancelled event
          console.log(`Processing cancelled order: ${msg.data.orderId}`);
        },
      },
    },
  ],
});

// Publishing events
app.post("/create-order", async (req, reply) => {
  // Create order logic...

  // Publish event
  req.EventBus.publish("order.created", {
    orderId: "order-123",
    customerId: "customer-456",
  });

  return { success: true };
});

// Delayed events (process after specified milliseconds)
app.post("/schedule-reminder", async (req, reply) => {
  req.EventBus.publish(
    "reminder.send",
    {
      userId: "user-123",
      message: "Reminder to complete your profile",
    },
    3600000,
  ); // Process after 1 hour

  return { scheduled: true };
});
```

#### Event Consumer

To consume events from external services, use the `CreateEventConsumer` function:

```typescript
import { CreateEventConsumer } from "@stackbox/fp-plugins";

// Create consumer that matches your EventBus busType
const consumer = await CreateEventConsumer(app, "rabbitmq");

// Close consumer when application shuts down
app.addHook("onClose", async () => {
  await consumer.close();
});
```

### File Store Plugin

A plugin that provides an abstraction for file storage operations across different providers:

- Local filesystem
- AWS S3
- Google Cloud Storage
- Azure Blob Storage
- MinIO

#### Configuration

```typescript
app.register(Plugins.FileStore, {
  // Required: type of storage provider to use
  type: "local" | "s3" | "gcs" | "azureBlob" | "minio",
});
```

#### Environment Variables

##### Local File System

- `LOCAL_STORAGE_DIR`: Directory for file storage (default: system temp directory)

##### AWS S3

- `AWS_S3_REGION`: AWS region (default: "us-east-1")
- `S3_BUCKET`: S3 bucket name (required)
- Standard AWS authentication environment variables

The plugin also supports other AWS authentication methods including:
- ECS/EC2 instance roles
- AWS IAM roles for service accounts (IRSA)
- Web identity providers
- AWS profiles

##### Google Cloud Storage

- `STORAGE_BUCKET`: GCS bucket name (required)
- Standard GCP authentication environment variables

The plugin also supports other GCP authentication methods including:
- GKE Workload Identity
- Compute Engine service accounts
- GCP Application Default Credentials (ADC)
- Service account key files (not recommended for production)

##### Azure Blob Storage

- `AZURE_STORAGE_ACCOUNT_URL`: Azure storage account URL (required)
- `AZURE_STORAGE_CONTAINER`: Azure storage container name (required)
- Standard Azure authentication environment variables

The plugin also supports other Azure authentication methods including:
- Managed Identities for Azure resources
- Azure AD workload identity
- Azure service principals
- Azure DefaultAzureCredential chain

##### MinIO

- `MINIO_ENDPOINT`: MinIO server endpoint (required)
- `MINIO_ACCESS_KEY_ID`: MinIO access key (required)
- `MINIO_SECRET_ACCESS_KEY`: MinIO secret key (required)
- `MINIO_REGION`: MinIO region (default: "us-east-1")
- `MINIO_BUCKET`: MinIO bucket name (required)

#### FileStore Interface

The plugin provides a `FileStore` interface with the following methods:

```typescript
interface FileStore {
  exists(filepath: string): Promise<boolean>;
  save(
    filepath: string,
    contentType: string,
    data: string | Buffer,
  ): Promise<void>;
  getAsBuffer(filepath: string): Promise<Buffer>;
  getAsStream(filepath: string): Promise<NodeJS.ReadableStream>;
  copyFromStream(
    filepath: string,
    contentType: string,
    stream: stream.Readable,
  ): Promise<void>;
  copyFromLocalFile(
    filepath: string,
    contentType: string,
    localFilepath: string,
  ): Promise<void>;
}
```

#### Usage Example

```typescript
import { Plugins } from "@stackbox/fp-plugins";
import { fastify } from "fastify";

const app = fastify();

// Register file store plugin
app.register(Plugins.FileStore, {
  type: "s3", // Choose the appropriate storage type
});

// Using the file store
app.post("/upload", async (request, reply) => {
  const { filepath, contentType, data } = request.body;

  // Check if file exists
  const exists = await request.server.FileStore.exists(filepath);

  // Save file
  await request.server.FileStore.save(filepath, contentType, data);

  // Get file as buffer
  const fileContent = await request.server.FileStore.getAsBuffer(filepath);

  // Get file as stream
  const fileStream = await request.server.FileStore.getAsStream(filepath);

  // Copy from stream
  await request.server.FileStore.copyFromStream(
    filepath,
    contentType,
    someReadableStream,
  );

  // Copy from local file
  await request.server.FileStore.copyFromLocalFile(
    filepath,
    contentType,
    "/path/to/local/file",
  );

  return { success: true };
});
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[MIT License](LICENSE)
