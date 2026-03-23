# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

- **Build**: `pnpm run build` - Cleans dist and compiles TypeScript
- **Test**: `pnpm test` - Runs Jest test suite
- **Test with coverage**: `pnpm run test:coverage` - Runs tests with coverage report
- **Test RabbitMQ**: `pnpm run test:rabbitmq` - RabbitMQ integration tests (requires Docker, 2min timeout)
- **Test integration**: `pnpm run test:integration` - Event bus integration tests with testcontainers (3min timeout)
- **Test all**: `pnpm run test:all` - All tests including integration (3min timeout)
- **Format code**: `pnpm run pretty` - Formats code with Prettier
- **Clean**: `pnpm run clean` - Removes dist directory
- **Transpile**: `pnpm run transpile` - TypeScript compilation only

## Project Architecture

This is a Fastify plugin library (`@stackbox-dev/fp-plugins`) that provides reusable plugins for Stackbox applications. The architecture follows a modular plugin-based design:

### Core Structure
- **Main exports** (`src/index.ts`): Exposes `Plugins.EventBus` and `Plugins.FileStore`
- **Event Bus Plugin** (`src/event-bus/`): Message broker abstraction supporting RabbitMQ, GCP Pub/Sub, Azure Service Bus, NATS JetStream, and local in-process messaging
- **File Store Plugin** (`src/file-store.ts`): Cloud storage abstraction supporting AWS S3, GCS, Azure Blob Storage, MinIO, and local filesystem

### Event Bus System
- Uses a factory pattern to instantiate different message brokers based on `busType` configuration
- Provides event consumer functionality for external event processing
- Includes built-in `/event-bus/publish/:event` endpoint (can be disabled)
- Supports delayed event processing and retry mechanisms with exponential backoff
- Optional Prometheus metrics via `prom-client` (pass a `Registry` in options)

### File Store System
- Implements a common `FileStore` interface across all storage providers
- Supports both streaming and buffer-based file operations
- Uses environment variables for provider-specific configuration
- Handles authentication through provider-specific credential chains (AWS IAM, GCP ADC, Azure Managed Identity)

### Event Consumer System
- Standalone consumer module (`src/event-bus/event-consumer/`) for processing events outside of Fastify
- Per-provider implementations: RabbitMQ, GCP Pub/Sub, Azure Service Bus, NATS JetStream
- Uses `EventConsumerBuilder` factory pattern — takes a `FastifyInstance`, returns an `EventConsumer` with `close()`
- Includes shared retry utilities with exponential backoff in `utils.ts`

### Plugin Registration
Both plugins are registered as Fastify plugins using `fastify-plugin` and follow the standard Fastify plugin lifecycle. They decorate the Fastify instance with their respective interfaces (`EventBus` and `FileStore`).

## Testing
Tests are configured with Jest and ts-jest, located in the `src/` directory with `.spec.ts` extension. Coverage reports are generated in the `coverage/` directory.

## Build Configuration
- TypeScript configuration uses `tsconfig.build.json` for production builds
- Excludes test files (`**/*.spec.ts`) from production builds
- Outputs to `dist/` directory with type definitions

## Gotchas

- **Pre-commit hooks**: Husky + lint-staged auto-runs Prettier on staged `.js/.ts/.json/.md` files at commit time
- **Lazy require in event-bus factory**: Provider modules are loaded via `require()` (not static imports) so only the selected provider's dependencies are needed at runtime
- **Fastify augmentation**: `src/types.ts` augments `FastifyRequest` with `EventBus` — this is imported as a side-effect
- **`tsconfig.build.json` skipLibCheck**: Set to `true` as a workaround for `@nats-io/jetstream` subpath import `.d.ts` files requiring `moduleResolution: "node16"+`
- **Default test command excludes integration tests**: `pnpm test` skips `rabbitmq.spec.ts` and `event-bus.integration.spec.ts` — use `pnpm run test:all` to include them