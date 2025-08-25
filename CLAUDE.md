# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

- **Build**: `pnpm run build` - Cleans dist and compiles TypeScript
- **Test**: `pnpm test` - Runs Jest test suite
- **Test with coverage**: `pnpm run test:coverage` - Runs tests with coverage report
- **Format code**: `pnpm run pretty` - Formats code with Prettier
- **Clean**: `pnpm run clean` - Removes dist directory
- **Transpile**: `pnpm run transpile` - TypeScript compilation only

## Project Architecture

This is a Fastify plugin library (`@stackbox-dev/fp-plugins`) that provides reusable plugins for Stackbox applications. The architecture follows a modular plugin-based design:

### Core Structure
- **Main exports** (`src/index.ts`): Exposes `Plugins.EventBus` and `Plugins.FileStore`
- **Event Bus Plugin** (`src/event-bus/`): Message broker abstraction supporting RabbitMQ, GCP Pub/Sub, Azure Service Bus, and local in-process messaging
- **File Store Plugin** (`src/file-store.ts`): Cloud storage abstraction supporting AWS S3, GCS, Azure Blob Storage, MinIO, and local filesystem

### Event Bus System
- Uses a factory pattern to instantiate different message brokers based on `busType` configuration
- Provides event consumer functionality for external event processing
- Includes built-in `/event-bus/publish/:event` endpoint (can be disabled)
- Supports delayed event processing and retry mechanisms with exponential backoff

### File Store System
- Implements a common `FileStore` interface across all storage providers
- Supports both streaming and buffer-based file operations
- Uses environment variables for provider-specific configuration
- Handles authentication through provider-specific credential chains (AWS IAM, GCP ADC, Azure Managed Identity)

### Plugin Registration
Both plugins are registered as Fastify plugins using `fastify-plugin` and follow the standard Fastify plugin lifecycle. They decorate the Fastify instance with their respective interfaces (`EventBus` and `FileStore`).

## Testing
Tests are configured with Jest and ts-jest, located in the `src/` directory with `.spec.ts` extension. Coverage reports are generated in the `coverage/` directory.

## Build Configuration
- TypeScript configuration uses `tsconfig.build.json` for production builds
- Excludes test files (`**/*.spec.ts`) from production builds
- Outputs to `dist/` directory with type definitions