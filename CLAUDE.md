# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

- **Build**: `pnpm run build` — cleans `dist` and compiles TypeScript
- **Test**: `pnpm test` — runs the Jest suite
- **Test with coverage**: `pnpm run test:coverage`
- **Format code**: `pnpm run pretty` — Prettier over `src/**`
- **Clean**: `pnpm run clean` — removes `dist`
- **Transpile**: `pnpm run transpile` — TypeScript compilation only

These are the only scripts defined in `package.json`. CI (`.github/workflows/`) runs
`npm install` + `npm test` on Node 24; the package declares `engines: node >=22`.

## Project Architecture

This is a Fastify plugin library (`@stackbox-dev/fp-plugins`) providing one reusable
plugin for Stackbox applications.

### Core Structure

- **Main exports** (`src/index.ts`): exposes `Plugins.FileStore`
- **File Store Plugin** (`src/file-store.ts`): cloud storage abstraction over AWS S3,
  GCS, Azure Blob Storage, MinIO, and the local filesystem
- **`src/utils.ts`**: `streamToBuffer`, safe with both buffer- and string-mode streams
- **`src/types.ts`**: augments `FastifySchema` with `operationId` / `summary` /
  `description`; imported for its side effect

An event-bus plugin (RabbitMQ / GCP Pub/Sub / Azure Service Bus / NATS JetStream)
used to live here and was removed in `c001efc`. Ignore references to it in
`docs/superpowers/plans/` — those are historical planning records, not current design.

### File Store System

- One `FileStore` interface implemented across all five providers
- Both streaming and buffer-based operations
- Provider chosen by the `type` option at registration: `local`, `gcs`, `s3`,
  `minio`, `azureBlob`
- Configured via environment variables per provider — see `README.md`
- Authentication through provider credential chains (AWS IAM, GCP ADC, Azure Managed Identity)

### Plugin Registration

Registered via `fastify-plugin` (v6) and decorates the Fastify instance with
`FileStore`.

## Testing

Jest + ts-jest. Tests live beside their source in `src/` as `*.spec.ts`, split by
provider: `file-store.local.spec.ts`, `.gcs.`, `.s3.`, `.azure.`, plus the original
`file-store.spec.ts` and `integration.spec.ts`. Coverage is 100% on statements,
branches, functions and lines — keep it there.

Cloud SDKs are mocked with `jest.mock(...)`; only `LocalFileStore` is exercised
against the real filesystem, using temp dirs. No test performs real network I/O.

## Build Configuration

- Production builds use `tsconfig.build.json`, which excludes `**/*.spec.ts`
- Outputs to `dist/` with type definitions and **no source maps**

## Gotchas

- **Cloud SDKs are lazily required.** `file-store.ts` holds `S3` / `Upload` /
  `AzureBlob` / `AzureIden` / `Gcs` as module-level bindings assigned by `loadAWS()`,
  `loadAzure()` and `loadGCP()`, each called from the matching `Configure*` function.
  Importing all three at module scope cost ~360ms and ~31MB of heap per boot when a
  deployment only ever uses one. Keep new provider code behind the same pattern, and
  use `import type` for anything that is only a type.
- **ts-jest overrides `sourceMap`.** `tsconfig.json` sets `sourceMap: false`, which
  makes istanbul report emitted-JS line numbers — TypeScript parameter properties
  expand on compile, so coverage gets attributed to imports and comments and reads
  several points low. `jest.config.js` overrides `sourceMap: true` for the transform
  only. Do not remove it or coverage numbers become meaningless.
- **`diagnostics: false` in `jest.config.js`** suppresses TypeScript errors during
  test runs, which masks real type gaps. One known consequence: `src/types.ts` does
  not augment `FastifyInstance` with `FileStore`, so consumers get no compile-time
  safety on `fastify.FileStore` and the specs do not complain either. Consumers work
  around it by declaring the augmentation themselves.
- **Pre-commit hooks**: Husky + lint-staged run Prettier on staged
  `.js/.ts/.json/.md` files.
- **`tsconfig.build.json` sets `skipLibCheck: true`** — a workaround retained from
  when this package depended on `@nats-io/jetstream`.
- **Releasing**: version bumps happen on `main`, in their own commit named just the
  version (`2.16.0`). Do not put a version bump in a feature PR and do not create
  tags by hand — publishing is triggered by creating a GitHub Release. See
  `.claude/skills/release/SKILL.md`.
