# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

- **Build**: `pnpm run build` — cleans `dist` and compiles TypeScript
- **Test**: `pnpm test` — runs the Jest suite
- **Test with coverage**: `pnpm run test:coverage`
- **Lint**: `pnpm run lint` (`pnpm run lint:fix` to autofix)
- **Integration test**: `pnpm run test:integration` — needs MinIO, see below
- **Format code**: `pnpm run pretty` — Prettier over `src/**`
- **Clean**: `pnpm run clean` — removes `dist`
- **Transpile**: `pnpm run transpile` — TypeScript compilation only

The only other scripts in `package.json` are `prepare` (husky) and `prepublishOnly`
(build before publish). CI (`.github/workflows/`) runs
`pnpm install --frozen-lockfile`, then lint, build and test on Node 24; the package declares
`engines: node >=22`.

**pnpm only** — never npm or yarn. `pnpm-lock.yaml` is committed and CI installs
frozen, so dependency changes are reviewed as a lockfile diff. The pnpm version is
pinned by `packageManager` in `package.json`; `pnpm/action-setup` reads it.

## Project Architecture

This is a Fastify plugin library (`@stackbox-dev/fp-plugins`) providing one reusable
plugin for Stackbox applications.

### Core Structure

- **Main exports** (`src/index.ts`): exposes `Plugins.FileStore`
- **File Store Plugin** (`src/file-store.ts`): cloud storage abstraction over AWS S3,
  GCS, Azure Blob Storage, MinIO, and the local filesystem
- **`src/utils.ts`**: `streamToBuffer`, safe with both buffer- and string-mode streams
- **`src/types.ts`**: augments `FastifySchema` and adds `FastifyInstance.FileStore`.
  `index.ts` imports it for its side effect — without that import the augmentations
  never reach consumers, since TypeScript only applies them if the declaring file is
  pulled into the consumer's compilation.

An event-bus plugin (RabbitMQ / GCP Pub/Sub / Azure Service Bus / NATS JetStream)
used to live here and was removed in `c001efc`.

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
provider: `file-store.local.spec.ts`, `.gcs.`, `.s3.`, `.azure.`, plus
`file-store.spec.ts`, `integration.spec.ts` and `utils.spec.ts`. Coverage is 100% on statements,
branches, functions and lines — keep it there.

Cloud SDKs are mocked with `jest.mock(...)`; `LocalFileStore` runs against the real
filesystem in temp dirs.

`file-store.minio.integration.spec.ts` is the one test that speaks a real S3 wire
protocol. It is skipped unless `MINIO_TEST_ENDPOINT` is set, so the default suite
needs no Docker:

```bash
docker run -d --name fp-minio -p 19000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:latest server /data
pnpm run test:integration
```

Mocked tests cannot catch a behavioural change in an SDK — the NoSuchKey/NotFound
mismatch fixed in #9 was invisible to them. Add to the integration spec when touching
S3 or MinIO behaviour.

## Build Configuration

- Production builds use `tsconfig.build.json`, which excludes `**/*.spec.ts`
- Outputs to `dist/` with type definitions and **no source maps**

## Issues

Bugs, audit findings and follow-ups belong in **GitHub issues** on this repository.
Do not reintroduce a tracked `ISSUES.md` or any other in-repo issue list — one existed
until 2026-08-14 and its open finding moved to #11. Context that is not itself an
issue — what was audited, when, what turned out to be a false positive — belongs in
the issue thread or the PR that resolves it.

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
- **ts-jest type checking is on.** `diagnostics: false` used to suppress TypeScript
  errors in tests, which is how the missing `FastifyInstance` augmentation went
  unnoticed. Only ts-jest config warning 151002 is ignored: the fix it suggests
  (`isolatedModules: true`) zeroes function coverage for `index.ts`.
- **`types: ["jest", "node"]`** is set explicitly in the ts-jest tsconfig override.
  Without it, enabling diagnostics fails every spec with `Cannot find name 'describe'`.
- **Pre-commit hooks**: Husky + lint-staged run Prettier on staged
  `.js/.ts/.json/.md` files.
- **`tsconfig.build.json` sets `skipLibCheck: true`** — still required. Turning it off
  fails on `thread-stream@4.2.0` (a pino dependency), whose `.d.ts` references
  `TransferListItem`, a name its `worker_threads` types do not export.
- **Releasing**: bump the version with `pnpm version` on a `release/X.Y.Z` branch and
  merge it. `.github/workflows/release.yml` does the rest — tag, GitHub Release,
  publish — on any push to `main` whose version has no matching tag. Do not push tags
  or create releases by hand, and never put a version bump in a feature PR. See
  `.claude/skills/release/SKILL.md`.
