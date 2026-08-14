---
name: gen-test
description: Generate a Jest test file for a source module in this package
disable-model-invocation: true
---

# Generate Test File

Generate a Jest test file for a given source module in this project.

## Workflow

1. Ask which source file to generate tests for (if not specified)
2. Read the source file to understand exports, interfaces, and dependencies
3. Read an existing sibling spec first — `src/file-store.gcs.spec.ts` is the clearest
   model — and follow its shape rather than inventing a new one

## Conventions

- Test files live alongside source: `src/foo.ts` → `src/foo.spec.ts`.
  Provider-specific specs are suffixed: `file-store.<provider>.spec.ts`
- `ts-jest` preset, `testEnvironment: "node"`
- Cloud SDKs are **mocked**, never containerised — `jest.mock("@google-cloud/storage")`
  and friends. This package has no Docker-based tests and no `testcontainers`
  dependency; do not add one
- `LocalFileStore` is the exception: test it against the real filesystem using
  `fs.promises.mkdtemp` under `os.tmpdir()`, and clean up in `afterEach`
- Save and restore `process.env` around tests that set provider env vars
- Import order follows the Prettier config: node builtins → third-party → relative
- **Coverage is 100% and must stay there** — every branch, including error paths and
  `||` / `??` fallbacks

## Reaching the provider classes

None of the `FileStore` implementations are exported. Get one by registering the
plugin and reading the decoration:

```typescript
const fastify = Fastify();
await fastify.register(FileStorePlugin, { type: "gcs" });
const store = fastify.FileStore;
```

The cloud SDKs are lazily `require()`d inside each `Configure*` function rather than
imported at module scope. `jest.mock()` still intercepts those `require()` calls
normally. If a test needs a different mock shape than one already established in the
file, use `jest.resetModules()` and re-`require("./file-store")`.

## Unit Test Template

```typescript
import Fastify from "fastify";
import FileStorePlugin, { FileStore } from "./file-store";

jest.mock("@google-cloud/storage");

describe("moduleName", () => {
  let fastify: ReturnType<typeof Fastify>;
  const ORIGINAL_ENV = process.env;

  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV };
    jest.clearAllMocks();
    fastify = Fastify();
  });

  afterEach(async () => {
    process.env = ORIGINAL_ENV;
    await fastify.close();
  });

  it("should do X when Y", async () => {
    // arrange, act, assert
  });
});
```
