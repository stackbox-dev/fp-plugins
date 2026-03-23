---
name: gen-test
description: Generate a Jest test file for a source module, with testcontainers setup for integration tests
disable-model-invocation: true
---

# Generate Test File

Generate a Jest test file for a given source module in this project.

## Workflow

1. Ask which source file to generate tests for (if not specified)
2. Read the source file to understand exports, interfaces, and dependencies
3. Determine test type:
   - **Unit test**: For pure logic, utilities, interfaces — mock external dependencies
   - **Integration test**: For provider implementations (rabbitmq, gcp-pubsub, azure-servicebus, nats-jetstream) — use testcontainers

## Conventions

- Test files live alongside source: `src/foo.ts` → `src/foo.spec.ts`
- Use `ts-jest` preset with `testEnvironment: "node"`
- Integration tests use `testcontainers` for Docker-based services
- Integration test files should have long timeouts (120-180s) via `--testTimeout`
- Follow existing test patterns in the codebase (read a few `.spec.ts` files first)
- Import order follows Prettier config: node builtins → third-party → @stackbox-dev → relative

## Integration Test Template

For provider tests that need Docker containers:

```typescript
import { GenericContainer, StartedTestContainer } from "testcontainers";

describe("ProviderName", () => {
  let container: StartedTestContainer;

  beforeAll(async () => {
    container = await new GenericContainer("image:tag")
      .withExposedPorts(PORT)
      .start();
  }, 60_000);

  afterAll(async () => {
    await container?.stop();
  });

  // tests here
});
```

## Unit Test Template

```typescript
import { functionUnderTest } from "./module";

describe("moduleName", () => {
  it("should do X when Y", () => {
    // arrange, act, assert
  });
});
```
