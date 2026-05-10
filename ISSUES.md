# Codebase Issues Audit

## Audits

| #   | Date       | Description                                                                                                     | New Issues           | False Positives |
| --- | ---------- | --------------------------------------------------------------------------------------------------------------- | -------------------- | --------------- |
| 1   | 2026-05-09 | Initial audit — file-store, utils, types, all specs (cloud providers + utils only; LocalFileStore out of scope) | 1 HIGH, 2 MED, 2 LOW | 0               |
| 2   | 2026-05-10 | Re-verify MED-02 after researching credentialDefaultProvider in S3Client source                                 | 0                    | 1 (MED-02)      |

---

## Fixed Issues

| ID  | Issue | Commit |
| --- | ----- | ------ |
| —   | —     | —      |

## False Positives Removed

| Original ID | Why Removed                                                                                                                                                                                                                                                                                                |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MED-02      | `credentialDefaultProvider` IS a valid internal S3ClientConfig field — confirmed in `node_modules/@aws-sdk/client-s3/dist-cjs/runtimeConfig.js`. The SDK reads it to determine the credential provider when none are explicitly supplied. Passing `defaultProvider` explicitly is intentional and correct. |

---

## Table of Contents

- [HIGH Issues (1 remaining)](#high-issues)
- [MEDIUM Issues (1 remaining)](#medium-issues)
- [LOW Issues (2 remaining)](#low-issues)
- [Summary](#summary)

---

## HIGH Issues

### HIGH-01: Unsafe `err["$metadata"].httpStatusCode` access in `S3FileStore.exists()`

**File:** `src/file-store.ts:327`
**Severity:** HIGH — if the thrown error has no `$metadata` property (network error, credentials error, SDK version change), this line throws `TypeError: Cannot read properties of undefined`, masking the original error entirely.

```typescript
if (err["$metadata"].httpStatusCode === 404) {   // no optional chaining
```

The same class's `getInfo()` method at line 425 uses optional chaining correctly:

```typescript
err["$metadata"]?.httpStatusCode === 404;
```

**Suggested fix:** Apply optional chaining to match `getInfo()`:

```typescript
if (err["$metadata"]?.httpStatusCode === 404) {
  return false;
}
```

---

## MEDIUM Issues

### MED-01: No TypeScript augmentation for `FastifyInstance.FileStore`

**File:** `src/types.ts:1-9`, `src/file-store.ts:443`
**Severity:** MEDIUM — users of this library get no compile-time type safety when accessing `fastify.FileStore`; IDE autocomplete does not surface the `FileStore` interface. The `tsconfig.json` is strict (`noImplicitAny`, `strictNullChecks`), making this a meaningful gap.

`file-store.ts` decorates the instance at runtime:

```typescript
f.decorate("FileStore", new LocalFileStore(dir));
```

But `src/types.ts` only augments `FastifySchema`. The required `FastifyInstance` augmentation is missing. The Jest config (`jest.config.js:10`) sets `diagnostics: false` in ts-jest, which suppresses TypeScript errors during test runs and masks this gap.

**Suggested fix:** Add to `src/types.ts`:

```typescript
import type { FileStore } from "./file-store";

declare module "fastify" {
  interface FastifyInstance {
    FileStore: FileStore;
  }
}
```

Also remove `diagnostics: false` from `jest.config.js` so TypeScript errors surface during `pnpm test`.

---

## LOW Issues

### LOW-01: `streamToBuffer` is unsafe when a stream emits string chunks

**File:** `src/utils.ts:6-14`
**Severity:** LOW — if `setEncoding()` was called on the stream before passing it to `streamToBuffer`, Node.js emits `string` chunks instead of `Buffer`s. Pushing strings into the `Buffer[]` array causes `Buffer.concat()` to throw a `TypeError` at runtime.

```typescript
const chunks: Buffer[] = [];
stream.on("data", (chunk) => chunks.push(chunk)); // chunk may be a string
// ...
Buffer.concat(chunks); // throws TypeError if any chunk is a string
```

The `DataStream` interface's `on("data")` listener signature is `(...args: any[])`, so TypeScript does not catch this.

**Suggested fix:**

```typescript
stream.on("data", (chunk: Buffer | string) =>
  chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)),
);
```

---

### LOW-02: `AzureFileStore.getInfo()` falls back to `new Date()` for missing `lastModified`

**File:** `src/file-store.ts:217`
**Severity:** LOW — when Azure returns properties without a `lastModified` timestamp, the code silently substitutes the current time, making the returned `FileInfo` appear freshly modified. Callers using `lastModified` for cache invalidation or change detection will receive incorrect data.

```typescript
lastModified: properties.lastModified || new Date(),
```

The same pattern exists in `GCPFileStore.getInfo()` at line 300.

**Suggested fix:** Throw or surface the absence explicitly rather than substituting the current time:

```typescript
lastModified: properties.lastModified ?? (() => { throw new Error("lastModified missing from Azure response"); })(),
```

Or, if a sentinel is acceptable, document it clearly and use a fixed epoch (`new Date(0)`) so callers can detect the fallback.

---

## Summary

| Severity                    | Remaining  |
| --------------------------- | ---------- |
| **HIGH**                    | 1          |
| **MEDIUM**                  | 1          |
| **LOW**                     | 2          |
| **TOTAL**                   | **4 open** |
| **Fixed**                   | 0          |
| **False Positives Removed** | 0          |
