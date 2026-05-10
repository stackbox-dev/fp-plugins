# Codebase Issues Audit

## Audits

| #   | Date       | Description                                                                                                     | New Issues           | False Positives |
| --- | ---------- | --------------------------------------------------------------------------------------------------------------- | -------------------- | --------------- |
| 1   | 2026-05-09 | Initial audit — file-store, utils, types, all specs (cloud providers + utils only; LocalFileStore out of scope) | 1 HIGH, 2 MED, 2 LOW | 0               |
| 2   | 2026-05-10 | Re-verify MED-02 after researching credentialDefaultProvider in S3Client source; fix HIGH-01, LOW-01, LOW-02    | 0                    | 1 (MED-02)      |

---

## Fixed Issues

| ID      | Issue                                                                     | Commit  |
| ------- | ------------------------------------------------------------------------- | ------- |
| HIGH-01 | Unsafe `err["$metadata"].httpStatusCode` access in `S3FileStore.exists()` | pending |
| LOW-01  | `streamToBuffer` unsafe with string-mode streams                          | pending |
| LOW-02  | Azure/GCP `getInfo()` fallback to `new Date()` for missing `lastModified` | pending |

## False Positives Removed

| Original ID | Why Removed                                                                                                                                                                                                                                                                                                |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MED-02      | `credentialDefaultProvider` IS a valid internal S3ClientConfig field — confirmed in `node_modules/@aws-sdk/client-s3/dist-cjs/runtimeConfig.js`. The SDK reads it to determine the credential provider when none are explicitly supplied. Passing `defaultProvider` explicitly is intentional and correct. |

---

## Table of Contents

- [MEDIUM Issues (1 remaining)](#medium-issues)
- [Summary](#summary)

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

## Summary

| Severity                    | Remaining  |
| --------------------------- | ---------- |
| **MEDIUM**                  | 1          |
| **TOTAL**                   | **1 open** |
| **Fixed**                   | 3          |
| **False Positives Removed** | 1          |
