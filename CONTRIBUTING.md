# Contributing

## Setup

This repository uses **pnpm** (pinned via `packageManager` in `package.json`). Do not
use npm or yarn — `pnpm-lock.yaml` is the committed lockfile and the only one CI reads.

```bash
pnpm install
pnpm test
pnpm run build
```

`pnpm-lock.yaml` is committed deliberately. CI installs with `--frozen-lockfile`, so a
dependency change must be reviewed as a lockfile diff like any other change. If an
install fails in CI with a lockfile mismatch, run `pnpm install` locally and commit the
updated lockfile.

## Pull requests

- `main` requires one approving review, and `require_last_push_approval` is set — any
  push after an approval dismisses it, so get the branch final before requesting review.
- Keep coverage at 100% — `jest.config.js` enforces it as a threshold, so a drop
  fails the build rather than being reported and ignored.
- `pnpm run lint` must pass; CI runs lint, build and test.
- Touching S3 or MinIO behaviour? Run the integration suite against real MinIO:
  ```bash
  docker run -d --name fp-minio -p 19000:9000 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio:latest server /data
  pnpm run test:integration
  ```
- Bugs and follow-ups go in **GitHub issues**, not a tracked file in the repo.

## Releases

**Do not put a version bump in a feature or fix PR**, and do not create tags by hand.

Releasing is a separate act on `main`:

1. Bump `version` in `package.json` in its own commit, whose message is just the
   version number (`2.16.0`), matching existing history.
2. Create a **GitHub Release**. That is what creates the tag and triggers
   `npm-publish-github-packages.yml` to publish. Pushing to `main` alone publishes
   nothing.

See `.claude/skills/release/SKILL.md` for the full checklist.
