# Contributing

## Setup

Node.js >= 22 (CI runs 24). This repository uses **pnpm** (pinned via `packageManager`
in `package.json`). Do not use npm or yarn — `pnpm-lock.yaml` is the committed lockfile
and the only one CI reads.

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

Cut the version on a branch — `main` requires a PR:

```bash
git checkout -b release/X.Y.Z
pnpm version <patch|minor|major>
git push -u origin release/X.Y.Z   # branch only; do not push the tag
```

Open a PR titled with the version. **Merging it is the release**: the `Release`
workflow tags, creates the GitHub Release and publishes to GitHub Packages, after
running lint, build, unit tests and the MinIO integration suite.

Do not push tags or create releases by hand. Merges whose version is already tagged
are a no-op, so normal PRs do not publish.

See `.claude/skills/release/SKILL.md`.
