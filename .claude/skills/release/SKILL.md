---
name: release
description: Bump version, build, and prepare a release for npm publish via GitHub Packages
disable-model-invocation: true
---

# Prepare Release

Standardized release workflow for @stackbox-dev/fp-plugins.

## Workflow

1. **Pre-flight**
   - Working tree clean, on `main`, up to date
   - `pnpm test` and `pnpm run build` pass

2. **Cut the version on a branch** — `main` requires a PR, so this cannot be pushed
   directly:

   ```bash
   git checkout -b release/X.Y.Z
   pnpm version <patch|minor|major>   # commit "X.Y.Z" + local tag vX.Y.Z
   git push -u origin release/X.Y.Z   # branch only, not the tag
   ```

   Ask the user for the bump type. `pnpm version` writes a commit whose message is
   just the version, matching existing history.

3. **Open the PR**, titled with the version (see `release/2.14.0`, PR #4).

4. **Merging is the release.** `.github/workflows/release.yml` runs on every push to
   `main`: if `package.json`'s version has no matching `vX.Y.Z` tag, it runs the full
   gate (lint, build, unit tests, MinIO integration), then tags, creates the GitHub
   Release and publishes to GitHub Packages. Nothing else is required.

## Notes

- **Do not push the tag by hand.** The workflow creates it. `pnpm version` makes one
  locally; delete it (`git tag -d vX.Y.Z`) or just leave it unpushed.
- **Do not create the GitHub Release by hand.** The workflow does that too, with
  generated notes.
- A version bump belongs on `main` and nowhere else — never in a feature or fix PR.
- Pushes to `main` whose version is already tagged are a no-op, so ordinary merges do
  not publish.
- A failed run can be retried with `workflow_dispatch` without bumping the version
  again; every step is idempotent.
- `main` requires one approving review and sets `require_last_push_approval`, so any
  push after an approval dismisses it. Get the branch final before requesting review.
