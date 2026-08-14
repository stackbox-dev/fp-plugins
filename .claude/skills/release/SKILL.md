---
name: release
description: Bump version, build, and prepare a release for npm publish via GitHub Packages
disable-model-invocation: true
---

# Prepare Release

Standardized release workflow for @stackbox-dev/fp-plugins.

## Workflow

1. **Pre-flight checks**
   - Ensure working tree is clean (`git status`)
   - Ensure on `main` branch
   - Run `pnpm test` to verify all tests pass
   - Run `pnpm run build` to verify build succeeds

2. **Version bump**
   - Ask user for bump type: patch, minor, or major
   - Update `version` in `package.json`
   - Create a commit with message: `X.Y.Z` (version number only, matching existing convention)

3. **Verify**
   - Show the user the version diff and commit
   - Remind the user that publishing is triggered by **creating a GitHub Release**,
     not by pushing. `npm-publish-github-packages.yml` runs `on: release: [created]`;
     a push to `main` alone publishes nothing.

## Notes

- The version bump belongs on `main` and nowhere else. Never put one in a feature or
  fix PR — those PRs describe changes, releases decide versions. A bump riding along
  in a feature branch also makes the PR harder to review and forces a rebuild if the
  release is deferred.
- Do NOT push automatically — let the user decide when to push
- Do NOT create git tags by hand. Creating the GitHub Release creates the tag.
- Follow existing commit message convention (see `git log` — version bumps use just the version number like `2.12.0`)
- `main` is protected by a ruleset requiring one approving review, code-owner review,
  and `require_last_push_approval` — so any push after an approval dismisses it. Get
  the branch final before asking for review.
