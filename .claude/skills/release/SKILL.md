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
   - Remind user that pushing to `main` triggers the `npm-publish-github-packages` workflow

## Notes

- Do NOT push automatically — let the user decide when to push
- Do NOT create git tags — the publish workflow handles that
- Follow existing commit message convention (see `git log` — version bumps use just the version number like `2.12.0`)
