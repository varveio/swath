# Releasing swath

This is the maintainer playbook for cutting a swath release. It covers the versioning
scheme and the per-release steps. The heavy lifting is automated by
[`.github/workflows/release.yml`](.github/workflows/release.yml); this document is the
human side of it.

## Versioning

- **Semantic Versioning, stable `X.Y.Z` only.** Pre-release and build-metadata tags
  (`v1.2.3-rc1`, `v1.2.3+build`) are intentionally **not** supported — the release build
  rejects them.
- **One source of truth.** `version` in [`gradle.properties`](gradle.properties) drives
  every produced artifact: the Gradle build, `swath --version`, jar manifests, archive
  names, and the container image labels. Between releases it carries a `-SNAPSHOT`
  suffix (e.g. `0.2.0-SNAPSHOT`).
- **The tag is checked against it.** On a `vX.Y.Z` tag, `verifyReleaseVersion` fails the
  build unless the tag exactly equals `v` + the canonical version, so a release can never
  ship mismatched identifiers.

## Container tags

- **Releases** publish `ghcr.io/varveio/swath:X.Y.Z` and `:latest`.
- **Merges to `main`** publish the immutable `sha-<gitsha>` tag plus the mutable `main`
  pointer.
- **Manual dispatch** (any branch) publishes `sha-<gitsha>` **only** — the branch-name tag
  is restricted to `main`, so internal branch names never reach the public package. Pull a
  branch build by its `sha-` tag.
- `:latest` and every semver tag are owned solely by releases; no development build can
  regress them. To pin an exact build, use the immutable digest:
  `ghcr.io/varveio/swath@sha256:…`.

## Cutting a release

1. Make sure `main` is green and you are on a clean checkout of the commit you want to
   release.
2. Prepare the release commit and tag:
   ```
   just release 0.2.0
   ```
   This sets the canonical version, commits `Release v0.2.0`, and creates the annotated
   tag `v0.2.0`. It does **not** push — review first.
3. Push to trigger the release pipeline:
   ```
   git push origin main v0.2.0
   ```
4. The `Release` workflow builds the assets once, generates checksums and an SBOM, and
   then waits on the protected `public-release` environment. **Approve it** to publish the
   signed jar, distributions, container image, and GitHub release.
5. Bump the canonical version to the next development cycle and commit:
   ```
   # edit gradle.properties: version=0.3.0-SNAPSHOT   (or 0.2.1-SNAPSHOT)
   git commit -am "Begin 0.3.0 development"
   git push
   ```

## What the pipeline produces

For each `vX.Y.Z` tag, once the environment is approved:

- the exact tested fat jar, `.zip`/`.tar` distributions, and an SPDX SBOM;
- a `SHA256SUMS` file plus a per-asset keyless (cosign) signature bundle;
- a multi-arch (`linux/amd64,linux/arm64`) container image built from the exact tested
  jar, signed and attested;
- a GitHub release with auto-generated notes from the merged pull requests.

## Notes

- Release notes are generated from merged PRs (`--generate-notes`); write PR titles with
  that in mind.
- The version bump is manual by design (the canonical version lives in `gradle.properties`).
  `just release` wraps the mechanical steps so the tag and version cannot drift.
