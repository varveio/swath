# Releasing swath

This is the maintainer playbook for cutting a swath release. It covers the versioning
scheme and the per-release steps. The heavy lifting is automated by
[`.github/workflows/release.yml`](.github/workflows/release.yml); this document is the
human side of it.

## Versioning

- **Semantic Versioning: `X.Y.Z`, or `X.Y.Z-rc.N` for a pre-release.** No other
  pre-release identifiers (`-alpha`, `-beta.2`) and no build metadata (`+build`) — the
  release build rejects them. `-rc.N` is the only pre-release form the pipeline
  implements publish semantics for.
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

## Release candidates

A `vX.Y.Z-rc.N` tag runs the **entire** publish path — promotion, digest push, deep
container smoke, signing, attestation, self-verification — and differs only in what it
names: the `X.Y.Z-rc.N` container tag alone (no `:latest`, no rolling `X.Y`) and a GitHub
**pre-release**.

Use one whenever the publish path itself has changed. Its purpose is that a failure costs
an `rc` number instead of a version number: tags are immutable, so a `vX.Y.Z` that dies
half-way leaves a partial release and a version you cannot cleanly reuse.

**An RC is not promoted.** `gradle.properties` reads `X.Y.Z-rc.N`, and that string is
baked into the jar manifest and reported by `swath --version`, so shipping those bytes as
`X.Y.Z` would contradict the tag — which `verifyReleaseVersion` exists to prevent. The
final tag is a fresh build. What the rehearsal proves is the *pipeline*: credentials,
signing identity, attestation subjects, and whether the verification commands below
actually work.

## Cutting a release

1. Draft the human summary in
   [`docs/ops/dev/RELEASE_NOTES.md`](docs/ops/dev/RELEASE_NOTES.md): replace the version
   in its title, fill all three sections, and commit it. Lead with what users will notice,
   then the evidence for the release and its honest limits. The release recipe and workflow
   reject an untouched or incomplete template.
2. Make sure `main` is green and you are on a clean checkout of the commit you want to
   release.
3. Prepare the release commits and tag:
   ```sh
   just release 0.2.0
   ```
   This produces **two** commits and tags the first:

   1. `Release v0.2.0` — the canonical version, **tagged**; this is what the workflow builds
   2. `Begin 0.2.1 development` — restores a `-SNAPSHOT` version on `main`

   so `main` can never be left sitting on a released version. Override the next cycle with a
   second argument (`just release 0.2.0 0.3.0`); after an `-rc.N` the default returns to the
   same `X.Y.Z-SNAPSHOT`, since development continues toward that release. It does **not**
   push — review both commits first.
4. Push to trigger the release pipeline — this sends both commits and the tag together:
   ```sh
   git push origin main v0.2.0
   ```
5. The `Release` workflow first proves the tag commit is an ancestor of `origin/main`,
   then runs the fast, integration, and deep tiers on that exact tag SHA. It builds the
   assets once, generates checksums and an SBOM, and then waits on the protected
   `public-release` environment. **Approve it.** The publish job then pushes the image by
   digest, smokes that digest, tags it, signs and attests everything, creates the GitHub
   release as a **draft**, runs the verification commands below against what it just
   published, and only then un-drafts it. If verification fails the release stays a draft
   — fix and re-tag rather than publishing by hand.

## What the pipeline produces

For each `vX.Y.Z` tag, once the environment is approved:

- the exact tested fat jar as `swath-X.Y.Z.jar`, `.zip`/`.tar.gz` distributions, and an
  SPDX SBOM;
- a `SHA256SUMS` file plus a per-asset keyless (cosign) signature bundle;
- a multi-arch (`linux/amd64,linux/arm64`) container image built from the exact tested
  jar, signed and attested;
- a GitHub release led by the repository-owned human summary, followed by generated notes
  from the merged pull requests.

## Notes

- GitHub appends generated merged-PR notes (`--generate-notes`) after the human summary;
  write PR titles with that in mind, but do not use them as a substitute for the summary's
  user-facing changes, evidence, and limits.
- Choosing the version is manual by design (the canonical version lives in
  `gradle.properties`); `just release` wraps the mechanical steps so the tag and version
  cannot drift, and reopens the next `-SNAPSHOT` cycle in the same invocation.

## Verifying a release

These are the commands a user runs, and the same ones the publish job runs against itself
before un-drafting. `IDENTITY` is the workflow that produced the release — renaming
`release.yml` would change it and invalidate every published instruction, which is why the
filename is frozen.

```sh
TAG=v0.1.0
IDENTITY="https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/${TAG}"
ISSUER=https://token.actions.githubusercontent.com

# 1. Checksums cover every asset.
sha256sum --check SHA256SUMS

# 2. The checksum file itself is signed, so step 1 is trustworthy.
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" SHA256SUMS

# 3. The image, by digest.
cosign verify --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" \
  ghcr.io/varveio/swath@sha256:<digest>

# 4. Build provenance — which workflow, at which commit, built this.
gh attestation verify oci://ghcr.io/varveio/swath@sha256:<digest> --repo varveio/swath
gh attestation verify SHA256SUMS --repo varveio/swath
```
