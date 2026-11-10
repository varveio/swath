# Releasing swath

This is the maintainer playbook for cutting a swath release. It covers the versioning
scheme and the per-release steps. The heavy lifting is automated by
[`.github/workflows/release.yml`](.github/workflows/release.yml); this document is the
human side of it.

A release ships the Go implementation (`go/`): the `swath` lister and the `swath-replay`
server. The Java implementation still builds and tests in `ci.yml` until it is removed, but
no release publishes it; v0.3.2 was its last.

## Versioning

- **Semantic Versioning: `X.Y.Z`, or `X.Y.Z-rc.N` for a pre-release.** No other
  pre-release identifiers (`-alpha`, `-beta.2`) and no build metadata (`+build`) — the
  release workflow rejects them. `-rc.N` is the only pre-release form the pipeline
  implements publish semantics for.
- **The tag is the one source of truth.** The workflow reads the version from the `vX.Y.Z`
  tag and stamps it into every binary (`go/build.sh` links `$SWATH_VERSION` into
  `internal/buildinfo`), so `swath --version`, the run report's `build.version`, the
  Parquet footer's `created_by`, the archive names, the image tags and the image labels
  all carry it. There is no version file to bump. A build from a source checkout reports
  `devel`.
- **The binaries are checked against it.** The build job fails unless each linux/amd64
  binary prints exactly `swath X.Y.Z (<commit>)` or `swath-replay X.Y.Z (<commit>)`,
  and the publish job checks the same of each pushed image digest.

## What the pipeline produces

For each `vX.Y.Z` tag, once the environment is approved:

- one archive per target, each holding `swath`, `swath-replay`, `LICENSE`, `NOTICE` and
  `THIRD_PARTY_NOTICES` under a `swath-X.Y.Z-OS-ARCH/` directory: `swath-X.Y.Z-linux-amd64.tar.gz`,
  `swath-X.Y.Z-linux-arm64.tar.gz`, `swath-X.Y.Z-darwin-amd64.tar.gz`,
  `swath-X.Y.Z-darwin-arm64.tar.gz` and
  `swath-X.Y.Z-windows-amd64.zip` (`.exe` binaries). The binaries are static
  (`CGO_ENABLED=0`, `-trimpath`), built once by `scripts/ci/release-assets.sh build`, which
  also generates `THIRD_PARTY_NOTICES` with `go/notices.sh`: the license of every module
  linked into the binaries, and the Go distribution's;
- `swath-X.Y.Z.spdx.json`, an SPDX SBOM of the modules linked into every shipped binary;
- `SHA256SUMS` over all of them, and a keyless (cosign) signature bundle per asset;
- multi-arch (`linux/amd64`, `linux/arm64`) images `ghcr.io/varveio/swath` (the lister, on
  distroless static, for its CA certificates) and `ghcr.io/varveio/swath-replay` (on
  `scratch`), both from `Dockerfile.go`, signed and attested. They carry the archived linux
  binaries byte for byte, and the archives' three legal files in `/licenses/`: the publish job
  replaces the Dockerfile's build stage with the binaries and notices unpacked from the
  verified archives (`release-assets.sh image-context`);
- build provenance for the archives, `SHA256SUMS` and both images, and an SBOM attestation
  for the archives;
- a GitHub release led by the repository-owned human summary, followed by generated notes
  from the merged pull requests.

## Container tags

- **Stable releases** publish `ghcr.io/varveio/swath:X.Y.Z` and
  `ghcr.io/varveio/swath-replay:X.Y.Z`, plus rolling `:X.Y`, `:X` (for major versions
  at least 1), and `:latest` tags for each package. Release candidates publish only
  their exact `:X.Y.Z-rc.N` version tag.
- Nothing else publishes: `main` pushes and manual dispatch build and smoke the images without
  pushing ([CI architecture](docs/ops/dev/CI.md#registry-and-artifacts)). To pin an exact
  build, use the immutable digest:
  `ghcr.io/varveio/swath@sha256:…`.
- GHCR creates a newly published package as private. After the first
  `ghcr.io/varveio/swath-replay` publish, set that package's visibility to **public** and
  connect it to this repository before advertising or cutting its first release. Confirm both
  packages are anonymously pullable as part of the release checklist.

## What gates a release

The `source` job verifies tag SHA and main ancestry, then independently requires a successful
**`go-full-gate`** from that exact commit's `ci.yml` main push run. This gate certifies all Go modules,
race shards, non-race checks, platforms, cross-builds, images and replay fixtures; it is independent
of the legacy Java jobs. Being on main or
having a green PR is insufficient. The verifier selects the newest run and its current attempt;
it rejects skipped/failed gates and does not accept a PR or manual dispatch in place of main.

Only a SHA that was the head of a main push can qualify. An intermediate commit in a multi-commit
push, or a PR branch commit merely reachable through a merge, has no qualifying push run: release
the validated pushed main head instead. If its main CI is pending, release waits up to ten minutes. If the gate is failed, missing after that
wait, or predates this qualification mechanism, release stops before building/publishing. For an existing qualifying main run, fix or rerun its failed checks, then rerun release. A commit
without a qualifying push run must be replaced by a validated pushed main head. A successful later commit does
not qualify an earlier tag. The release token needs read-only Actions access to verify this result.

With full validation established, release does not repeat that whole platform matrix. It checks
what guards the bytes it ships, on the tag SHA, from a cold cache:

- `go-tests`: `go vet ./...` and `go test -race ./...` of the main module unsharded on
  linux/amd64 (as nightly's `go-race-nightly`), and of the reader module it compiles in;
- `build`: the static builds for all five targets and their `--version` stamp, and the
  license gate: `go/notices.sh` fails on a linked module whose license go-licenses does not
  recognize or that is not on its allow-list (the Java build's, as SPDX identifiers), or when
  the targets link different modules
  ([license gate](go/docs/internals/testing.md#license-gate));
- `publish`, before any tag exists: both images' `--version` against the pushed digests,
  the image smoke (`scripts/ci/smoke-container.sh`: an anonymous listing of a
  small public S3 prefix over HTTPS into a dataset, read back with DuckDB by the manifest's
  part list, then resumed as a no-op), and the replay image serving that dataset.

`ci.yml` `go-build` runs the same image smoke offline on every main/dispatch run and on
packaging/dependency/full PRs. Ordinary code PRs retain native cross-builds and the license gate;
the main qualification ensures their packaged images are validated before release.

## Release candidates

A `vX.Y.Z-rc.N` tag runs the **entire** publish path — archives, digest push, deep
container smoke, signing, attestation, self-verification — and differs only in what it
names: the `X.Y.Z-rc.N` container tags alone (no `:latest`, no rolling `X.Y`) and a GitHub
**pre-release**.

Use one whenever the publish path itself has changed, and for the first release from this
pipeline. Its purpose is that a failure costs an `rc` number instead of a version number:
tags are immutable, so a `vX.Y.Z` that dies half-way leaves a partial release and a version
you cannot cleanly reuse.

**An RC is not promoted.** Its binaries report `X.Y.Z-rc.N`, so shipping those bytes as
`X.Y.Z` would contradict the tag. The final tag is a fresh build. What the rehearsal proves
is the *pipeline*: credentials, signing identity, attestation subjects, the image smoke
against a real bucket, and whether the verification commands below actually work.

### Release candidate checklist

The steps for `v0.4.0-rc.1`, the first release from this pipeline; for a later candidate,
substitute its version.

1. Merge the release-notes pull request first: `docs/ops/dev/RELEASE_NOTES.md` titled
   `# swath 0.4.0-rc.1`. Wait for `ci.yml` on `main` to be green at that commit, macOS legs
   included.
2. Before the first publish, check the repository settings the `publish` job reads: the
   `PUBLIC_RELEASE_ENABLED` variable is `true`, `SWATH_SMOKE_BUCKET` is unset (the image
   smoke's default public prefix) or names a prefix it can list anonymously, and the
   `public-release` environment has its reviewers.
3. On a clean checkout of `main` at that commit, tag and push:
   ```sh
   just release 0.4.0-rc.1
   git push origin v0.4.0-rc.1
   ```
4. Approve the `public-release` environment when the `Release` run waits on it.
5. Check that the SBOM lists the Go modules, not Java artifacts: the names include `stdlib`
   and module paths such as `github.com/aws/aws-sdk-go-v2/service/s3`, and no Maven
   coordinates.
   ```sh
   gh release download v0.4.0-rc.1 --repo varveio/swath --dir rc
   jq -r '.packages[].name' rc/swath-0.4.0-rc.1.spdx.json | sort -u
   ```
6. Check that each archive holds `THIRD_PARTY_NOTICES` beside the binaries
   (`tar -tzf rc/swath-0.4.0-rc.1-linux-amd64.tar.gz`), then run every command under
   [Verifying a release](#verifying-a-release) with `TAG=v0.4.0-rc.1`, the archive
   `swath-0.4.0-rc.1-linux-amd64.tar.gz`, and the digests from the run's summary.
7. Set the new `ghcr.io/varveio/swath-replay` package public and connect it to this
   repository ([Container tags](#container-tags)), then confirm both images pull
   anonymously and name the tag:
   `docker run --rm ghcr.io/varveio/swath:0.4.0-rc.1 --version` and the same for
   `ghcr.io/varveio/swath-replay:0.4.0-rc.1`.

If a step fails, fix it through a pull request and cut `v0.4.0-rc.2`, retitling the notes;
do not repair the published candidate by hand.

## Cutting a release

1. Draft the human summary in
   [`docs/ops/dev/RELEASE_NOTES.md`](docs/ops/dev/RELEASE_NOTES.md): replace the version
   in its title, fill all three sections, and commit it through a pull request. Lead with
   what users will notice, then the evidence for the release and its honest limits. The
   release recipe and workflow reject an untouched or incomplete template.
2. Make sure `main` is green and you are on a clean checkout of `main` at the commit you
   want to release, with that commit already on `origin/main`.
3. Tag it:
   ```sh
   just release 0.4.0
   ```
   The recipe checks the version's form, the branch, a clean tree, that `HEAD` is on
   `origin/main`, that the tag is new locally and on `origin`, and the release notes, then
   creates the annotated tag `v0.4.0`. It makes no commit and does **not** push.
4. Push the tag to trigger the release pipeline:
   ```sh
   git push origin v0.4.0
   ```
5. The `Release` workflow first proves the tag commit is an ancestor of `origin/main`, requires
   its successful full Go qualification, and checks the release notes, then runs the Go gate and builds the assets once on that exact
   tag SHA, and then waits on the protected `public-release` environment. **Approve it.**
   The publish job then pushes both images by digest, smokes those digests, tags them,
   signs and attests everything, creates the GitHub release as a **draft**, runs the
   verification commands below against what it just published, and only then un-drafts
   it. If verification fails the release stays a draft — fix and re-tag rather than
   publishing by hand.

## Notes

- GitHub appends generated merged-PR notes (`--generate-notes`) after the human summary;
  write PR titles with that in mind, but do not use them as a substitute for the summary's
  user-facing changes, evidence, and limits.
- Choosing the version is manual by design; `just release` wraps the guards so the tag
  names a commit the workflow will accept.
- Artifact retention follows the [registry and artifacts](docs/ops/dev/CI.md#registry-and-artifacts)
  policy: `release-assets`, a hand-off between jobs, 1 day; the `go-race-release` stream 30 days.

## Verifying a release

These are the commands a user runs, and the same ones the publish job runs against itself
before un-drafting. `IDENTITY` is the workflow that produced the release — renaming
`release.yml` would change it and invalidate every published instruction, which is why the
filename is frozen.

The image signatures are stored in the Sigstore bundle format, which cosign reads from v3:
cosign 2.x verifies the `SHA256SUMS` bundle but reports no signatures on the images.

```sh
TAG=v0.4.0
IDENTITY="https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/${TAG}"
ISSUER=https://token.actions.githubusercontent.com

# 1. Checksums cover every asset.
sha256sum --check --ignore-missing SHA256SUMS

# 2. The checksum file itself is signed, so step 1 is trustworthy.
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" SHA256SUMS

# 3. The image, by digest.
cosign verify --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" \
  ghcr.io/varveio/swath@sha256:<digest>
cosign verify --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" \
  ghcr.io/varveio/swath-replay@sha256:<replay-digest>

# 4. Build provenance — which workflow, at which commit, built this.
gh attestation verify oci://ghcr.io/varveio/swath@sha256:<digest> --repo varveio/swath
gh attestation verify oci://ghcr.io/varveio/swath-replay@sha256:<replay-digest> --repo varveio/swath
gh attestation verify SHA256SUMS --repo varveio/swath
gh attestation verify swath-0.4.0-linux-amd64.tar.gz --repo varveio/swath
```
