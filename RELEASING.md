# Releasing swath

This is the maintainer playbook for cutting a swath release. It covers the versioning
scheme and the per-release steps. The heavy lifting is automated by
[`.github/workflows/release.yml`](.github/workflows/release.yml); this document is the
human side of it.

A release ships the Go implementation (the module at the repository root): the `swath` lister and the `swath-replay`
server. It also versions the nested Go modules `github.com/varveio/swath/reader` and
`github.com/varveio/swath/objfile` with the same version, so a program can require them by
version, and the root module requires those versions, so a program can require it too. The Java
implementation's last release was v0.3.2; it is no longer on `main` and is preserved at the tag
`java-final`.

## Versioning

- **Semantic Versioning: `X.Y.Z`, or `X.Y.Z-rc.N` for a pre-release.** No other
  pre-release identifiers (`-alpha`, `-beta.2`) and no build metadata (`+build`) — the
  release workflow rejects them. `-rc.N` is the only pre-release form the pipeline
  implements publish semantics for.
- **The tag is the one source of truth.** The workflow reads the version from the `vX.Y.Z`
  tag and stamps it into every binary (`build.sh` links `$SWATH_VERSION` into
  `internal/buildinfo`), so `swath --version`, the run report's `build.version`, the
  Parquet footer's `created_by`, the archive names, the image tags and the image labels
  all carry it. There is no version file to bump. A build from a source checkout reports
  `devel`.
- **Every module is tagged with it, in dependency order.** Go requires a nested module's version
  tag to carry its directory, so release `0.5.0` is three tags: `reader/v0.5.0` and
  `objfile/v0.5.0`, one for each `go.mod` below the root, then `v0.5.0` (the root module, and the
  tag that triggers the workflow). The root's `go.mod` requires the nested modules by version and
  carries no `replace`, so it must name the nested tags before it is tagged: the nested tags go on
  a commit of `main` first, a pull request points `go.mod` at them, and the root tag goes on its
  merge commit ([Cutting a release](#cutting-a-release)). Each module is at version 0: no API
  promise yet.
- **Local builds use the local copies.** `go.work` at the repository root joins the three
  modules, so a build, test or CI job in a checkout compiles `reader/` and `objfile/` as they are,
  whatever version `go.mod` requires. The release builds without it (`GOWORK=off`), as an outside
  project does.
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
  also generates `THIRD_PARTY_NOTICES` with `notices.sh`: the license of every module
  linked into the binaries, and the Go distribution's;
- `swath-X.Y.Z.spdx.json`, an SPDX SBOM of the modules linked into every shipped binary;
- `SHA256SUMS` over all of them, and a keyless (cosign) signature bundle per asset;
- multi-arch (`linux/amd64`, `linux/arm64`) images `ghcr.io/varveio/swath` (the lister, on
  distroless static, for its CA certificates) and `ghcr.io/varveio/swath-replay` (on
  `scratch`), both from the `Dockerfile`, signed and attested. They carry the archived linux
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

The `source` job verifies that the root tag names the event's commit and that the commit is an
ancestor of main, and that the commit's `go.mod` is what an outside project resolves: it carries no
`replace` and requires each nested module at this release's tag, and each nested tag is an
ancestor of the commit with the same directory tree, so the nested modules a program downloads
are the code main CI validated through the workspace. It then independently requires a successful
**`go-full-gate`** from that exact commit's `ci.yml` main push run. This gate certifies all Go modules,
race shards, non-race checks, platforms, cross-builds, images and replay fixtures. Being on main or
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

- `go-tests`: the race tests of the main module in the shards of
  `scripts/ci/go-race-shards.sh`, one leg per shard and every leg on linux/amd64, and one more
  leg, `vet-reader`: the main module built as an outside project builds it (`go build ./...` and
  `go mod tidy -diff`, with the workspace off like every job of the workflow), `go vet` of the
  main module and of the reader module it compiles in, the shard table's check that every test of
  the main module runs in exactly one shard, and the reader module's race tests. A failed or
  cancelled leg stops the release before `publish`;
- `build`: the static builds for all five targets and their `--version` stamp, and the
  license gate: `notices.sh` fails on a linked module whose license go-licenses does not
  recognize or that is not on its allow-list (SPDX identifiers), or when
  the targets link different modules
  ([license gate](docs/internals/testing.md#license-gate));
- `publish`, before any tag exists: both images' `--version` against the pushed digests,
  the image smoke (`scripts/ci/smoke-container.sh`: an anonymous listing of a
  small public S3 prefix over HTTPS into a dataset, read back with DuckDB by the manifest's
  part list, then resumed as a no-op), and the replay image serving that dataset.

Sharding gives one thing up: no release leg runs all the tests of `internal/listing`, or all
those of `cmd/swath` (the two packages the shards split), in one process under the race detector,
so a test that leaks state (a global, a goroutine, a file) into a test of another shard is not
caught at the tag. Only nightly's `go-race-nightly` job, the unsharded `go test -race ./...` on
amd64, runs them together, and no workflow checks it: a successful `go-race-nightly` on the commit
being released, or on one with the same Go sources, is a step of the checklist below
([Cutting a release](#cutting-a-release)).

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
3. On a clean checkout of `main` at that commit, release the modules in order, as steps 3 to 5 of
   [Cutting a release](#cutting-a-release) do, with the version `0.4.0-rc.1`: the nested tags,
   the pull request that points `go.mod` at them, then the root tag `v0.4.0-rc.1` on its merge
   commit.
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
   release recipes and workflow reject an untouched or incomplete template.
2. Make sure `main` is green and you are on a clean checkout of `main` at the commit you
   want to release, with that commit already on `origin/main`. Check by hand that the
   `go-race-nightly` job (not just its run: a dispatch with another `jobs` value skips it)
   succeeded on that commit, or on an earlier one with the same Go sources; the release does
   not check it, and it is the only race run of all the tests of `internal/listing` and of
   `cmd/swath` in one process each ([What gates a release](#what-gates-a-release)):
   ```sh
   gh run list --workflow nightly.yml --branch main --limit 5 --json databaseId,headSha,event
   gh run view RUN --json jobs --jq '.jobs[] | select(.name == "go-race-nightly") | .conclusion'
   git diff --stat SHA HEAD -- '*.go' '*go.mod' '*go.sum'   # empty: the same Go sources
   ```
3. Tag the nested modules, and push their tags in one atomic push, as the recipe prints it:
   ```sh
   just release-modules 0.5.0
   git push --atomic origin objfile/v0.5.0 reader/v0.5.0
   ```
   The recipe checks the version's form, the branch, a clean tree, that `HEAD` is on
   `origin/main`, that each tag is new locally and on `origin`, and the release notes, then
   creates the annotated tags `objfile/v0.5.0` and `reader/v0.5.0`. It makes no commit and does
   **not** push. A nested tag starts no workflow, but once pushed it is public: the Go module
   proxy serves it and keeps it, so tag only a commit you mean to release.
4. Point the root module at them through a pull request:
   ```sh
   just release-prep 0.5.0
   git push -u origin release-prep-0.5.0
   gh pr create --fill
   ```
   The recipe makes the same checks of `main`, fetches the nested tags from `origin`, and on a
   new branch `release-prep-0.5.0` requires them in `go.mod` (`go get`, then `go mod tidy`, with
   the workspace off). It builds the root module as an outside project would
   (`GOWORK=off go build ./...` and `GOWORK=off go mod tidy -diff`), commits `go.mod` and `go.sum`
   (signed off), and checks the commit with `scripts/ci/verify-release-source.sh modules`. Merge
   the pull request. Its merge commit differs from the commit of step 2 in `go.mod` and `go.sum`
   alone, and the workspace compiles the same `reader/` and `objfile/`, so step 2's
   `go-race-nightly` check still holds. Nothing may change `reader/` or `objfile/` between the
   nested tags and that merge commit: the release refuses it, and as tags are immutable the way
   out is the next version.
5. Wait for `ci.yml` on `main` to be green at the merge commit, then, on a clean checkout of
   `main` at it, tag the root module and push its tag, which starts the release pipeline:
   ```sh
   git switch main && git pull --ff-only
   just release 0.5.0
   git push origin v0.5.0
   ```
   The recipe makes the same checks of `main`, that `v0.5.0` is new locally and on `origin`, and
   that the nested tags are on `origin`; then that `go.mod` carries no `replace` and requires the
   nested modules at `v0.5.0`, each nested tag being an ancestor of `HEAD` with the same
   directory tree (`verify-release-source.sh modules`), the outside-project build, and the release
   notes. It creates the annotated tag `v0.5.0`, makes no commit and does **not** push.
6. The `Release` workflow first proves that the root tag names the tagged commit, that it is an
   ancestor of `origin/main`, and that its `go.mod` requires the nested modules at their tags
   with the same trees, requires its successful full Go qualification,
   and checks the release notes, then runs the Go gate and builds the assets once on that
   exact tag SHA, and then waits on the protected `public-release` environment. **Approve it.**
   The publish job then pushes both images by digest, smokes those digests, tags them,
   signs and attests everything, creates the GitHub release as a **draft**, runs the
   verification commands below against what it just published, and only then un-drafts
   it. If verification fails the release stays a draft — fix and re-tag rather than
   publishing by hand.

## Notes

- GitHub appends generated merged-PR notes (`--generate-notes`) after the human summary;
  write PR titles with that in mind, but do not use them as a substitute for the summary's
  user-facing changes, evidence, and limits.
- Choosing the version is manual by design; `just release-modules`, `just release-prep` and
  `just release` wrap the guards so that the tags name commits the workflow will accept. None of
  them commits to `main`: the one commit a release needs reaches `main` through its pull request.
- Artifact retention follows the [registry and artifacts](docs/ops/dev/CI.md#registry-and-artifacts)
  policy: `release-assets`, a hand-off between jobs, 1 day; the `go-race-release-SHARD` streams
  30 days.

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
