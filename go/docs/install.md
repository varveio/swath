# Install

swath is one static binary with no runtime to install. The Go command line is available as release
candidates, including v0.4.0-rc.1 and v0.4.0-rc.2, from the
[GitHub releases page](https://github.com/varveio/swath/releases). Choose a pre-release explicitly:
v0.3.2 is the earlier Java command line and dataset format ([upgrade.md](upgrade.md)). These candidates
are for testing before the final 0.4.0 release.

## Download a release candidate

For v0.4.0-rc.2, choose the archive for your platform:

| Platform | Archive |
|---|---|
| Linux, x86-64 | `swath-0.4.0-rc.2-linux-amd64.tar.gz` |
| Linux, ARM64 | `swath-0.4.0-rc.2-linux-arm64.tar.gz` |
| macOS, Intel | `swath-0.4.0-rc.2-darwin-amd64.tar.gz` |
| macOS, Apple silicon | `swath-0.4.0-rc.2-darwin-arm64.tar.gz` |
| Windows, x86-64 | `swath-0.4.0-rc.2-windows-amd64.zip` |

Each archive holds `swath`, `swath-replay` (both with `.exe` on Windows), `LICENSE`, `NOTICE` and
`THIRD_PARTY_NOTICES` in a `swath-0.4.0-rc.2-OS-ARCH/` directory. The release also carries
`swath-0.4.0-rc.2.spdx.json` (the SPDX SBOM), `SHA256SUMS`, and a keyless cosign signature bundle
(`.sigstore.json`) for each archive, the SBOM and `SHA256SUMS`.

Download `SHA256SUMS` and `SHA256SUMS.sigstore.json` alongside your archive. Verify the checksum file's
signature, then the archive, before extracting or running it. For example, on Linux:

```bash
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/v0.4.0-rc.2 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify swath-0.4.0-rc.2-linux-amd64.tar.gz --repo varveio/swath
tar -xzf swath-0.4.0-rc.2-linux-amd64.tar.gz
./swath-0.4.0-rc.2-linux-amd64/swath --version
```

On macOS, `shasum -a 256 ARCHIVE` computes the hash to compare with its line in `SHA256SUMS`; on
Windows, use PowerShell's `Get-FileHash ARCHIVE -Algorithm SHA256`. Build provenance attestations cover
the archives, `SHA256SUMS` and both image digests; the workflow also attests the SBOM to each archive.
[RELEASING.md](../../RELEASING.md#verifying-a-release) has the image signature and provenance commands:
use the candidate's tag for `TAG` (image verification needs cosign v3 or later).

The macOS binaries are not yet signed with an Apple Developer ID or notarized
([#420](https://github.com/varveio/swath/issues/420)), so Gatekeeper refuses to run a quarantined
browser download ("Apple could not verify…"). After verifying and extracting the archive, clear the
quarantine attribute (use `darwin-amd64` for Intel):

```bash
xattr -d com.apple.quarantine swath-0.4.0-rc.2-darwin-arm64/swath swath-0.4.0-rc.2-darwin-arm64/swath-replay
```

Put the extracted binary on your `PATH`, for example with `install -m 755 swath ~/.local/bin/` from
inside the extracted directory on Linux or macOS.

## Build from source

You need Go 1.26 or later (the `go` line of `go/go.mod`; with an older `go` command the toolchain named there is
fetched) and git.

```bash
git clone https://github.com/varveio/swath.git
cd swath/go
./build.sh -o swath
./swath --version
```

`./build.sh` builds `./cmd/swath` with `CGO_ENABLED=0`, strips the binary and stamps it with the checkout's
commit (`-dirty` when `go/` has local changes, `$SWATH_REVISION` when set) and with `$SWATH_VERSION` as the
version (a release sets its `X.Y.Z`; unset, the version reads `devel`). Put the binary on your `PATH`, for example
`install -m 755 swath ~/.local/bin/`.

A plain `go build ./cmd/swath` works too, but takes the revision from the Go toolchain's version-control stamp,
which a build in a git worktree may lack.

## Verify the binary

```text
$ swath --version
swath devel (11ae90e42babcd159d52e8f492460802b9e0816c)
```

`--version` prints `swath VERSION (REVISION)`: the version (a release's `X.Y.Z`, `devel` for a build from a checkout) and the commit it was
built from.
Every run report carries the same in `build`. Keep it with any run you may need to investigate.

Then run a listing that needs no cloud account:

```bash
swath du -d 1 .
```

## Container images

The release publishes multi-arch images for linux/amd64 and linux/arm64, containing the archived Linux
binaries and the legal files in `/licenses/`:

| Image | Entrypoint | Base |
|---|---|---|
| `ghcr.io/varveio/swath:0.4.0-rc.2` | `/opt/swath/swath` | distroless static; no shell |
| `ghcr.io/varveio/swath-replay:0.4.0-rc.2` | `/opt/swath-replay/swath-replay` | `scratch` |

A release candidate publishes only its own `X.Y.Z-rc.N` tag, without updating `latest` or `X.Y`.
Both image digests are signed with cosign and have build provenance attestations (verification above).
Pin a digest for runs that must use the same image. Both images run as `65532:65532`: mount a directory
that user can write for `-o`. The lister's home directory is `/home/nonroot`: pass credentials through
the environment or mount a credential directory there (for example `-v ~/.aws:/home/nonroot/.aws:ro`),
with its files readable by that user:

```bash
docker run --rm -v "$PWD/out:/out" ghcr.io/varveio/swath:0.4.0-rc.2 \
  list s3://bucket/prefix/ --anonymous --s3-region us-east-1 -o /out/inventory
```

See [swath-replay](internals/swath-replay.md#container-image) for the replay server. To build an image
from a checkout, run `docker build -f Dockerfile.go -t swath:dev .` from the repository root
(`--target swath-replay` for the replay server; `--build-arg SWATH_REVISION="$(git rev-parse HEAD)"`
stamps the commit, as the build context carries no `.git`). The build downloads modules and generates
`THIRD_PARTY_NOTICES`, failing on a disallowed license ([license gate](internals/testing.md#license-gate)).

## Platforms

`build.sh` cross-compiles with `GOOS` and `GOARCH`:

```bash
GOOS=linux  GOARCH=arm64 ./build.sh -o swath-linux-arm64
GOOS=darwin GOARCH=arm64 ./build.sh -o swath-darwin-arm64
GOOS=darwin GOARCH=amd64 ./build.sh -o swath-darwin-amd64
GOOS=windows GOARCH=amd64 ./build.sh -o swath.exe
```

The test suite runs on Linux (amd64 and arm64), macOS (arm64) and Windows (amd64). On Windows, journaled
dataset runs of S3, GCS and Azure Blob sources are refused, both with local output and with a bucket
destination (`-o s3://…`, `gs://…` or `az://…` and `--work-dir DIR`). The local output or work directory
needs directory sync, unavailable on Windows, to retain acknowledged work after power loss.
`swath resume DIR` cannot continue a run or finish a bucket commit's cleanup there; an already
committed, finalized dataset takes the CLI's no-work path before this guard. Local filesystem scans
can write datasets locally or to a bucket without a journal or resume; text output also works.
Diagnostic `swath diag list --no-journal` can list an object store without resume. See
[troubleshooting](troubleshooting.md#the-run-was-interrupted) and [known limits](running.md#known-limits).

## Shell completion

```bash
source <(swath completion bash)    # in ~/.bashrc; zsh and fish: swath completion --help
```

See [swath-completion](commands/swath-completion.md).
