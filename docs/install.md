# Install

swath is one static binary with nothing else to install. Download a release, verify it, and put `swath` on
your `PATH`.

The current builds are release candidates for 0.4.0, published on the
[releases page](https://github.com/varveio/swath/releases). A candidate is for trying swath before the final
release; GitHub marks it as a pre-release, so choose it explicitly. Versions 0.3.x and earlier were a
different tool, written in Java, with another command line ([changelog](../CHANGELOG.md)).

## Download a release candidate

For v0.4.0-rc.2, choose the archive for your platform:

| Platform | Archive |
|---|---|
| Linux, x86-64 | `swath-0.4.0-rc.2-linux-amd64.tar.gz` |
| Linux, ARM64 | `swath-0.4.0-rc.2-linux-arm64.tar.gz` |
| macOS, Intel | `swath-0.4.0-rc.2-darwin-amd64.tar.gz` |
| macOS, Apple silicon | `swath-0.4.0-rc.2-darwin-arm64.tar.gz` |
| Windows, x86-64 | `swath-0.4.0-rc.2-windows-amd64.zip` |

Each archive holds `swath`, `swath-replay` (a replay server for testing; `.exe` on Windows), `LICENSE`,
`NOTICE` and `THIRD_PARTY_NOTICES` in a `swath-0.4.0-rc.2-OS-ARCH/` directory. The release also carries
`SHA256SUMS`, an SPDX SBOM (`swath-0.4.0-rc.2.spdx.json`) and a keyless cosign signature bundle
(`.sigstore.json`) for each of them.

### Verify and install

On Linux, with the [GitHub CLI](https://cli.github.com/) and [cosign](https://docs.sigstore.dev/):

```bash
VERSION=0.4.0-rc.2
ARCHIVE=swath-$VERSION-linux-amd64.tar.gz
gh release download "v$VERSION" --repo varveio/swath --pattern "$ARCHIVE" --pattern 'SHA256SUMS*'

cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify "$ARCHIVE" --repo varveio/swath

tar -xzf "$ARCHIVE"
install -m 755 "swath-$VERSION-linux-amd64/swath" ~/.local/bin/
swath --version
```

The cosign step proves that `SHA256SUMS` came from swath's release workflow, the checksum proves the archive
matches it, and the attestation records the build that produced it. `swath --version` prints the version and
the commit it was built from:

```text
swath 0.4.0-rc.2 (be8768aea7e5135847b06ff604c499479187313c)
```

Every run report carries the same in `build`; keep it with any result you may need to investigate.

On macOS, `shasum -a 256 ARCHIVE` computes the hash to compare with its line in `SHA256SUMS`. The macOS
binaries are not yet signed with an Apple Developer ID or notarized
([#420](https://github.com/varveio/swath/issues/420)), so Gatekeeper refuses a binary downloaded with a
browser. After verifying, clear the quarantine attribute (`darwin-amd64` on Intel):

```bash
xattr -d com.apple.quarantine swath-0.4.0-rc.2-darwin-arm64/swath
```

A download with `gh` or `curl` carries no quarantine attribute, and `xattr` then reports "No such xattr".

On Windows, PowerShell's `Get-FileHash ARCHIVE -Algorithm SHA256` computes the hash.

## Container images

Each release publishes a multi-arch image for linux/amd64 and linux/arm64, built from the same binaries:

```bash
docker run --rm -v "$PWD:/data:ro" ghcr.io/varveio/swath:0.4.0-rc.2 du -d 1 /data
```

The image `ghcr.io/varveio/swath:0.4.0-rc.2` is distroless, with no shell; its entrypoint is
`/opt/swath/swath` and it runs as user `65532:65532`. Mount a directory that user can write for `-o`, and pass
cloud credentials through the environment or a read-only mount under `/home/nonroot`, for example
`-v ~/.aws:/home/nonroot/.aws:ro`. A release candidate publishes only its own tag, never `latest`. Pin the
image digest for runs that must use the same image; the image signature and provenance checks are in
[RELEASING.md](../RELEASING.md#verifying-a-release).

## Build from source

You need Go 1.26 or later and git:

```bash
git clone https://github.com/varveio/swath.git
cd swath
./build.sh -o swath
./swath --version
```

`./build.sh` builds a static binary stamped with the checkout's commit; its version reads `devel`. Set
`GOOS` and `GOARCH` to cross-compile. `-o` names the binary's path relative to the clone, where
`build.sh` runs. A path that is an existing directory, such as `-o ../swath` from inside a clone named
`swath`, puts the binary inside that directory instead.

Go builds in a work directory under `TMPDIR` (`/tmp` by default), and this module's dependencies need more
room than a small tmpfs `/tmp` holds: the build then fails with "no space left on device". Point the work
directory and the build cache at a disk with a few GB free:

```bash
TMPDIR=/path/on/disk GOCACHE=/path/on/disk/.gocache ./build.sh -o swath
```

On one tester's machine that build took under 4 minutes and the cache grew to about 850 MB.

`go install github.com/varveio/swath/cmd/swath@VERSION` is not
supported yet: the module points its nested `reader` and `objfile` modules at local directories, which
`go install` refuses. Build from a checkout instead.

## Platforms

Release binaries run on Linux (x86-64 and ARM64), macOS (Intel and Apple silicon) and Windows (x86-64). On
Windows, local listings and text output work, but dataset runs of S3, GCS and Azure Blob sources and
`swath resume` are refused: they need directory sync, which Windows does not provide
([known limits](running.md#known-limits)).

## Shell completion

```bash
source <(swath completion bash)    # in ~/.bashrc; zsh and fish: swath completion --help
```

Next: [Your first inventory](first-inventory.md).
