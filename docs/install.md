# Install

swath is one static binary with nothing else to install. Download a release, verify it, and put `swath` on
your `PATH`.

The current builds are release candidates for 0.4.0, published on the
[releases page](https://github.com/varveio/swath/releases). A candidate is for trying swath before the final
release; GitHub marks it as a pre-release, so choose it explicitly. Versions 0.3.x and earlier were a different
tool, written in Java, with another command line ([changelog](../CHANGELOG.md)).

> [!NOTE]
> These pages describe the upcoming 0.4.0. The newest published build is swath 0.4.0-rc.2, which does not yet
> have everything described here, such as `swath du --from-dataset`. [Differences in 0.4.0-rc.2](release-candidate.md)
> lists what behaves differently.

## Download a release candidate

For v0.4.0-rc.2, choose the archive for your platform:

| Platform | Archive |
|---|---|
| Linux, x86-64 | [`swath-0.4.0-rc.2-linux-amd64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0-rc.2/swath-0.4.0-rc.2-linux-amd64.tar.gz) |
| Linux, ARM64 | [`swath-0.4.0-rc.2-linux-arm64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0-rc.2/swath-0.4.0-rc.2-linux-arm64.tar.gz) |
| macOS, Intel | [`swath-0.4.0-rc.2-darwin-amd64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0-rc.2/swath-0.4.0-rc.2-darwin-amd64.tar.gz) |
| macOS, Apple silicon | [`swath-0.4.0-rc.2-darwin-arm64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0-rc.2/swath-0.4.0-rc.2-darwin-arm64.tar.gz) |
| Windows, x86-64 | [`swath-0.4.0-rc.2-windows-amd64.zip`](https://github.com/varveio/swath/releases/download/v0.4.0-rc.2/swath-0.4.0-rc.2-windows-amd64.zip) |

Each archive holds `swath`, `swath-replay` (a replay server for testing; `.exe` on Windows), `LICENSE`,
`NOTICE` and `THIRD_PARTY_NOTICES` in a `swath-0.4.0-rc.2-OS-ARCH/` directory. The release also carries
[`SHA256SUMS`](https://github.com/varveio/swath/releases/download/v0.4.0-rc.2/SHA256SUMS), an SPDX SBOM ([`swath-0.4.0-rc.2.spdx.json`](https://github.com/varveio/swath/releases/download/v0.4.0-rc.2/swath-0.4.0-rc.2.spdx.json))
and a keyless cosign signature bundle (`.sigstore.json`) for each of them.

Each block below downloads with the [GitHub CLI](https://cli.github.com/), verifies, extracts, installs and
runs `swath --version`. A failed check stops it before anything is extracted or installed. The cosign step
proves that `SHA256SUMS` came from swath's release workflow, the checksum proves the archive matches it, and
the attestation records the build that produced it.

### Linux

With [cosign](https://docs.sigstore.dev/) installed:

```bash
VERSION=0.4.0-rc.2
ARCH=$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')
ARCHIVE=swath-$VERSION-linux-$ARCH.tar.gz
gh release download "v$VERSION" --repo varveio/swath --pattern "$ARCHIVE" --pattern 'SHA256SUMS*' &&
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS &&
sha256sum --check --ignore-missing SHA256SUMS &&
gh attestation verify "$ARCHIVE" --repo varveio/swath &&
tar -xzf "$ARCHIVE" &&
mkdir -p ~/.local/bin &&
install -m 755 "swath-$VERSION-linux-$ARCH/swath" ~/.local/bin/ &&
swath --version
```

`~/.local/bin` must be on your `PATH` for the last line, and for `swath` to run without its full path.
`swath --version` prints the version and the commit it was built from:

```text
swath 0.4.0-rc.2 (be8768aea7e5135847b06ff604c499479187313c)
```

Every run report carries the same in `build`; keep it with any result you may need to investigate.

### macOS

With cosign installed (`brew install cosign gh`); the block picks the Apple silicon or the Intel archive:

```bash
VERSION=0.4.0-rc.2
ARCH=$(uname -m | sed 's/x86_64/amd64/')
ARCHIVE=swath-$VERSION-darwin-$ARCH.tar.gz
gh release download "v$VERSION" --repo varveio/swath --pattern "$ARCHIVE" --pattern 'SHA256SUMS*' &&
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS &&
grep " $ARCHIVE\$" SHA256SUMS | shasum -a 256 --check &&
gh attestation verify "$ARCHIVE" --repo varveio/swath &&
tar -xzf "$ARCHIVE" &&
mkdir -p ~/.local/bin &&
install -m 755 "swath-$VERSION-darwin-$ARCH/swath" ~/.local/bin/ &&
swath --version
```

`~/.local/bin` must be on your `PATH`. The macOS binaries are not yet signed with an Apple Developer ID or
notarized ([#420](https://github.com/varveio/swath/issues/420)). A download with `gh` or `curl`, as above,
runs as it is. Gatekeeper refuses a binary downloaded with a browser: after verifying, clear its quarantine
attribute, `xattr -d com.apple.quarantine swath-0.4.0-rc.2-darwin-ARCH/swath`, with `arm64` on Apple silicon
and `amd64` on Intel.

### Windows

In PowerShell, with the GitHub CLI installed:

```powershell
$Version = "0.4.0-rc.2"
$Archive = "swath-$Version-windows-amd64.zip"
gh release download "v$Version" --repo varveio/swath --pattern $Archive --pattern "SHA256SUMS*"
$Sum = Select-String -Path SHA256SUMS -SimpleMatch "  $Archive"
$Ok = $Sum -and ((Get-FileHash $Archive -Algorithm SHA256).Hash -eq $Sum.Line.Split(" ")[0])
if ($Ok) { gh attestation verify $Archive --repo varveio/swath; $Ok = $LASTEXITCODE -eq 0 }
if ($Ok) {
  Expand-Archive $Archive -DestinationPath . -Force
  $Bin = "$env:LOCALAPPDATA\Programs\swath"
  New-Item -ItemType Directory -Force $Bin | Out-Null
  Copy-Item "swath-$Version-windows-amd64\swath.exe" $Bin -Force
  & "$Bin\swath.exe" --version
} else { Write-Error "verification failed: do not use $Archive" }
```

Then add `%LOCALAPPDATA%\Programs\swath` to your user `PATH` (Settings, "Edit environment variables for your
account") and open a new terminal. This block checks the checksum and the attestation but not the signature
on `SHA256SUMS`: for that, install cosign for Windows and run the `cosign verify-blob` command from the Linux
block. On Windows, dataset runs of S3, GCS and Azure Blob sources are refused ([platforms](#platforms)).

## Container images

Each release publishes a multi-arch image for linux/amd64 and linux/arm64, built from the same binaries:

```bash
docker run --rm -v "$PWD:/data:ro" ghcr.io/varveio/swath:0.4.0-rc.2 du -d 1 /data
```

The image `ghcr.io/varveio/swath:0.4.0-rc.2` is distroless, with no shell; its entrypoint is
`/opt/swath/swath` and it runs as user `65532:65532`, which cannot write your directories or read your
`0600` credential files. Run it as yourself instead, and point the AWS SDK at the mounted files:

```bash
docker run --rm --user "$(id -u):$(id -g)" \
  -v ~/.aws:/aws:ro -e AWS_SHARED_CREDENTIALS_FILE=/aws/credentials -e AWS_CONFIG_FILE=/aws/config \
  -v "$PWD/out:/out" ghcr.io/varveio/swath:0.4.0-rc.2 \
  list s3://bucket/prefix/ -o /out/inventory
```

Credentials in the environment work too (`-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e
AWS_SESSION_TOKEN`). A release candidate publishes only its own tag, never `latest`. Pin the
image digest for runs that must use the same image; the image signature and provenance checks are in
[Releasing swath](../RELEASING.md#verifying-a-release).

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
