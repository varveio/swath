# Install

swath is one static binary with nothing else to install. Download a release, verify it, and put `swath` on
your `PATH`.

The newest published build is swath 0.4.0, on the [releases page](https://github.com/varveio/swath/releases).
Versions 0.3.x were a different tool, with another command line and dataset format
([release notes](https://github.com/varveio/swath/releases/tag/v0.4.0)).

## Download a release

For 0.4.0, choose the archive for your platform:

| Platform | Archive |
|---|---|
| Linux, x86-64 | [`swath-0.4.0-linux-amd64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0/swath-0.4.0-linux-amd64.tar.gz) |
| Linux, ARM64 | [`swath-0.4.0-linux-arm64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0/swath-0.4.0-linux-arm64.tar.gz) |
| macOS, Intel | [`swath-0.4.0-darwin-amd64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0/swath-0.4.0-darwin-amd64.tar.gz) |
| macOS, Apple silicon | [`swath-0.4.0-darwin-arm64.tar.gz`](https://github.com/varveio/swath/releases/download/v0.4.0/swath-0.4.0-darwin-arm64.tar.gz) |
| Windows, x86-64 | [`swath-0.4.0-windows-amd64.zip`](https://github.com/varveio/swath/releases/download/v0.4.0/swath-0.4.0-windows-amd64.zip) |

Each archive holds `swath`, `swath-replay` (a replay server for testing; `.exe` on Windows), `LICENSE`,
`NOTICE` and `THIRD_PARTY_NOTICES` in a `swath-0.4.0-OS-ARCH/` directory. The release also carries
[`SHA256SUMS`](https://github.com/varveio/swath/releases/download/v0.4.0/SHA256SUMS), an SPDX SBOM ([`swath-0.4.0.spdx.json`](https://github.com/varveio/swath/releases/download/v0.4.0/swath-0.4.0.spdx.json))
and a keyless cosign signature bundle (`.sigstore.json`) for each of them.

Each block below downloads with the [GitHub CLI](https://cli.github.com/) (`gh`), verifies, extracts, installs
and runs the installed binary's `--version`. A failed check stops it before anything is extracted or installed.
The cosign step proves that `SHA256SUMS` came from swath's release workflow, the checksum proves the archive matches it, and
the attestation records the build that produced it.

### Linux

With the [GitHub CLI](https://cli.github.com/) (`gh`) and [cosign](https://docs.sigstore.dev/) installed:

```bash
VERSION=0.4.0
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
"$HOME/.local/bin/swath" --version
```

The last line prints the version and the commit it was built from, a 40-digit hash in place of `COMMIT`:

```text
swath 0.4.0 (COMMIT)
```

Every run report carries the same in `build`; keep it with any result you may need to investigate.

To run it as `swath`, put `~/.local/bin` first on your `PATH`: add this line to your shell's startup file
(`~/.bashrc` for bash, `~/.zshrc` for zsh), and run it in the current shell too. Then check that `swath`
is the binary you installed, not an older one elsewhere on your `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"
command -v swath && swath --version
```

### macOS

With the GitHub CLI and cosign installed (`brew install gh cosign`); the block picks the Apple silicon or the
Intel archive:

```bash
VERSION=0.4.0
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
"$HOME/.local/bin/swath" --version
```

Then put `~/.local/bin` on your `PATH` as on [Linux](#linux) (zsh, the macOS default shell, reads
`~/.zshrc`), and check with `command -v swath && swath --version`. The macOS binaries are not yet signed with
an Apple Developer ID or notarized ([#420](https://github.com/varveio/swath/issues/420)). A download with `gh` or `curl`, as above,
runs as it is. Gatekeeper refuses a binary downloaded with a browser: after verifying, clear its quarantine
attribute, `xattr -d com.apple.quarantine swath-0.4.0-darwin-ARCH/swath`, with `arm64` on Apple silicon
and `amd64` on Intel.

### Windows

In PowerShell, with the GitHub CLI installed:

```powershell
$Version = "0.4.0"
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
on `SHA256SUMS`. To check that too, install cosign for Windows and run this in the same PowerShell session and
directory; do not use the binary if it fails:

```powershell
cosign verify-blob --bundle SHA256SUMS.sigstore.json `
  --certificate-identity "https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/v$Version" `
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
```

On Windows, dataset runs of S3, GCS and Azure Blob sources are refused ([platforms](#platforms)).

## Container images

Each release publishes a multi-arch image for linux/amd64 and linux/arm64, built from the same binaries:

```bash
docker run --rm -v "$PWD:/data:ro" ghcr.io/varveio/swath:0.4.0 du -d 1 /data
```

The image `ghcr.io/varveio/swath:0.4.0` is distroless, with no shell; its entrypoint is
`/opt/swath/swath` and it runs as user `65532:65532`, which cannot write your directories or read your
`0600` credential files. Run it as yourself instead, and point the AWS SDK at the mounted files:

```bash
mkdir -p "$PWD/out"    # yours: a directory Docker creates for the mount belongs to root
docker run --rm --user "$(id -u):$(id -g)" \
  -v ~/.aws:/aws:ro -e AWS_SHARED_CREDENTIALS_FILE=/aws/credentials -e AWS_CONFIG_FILE=/aws/config \
  -v "$PWD/out:/out" ghcr.io/varveio/swath:0.4.0 \
  list s3://bucket/prefix/ -o /out/inventory
```

Credentials in the environment work too (`-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e
AWS_SESSION_TOKEN`). Without a shell the image cannot run `--s3-bearer-token-command` or a profile's
`credential_process` (both run under `sh -c`): use the release binary for those, or an image of your own that
adds a shell and the command. Besides `0.4.0`, a release moves the tags `0.4` and `latest`. Pin the
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
`swath`, puts the binary inside that directory instead. To run it as `swath`, install it where the release
binaries go and put that directory on your `PATH` as on [Linux](#linux):

```bash
mkdir -p ~/.local/bin && install -m 755 swath ~/.local/bin/
```

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
([known limits](troubleshooting.md#known-limits)).

## Shell completion

```bash
source <(swath completion bash)    # in ~/.bashrc; zsh and fish: swath completion --help
```

Next: [Your first inventory](first-inventory.md).
