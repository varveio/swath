# Install

swath is one static binary with nothing else to install. Download a release and put `swath` on your `PATH`;
[verifying the download](#verify-the-download) first is optional.

The newest published build is swath 0.4.0, on the [releases page](https://github.com/varveio/swath/releases).
Coming from 0.3.2: the command line and the dataset format are new
([moving from 0.3.2](https://github.com/varveio/swath/releases/tag/v0.4.0)).

## Install a release

For 0.4.0, the archive for each platform is:

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

Each block below downloads the archive for your machine, extracts it, installs `swath` and runs the installed
binary's `--version`. It needs no account and no other tools.

### Linux

```bash
VERSION=0.4.0
ARCH=$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')
URL=https://github.com/varveio/swath/releases/download/v$VERSION
ARCHIVE=swath-$VERSION-linux-$ARCH.tar.gz
curl -fLO "$URL/$ARCHIVE"
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

The block picks the Apple silicon or the Intel archive:

```bash
VERSION=0.4.0
ARCH=$(uname -m | sed 's/x86_64/amd64/')
URL=https://github.com/varveio/swath/releases/download/v$VERSION
ARCHIVE=swath-$VERSION-darwin-$ARCH.tar.gz
curl -fLO "$URL/$ARCHIVE"
tar -xzf "$ARCHIVE" &&
mkdir -p ~/.local/bin &&
install -m 755 "swath-$VERSION-darwin-$ARCH/swath" ~/.local/bin/ &&
"$HOME/.local/bin/swath" --version
```

Then put `~/.local/bin` on your `PATH` as on [Linux](#linux) (zsh, the macOS default shell, reads
`~/.zshrc`), and check with `command -v swath && swath --version`. The macOS binaries are not yet signed with
an Apple Developer ID or notarized ([#420](https://github.com/varveio/swath/issues/420)). A download with
`curl`, as above, runs as it is. Gatekeeper refuses a binary downloaded with a browser: clear its quarantine
attribute, `xattr -d com.apple.quarantine swath-0.4.0-darwin-ARCH/swath`, with `arm64` on Apple silicon
and `amd64` on Intel.

### Windows

In PowerShell:

```powershell
$Version = "0.4.0"
$Url = "https://github.com/varveio/swath/releases/download/v$Version"
$Archive = "swath-$Version-windows-amd64.zip"
curl.exe -fLO "$Url/$Archive"
Expand-Archive $Archive -DestinationPath . -Force
$Bin = "$env:LOCALAPPDATA\Programs\swath"
New-Item -ItemType Directory -Force $Bin | Out-Null
Copy-Item "swath-$Version-windows-amd64\swath.exe" $Bin -Force
& "$Bin\swath.exe" --version
```

`curl.exe` comes with Windows 10 (version 1803 and later) and Windows 11; write it with `.exe`, because in
Windows PowerShell `curl` is another name for `Invoke-WebRequest`. Then add `%LOCALAPPDATA%\Programs\swath`
to your user `PATH` (Settings, "Edit environment variables for your account") and open a new terminal.

## Verify the download

Verifying is optional. Do it when you need proof that the archive is the one swath's release workflow built
and published, for example before installing it on shared or production machines. Verify after downloading
and before installing: run your platform's install block up to and including its `curl` line, then the
checks below in the same shell and directory, then the rest of the block. If a check fails, do not install
the archive.

The checks are listed lightest first. Start by downloading `SHA256SUMS` and its signature bundle next to the
archive:

```bash
curl -fL -O "$URL/SHA256SUMS" -O "$URL/SHA256SUMS.sigstore.json"
```

On Windows, in PowerShell:

```powershell
curl.exe -fL -O "$Url/SHA256SUMS" -O "$Url/SHA256SUMS.sigstore.json"
```

### Checksum

Needs nothing else installed. It proves that the archive is the file `SHA256SUMS` lists, not a damaged or
altered copy; the [signature](#signature) proves who published `SHA256SUMS`.

On Linux, it prints the archive's name and `OK`:

```bash
sha256sum --check --ignore-missing SHA256SUMS
```

On macOS:

```bash
grep " $ARCHIVE\$" SHA256SUMS | shasum -a 256 --check
```

On Windows, `Get-FileHash` computes the checksum:

```powershell
$Sum = Select-String -Path SHA256SUMS -SimpleMatch "  $Archive"
$Ok = $Sum -and ((Get-FileHash $Archive -Algorithm SHA256).Hash -eq $Sum.Line.Split(" ")[0])
if ($Ok) { "OK" } else { Write-Error "checksum mismatch: do not use $Archive" }
```

### Signature

Needs [cosign](https://docs.sigstore.dev/). It proves that `SHA256SUMS` was signed by swath's release
workflow for this release's tag, so with the checksum it ties the archive to that workflow. It prints
`Verified OK`:

```bash
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
```

On Windows, in PowerShell:

```powershell
cosign verify-blob --bundle SHA256SUMS.sigstore.json `
  --certificate-identity "https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/v$Version" `
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
```

### Build attestation

Needs the [GitHub CLI](https://cli.github.com/) (`gh`), logged in (`gh auth login`); without a login it fails
with exit 4. It checks the archive itself against the attestation swath's release workflow recorded for it,
which names the build that produced it:

```bash
gh attestation verify "$ARCHIVE" --repo varveio/swath
```

On Windows, `gh attestation verify $Archive --repo varveio/swath`.

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

You need Go 1.26.3 or later and git:

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

Release binaries run on Linux (x86-64 and ARM64), macOS (Intel and Apple silicon) and Windows (x86-64),
with the same commands everywhere: dataset runs of S3, GCS and Azure Blob sources keep their journal and
`swath resume` continues them on all three. A journaled run needs an output on a filesystem that syncs
directories; on Windows swath checks this when the run starts and refuses one that cannot
([known limits](troubleshooting.md#known-limits)).

## Shell completion

```bash
source <(swath completion bash)    # in ~/.bashrc; zsh and fish: swath completion --help
```

Next: [Your first inventory](first-inventory.md).
