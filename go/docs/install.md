# Install

swath is one static binary with no runtime to install. The Go implementation is not released yet (the latest
published release is the Java v0.3.2, with another command line and dataset format), so build it from source.

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

## Release downloads and images

From 0.4, each [GitHub release](https://github.com/varveio/swath/releases) carries one archive per platform:
`swath-X.Y.Z-linux-amd64.tar.gz`, `-linux-arm64.tar.gz`, `-darwin-arm64.tar.gz` and `-windows-amd64.zip`. Each
holds `swath`, `swath-replay`, `LICENSE`, `NOTICE` and `THIRD_PARTY_NOTICES` (the licenses of every module linked
into the binaries) in a `swath-X.Y.Z-OS-ARCH/` directory. The release also carries an SPDX SBOM and a signed
`SHA256SUMS`. Verify before running ([RELEASING.md](../../RELEASING.md#verifying-a-release) has the signature and
attestation commands):

```bash
tar -xzf swath-X.Y.Z-linux-amd64.tar.gz
sha256sum --check --ignore-missing SHA256SUMS
./swath-X.Y.Z-linux-amd64/swath --version    # swath X.Y.Z (REVISION)
```

The same release publishes multi-arch images (linux/amd64, linux/arm64) with the archives' Linux binaries and
the three legal files in `/licenses/`:

| Image | Entrypoint | Base |
|---|---|---|
| `ghcr.io/varveio/swath` | `/opt/swath/swath` | distroless static (CA certificates, zone data); no shell |
| `ghcr.io/varveio/swath-replay` | `/opt/swath-replay/swath-replay` ([swath-replay](internals/swath-replay.md#container-image)) | `scratch` |

Tags are `X.Y.Z`, `X.Y` and `latest` (and `X` from 1.0); a release candidate publishes only `X.Y.Z-rc.N`. Pin
the digest for anything that must not move. Both images run as `65532:65532` (distroless's `nonroot`): mount a
directory that user can write for `-o`. The lister's home directory is `/home/nonroot`: pass credentials
through the environment, or mount the provider's credential directory there
(`-v ~/.aws:/home/nonroot/.aws:ro`), its files readable by that user:

```bash
docker run --rm -v "$PWD/out:/out" ghcr.io/varveio/swath:X.Y.Z \
  list s3://bucket/prefix/ --anonymous --s3-region us-east-1 -o /out/inventory
```

`docker build -f Dockerfile.go -t swath:dev .` builds the image from a checkout (`--target swath-replay` for the
replay server); `--build-arg SWATH_REVISION="$(git rev-parse HEAD)"` stamps the commit, as the build context
carries no `.git`. The build needs the network: it downloads the modules and generates `THIRD_PARTY_NOTICES`,
failing on a module whose license is not allowed ([license gate](internals/testing.md#license-gate)).

## Platforms

`build.sh` cross-compiles with `GOOS` and `GOARCH`:

```bash
GOOS=linux  GOARCH=arm64 ./build.sh -o swath-linux-arm64
GOOS=darwin GOARCH=arm64 ./build.sh -o swath-darwin-arm64
GOOS=windows GOARCH=amd64 ./build.sh -o swath.exe
```

The test suite runs on Linux (amd64 and arm64), macOS (arm64) and Windows (amd64). On Windows a dataset run of
an object store, which keeps a journal, is refused, and so is `swath resume`: Windows has no directory sync, so
a power loss could lose parts swath had recorded as written. Local trees and text output work there
([running.md](running.md#known-limits)).

## Shell completion

```bash
source <(swath completion bash)    # in ~/.bashrc; zsh and fish: swath completion --help
```

See [swath-completion](commands/swath-completion.md).
