# Install

swath is one static binary with no runtime to install. It is not released yet (the latest published release,
v0.3.2, has another command line and dataset format: [upgrade.md](upgrade.md)), so build it from source.

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

## Planned for the next release

No release of this command line exists yet. The release workflow is set up to publish, per release:

- one archive per platform (`swath-X.Y.Z-OS-ARCH.tar.gz` for linux and darwin, amd64 and arm64;
  `swath-X.Y.Z-windows-amd64.zip`), each holding `swath`, `swath-replay`, `LICENSE`, `NOTICE` and
  `THIRD_PARTY_NOTICES`, with an SPDX SBOM and a signed `SHA256SUMS`
  ([RELEASING.md](../../RELEASING.md#verifying-a-release) has the verification commands);
- multi-arch images (linux/amd64, linux/arm64): `ghcr.io/varveio/swath` (distroless static, entrypoint
  `/opt/swath/swath`) and `ghcr.io/varveio/swath-replay` (`scratch`,
  [swath-replay](internals/swath-replay.md#container-image)), both running as `65532:65532`.

This section will describe downloading and verifying them once a release exists. Until then, the image can be
built from the repository root of a checkout: `docker build -f Dockerfile.go -t swath:dev .` (`--target swath-replay` for the replay
server; `--build-arg SWATH_REVISION="$(git rev-parse HEAD)"` stamps the commit, as the build context carries no
`.git`). The build needs the network: it downloads the modules and generates `THIRD_PARTY_NOTICES`, failing on
a module whose license is not allowed ([license gate](internals/testing.md#license-gate)). The image runs as
`65532:65532` with its home directory at `/home/nonroot`: mount a directory that user can write for `-o`, and
pass credentials through the environment or a mounted credential directory:

```bash
docker run --rm -v "$PWD/out:/out" swath:dev \
  list s3://bucket/prefix/ --anonymous --s3-region us-east-1 -o /out/inventory
```

## Platforms

`build.sh` cross-compiles with `GOOS` and `GOARCH`:

```bash
GOOS=linux  GOARCH=arm64 ./build.sh -o swath-linux-arm64
GOOS=darwin GOARCH=arm64 ./build.sh -o swath-darwin-arm64
GOOS=darwin GOARCH=amd64 ./build.sh -o swath-darwin-amd64
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
