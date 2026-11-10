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
commit (`-dirty` when `go/` has local changes). Put the binary on your `PATH`, for example
`install -m 755 swath ~/.local/bin/`.

A plain `go build ./cmd/swath` works too, but takes the revision from the Go toolchain's version-control stamp,
which a build in a git worktree may lack.

## Verify the binary

```text
$ swath --version
swath devel (11ae90e42babcd159d52e8f492460802b9e0816c)
```

`--version` prints the module version (`devel` for a build from a checkout) and the commit it was built from.
Every run report carries the same in `build`. Keep it with any run you may need to investigate.

Then run a listing that needs no cloud account:

```bash
swath du -d 1 .
```

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
