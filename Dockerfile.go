# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
#
# The Go images, one target each: `swath` (the lister, the default target) and `swath-replay`
# (the replay server). Each holds its static binary and nothing it does not need:
#
#   docker build -f Dockerfile.go --build-arg SWATH_REVISION="$(git rev-parse HEAD)" -t swath:dev .
#   docker run --rm -v "$PWD/out:/out" swath:dev list s3://bucket/prefix/ -o /out/inventory
#   docker build -f Dockerfile.go --target swath-replay \
#     --build-arg SWATH_REVISION="$(git rev-parse HEAD)" -t swath-replay:dev .
#   docker run --rm -p 127.0.0.1:19090:19090 -v /data/sorted:/fixtures:ro swath-replay:dev \
#     serve --fixture /fixtures --bucket bkt --host 0.0.0.0 --port 19090
#
# The build stage runs on the build platform and cross-compiles both commands for
# TARGETOS/TARGETARCH (CGO_ENABLED=0 through go/build.sh), so a foreign-architecture image
# needs no emulation. It writes them to /out/TARGETOS-TARGETARCH/, and their third-party notices
# (go/notices.sh, which fails on a disallowed license) to /out/THIRD_PARTY_NOTICES. The release
# workflow replaces the stage with the contents of its release archives (`build-contexts:
# build=DIR`, DIR holding out/linux-amd64/, out/linux-arm64/ and out/THIRD_PARTY_NOTICES), so the
# images carry the archived bytes. Each image holds LICENSE, NOTICE and THIRD_PARTY_NOTICES in
# /licenses/.
#
# Neither runtime stage has a RUN, so a multi-architecture build pulls nothing and emulates
# nothing. Both run as 65532:65532, distroless's `nonroot` user, and the binary is owned by
# root and not writable by it. In the lister image that user has a passwd entry and a home
# directory (/home/nonroot), so credential files mounted under ~ (~/.aws, ~/.config/gcloud,
# ~/.azure) are found the same way as on a host. The replay runtime is `scratch`: the server
# speaks plain HTTP (no CA certificates), formats every time in UTC (no zone data) and needs no
# home directory or /etc/passwd, so it runs as the same numeric user, and there
# is nothing in the image but the binary to patch or for a scanner to flag, and nothing to
# exec into. The lister calls S3, GCS and Azure over HTTPS, so its runtime is distroless
# static: CA certificates, zone data and /etc/passwd, still no shell or package manager.
#
# Build arguments: SWATH_VERSION is the version `--version` prints (the release's X.Y.Z;
# without it, devel) and SWATH_REVISION the commit (the context carries no .git, so without
# it the binary names no revision). GOFLAGS passes through to the Go toolchain
# (`--build-arg GOFLAGS=-p=2` bounds the compiler's parallelism on a shared host); GOPROXY
# names a module mirror where proxy.golang.org is not reachable (go.sum still verifies every
# module).
#
# The builder's Go is the `toolchain` line of go/go.mod (go1.27.4), digest-pinned, so the image
# and CI compile with the same release: actions/setup-go reads that line from go-version-file.
# Raise both together. GOTOOLCHAIN=local keeps the builder from downloading another.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:08bcb99a38f85a265f9a93d1d3323e2440eec333380f3ea26ee0dec9264bb41c AS build
ENV GOTOOLCHAIN=local
WORKDIR /src/go

# Modules first, so a source change reuses the downloaded module layer.
COPY go/go.mod go/go.sum ./
# The reader module, which go.mod replaces with ./reader, is part of the module graph.
COPY go/reader/go.mod go/reader/go.sum ./reader/
ARG GOPROXY=https://proxy.golang.org,direct
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY go/ ./
ARG GOFLAGS=
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    mkdir -p /out && ./notices.sh /out/THIRD_PARTY_NOTICES
ARG TARGETOS
ARG TARGETARCH
ARG SWATH_VERSION=
ARG SWATH_REVISION=
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    for cmd in swath swath-replay; do \
      GOOS=$TARGETOS GOARCH=$TARGETARCH ./build.sh -o "/out/$TARGETOS-$TARGETARCH/$cmd" "./cmd/$cmd" || exit; \
    done

FROM scratch AS swath-replay
ARG TARGETOS
ARG TARGETARCH
COPY --from=build --chmod=0555 /out/${TARGETOS}-${TARGETARCH}/swath-replay /opt/swath-replay/swath-replay
COPY --chmod=0444 LICENSE NOTICE /licenses/
COPY --from=build --chmod=0444 /out/THIRD_PARTY_NOTICES /licenses/
USER 65532:65532
WORKDIR /opt/swath-replay
ENTRYPOINT ["/opt/swath-replay/swath-replay"]

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS swath
ARG TARGETOS
ARG TARGETARCH
COPY --from=build --chmod=0555 /out/${TARGETOS}-${TARGETARCH}/swath /opt/swath/swath
COPY --chmod=0444 LICENSE NOTICE /licenses/
COPY --from=build --chmod=0444 /out/THIRD_PARTY_NOTICES /licenses/
USER 65532:65532
WORKDIR /opt/swath
ENTRYPOINT ["/opt/swath/swath"]
