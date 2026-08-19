# Installation

Tagged releases provide a multi-architecture container image, a self-contained jar,
and application archives. Use the [GitHub releases page](https://github.com/varveio/swath/releases)
for the current version.

After installation, continue with [getting started](getting-started.md).

## Docker

Docker includes the required Java runtime:

```bash
docker pull ghcr.io/varveio/swath:latest
docker run --rm ghcr.io/varveio/swath:latest --version
```

Use a version tag or, for reproducible automation, the immutable digest printed on the
GitHub release. The image supports `linux/amd64` and `linux/arm64` and runs as non-root
UID 10001. When writing to a host mount, either make it writable by that UID or run the
container as your current user:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest --version
```

See the [Docker reference](packaging-and-docker.md#5-docker-image) for credentials,
signals, JVM options, local builds, and multi-architecture details.

## Self-contained jar

Download `swath-X.Y.Z.jar` from the release page. It requires a JDK 25 runtime:

```bash
java -jar swath-X.Y.Z.jar --version
java -jar swath-X.Y.Z.jar list s3://my-bucket/prefix/ --format parquet -o out/
```

The jar carries the runtime dependency classpath; nothing else needs to be installed.
swath uses final JDK 25 APIs and does not require `--enable-preview`.

## Application archive

The `distZip` and `distTar` release assets contain `bin/swath` launchers and a `lib/`
directory. They also require JDK 25. Extract one, then add its `bin/` directory to
`PATH` or invoke the launcher directly.

The launcher honors `SWATH_OPTS` and `JAVA_OPTS`. The jar and Docker image invoke Java
directly; use `JAVA_TOOL_OPTIONS` with those forms.

## Build from source

The Gradle wrapper handles the build; no system Gradle installation is needed. With
JDK 25 available:

```bash
./gradlew build
./gradlew :swath-cli:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PATH"
swath --version
```

`./gradlew build` is the integration gate, including LocalStack tests. For contributor
test tiers and a Docker-free inner loop, see [Testing](ops/dev/TESTING.md). The
[packaging reference](packaging-and-docker.md) documents the jar, launcher, and image
builds in detail.

<a id="verifying-a-download"></a>

## Verify a release

Every release ships `SHA256SUMS`, a Sigstore bundle, an SPDX SBOM, and build-provenance
attestations. Set `TAG` to the release you downloaded:

```bash
TAG=v0.2.4
IDENTITY="https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/${TAG}"
ISSUER=https://token.actions.githubusercontent.com

sha256sum --check SHA256SUMS
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" SHA256SUMS
gh attestation verify SHA256SUMS --repo varveio/swath
```

The signed checksum file covers the jar, archives, and other release assets. Verify a
container by digest rather than by its mutable tag:

```bash
cosign verify --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" \
  ghcr.io/varveio/swath@sha256:<digest>
gh attestation verify oci://ghcr.io/varveio/swath@sha256:<digest> \
  --repo varveio/swath
```

The release workflow runs these verification paths before publication.
