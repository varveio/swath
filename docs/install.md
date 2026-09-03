# Installation

Tagged releases provide a multi-architecture container image, a runnable JAR, and
application archives. Use the [GitHub releases page](https://github.com/varveio/swath/releases)
for the current version.

Docker is the recommended way to try swath because it includes the required Java runtime.
After installation, continue with [Getting started](getting-started.md).

| If you want to… | Choose | You need |
| --- | --- | --- |
| Try swath without installing Java | Docker | Docker with Linux-container support |
| Run one downloadable Java artifact | Runnable JAR | JDK 25 |
| Install a `swath` launcher and dependency directory | Application archive | JDK 25 |
| Build or contribute | Source build | JDK 25; Docker for the integration gate |

## Docker

```bash
docker pull ghcr.io/varveio/swath:0.3.2
docker run --rm ghcr.io/varveio/swath:0.3.2 --version
```

`--version` prints the swath version and commit; use its output for support requests and
reproduction reports. A stable tagged release also publishes a rolling `X.Y` tag and
`latest`; prefer the exact `X.Y.Z` tag shown above for reproducible examples, and prefer
the immutable digest over any tag for reproducible automation. See [Tags &
versioning](packaging-and-docker.md#tags--versioning) for the full tag policy.

The image supports `linux/amd64` and `linux/arm64` and runs as non-root UID 10001. The
[getting-started guide](getting-started.md) shows writable output mounts for Linux, macOS,
and Windows PowerShell.

For private credentials and writable output mounts, see
[Credentials in Docker](operating.md#credentials-in-docker). For image construction,
multi-architecture publishing, and release internals, see
[Packaging and release engineering](packaging-and-docker.md).

## Runnable JAR

Download `swath-X.Y.Z.jar` from the release page. It contains swath and its Java
dependencies, but it still requires a JDK 25 runtime:

```bash
java -jar swath-X.Y.Z.jar --version
java -jar swath-X.Y.Z.jar \
  list s3://my-bucket/prefix/ \
  --format parquet -o out/
```

swath uses final JDK 25 APIs and does not require `--enable-preview`.

## Application archive

The `distZip` and `distTar` release assets contain a `bin/swath` launcher and a `lib/`
directory. They also require JDK 25. Extract one, then add its `bin/` directory to `PATH`
or invoke the launcher directly.

The launcher honors `SWATH_OPTS` and `JAVA_OPTS`. The JAR and Docker image invoke Java
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

`./gradlew build` is the integration gate and includes LocalStack tests. For the
Docker-free contributor loop and opt-in test tiers, see
[Testing](ops/dev/TESTING.md).

## Check the install

`--version` prints the version, commit, and Java runtime without contacting object
storage, using the invocation shown for your install path above: `docker run --rm
ghcr.io/varveio/swath:0.3.2 --version`, `java -jar swath-X.Y.Z.jar --version`, or plain
`swath --version` once a launcher is on `PATH`. Include this output when filing a
support request or reproduction report.

<a id="verifying-a-download"></a>

## Verify a release

Every release ships checksums, Sigstore bundles, an SPDX SBOM, and build-provenance
attestations. Set `TAG` and `VERSION` to the release you downloaded:

```bash
TAG=vX.Y.Z
VERSION=X.Y.Z
IDENTITY="https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/${TAG}"
ISSUER=https://token.actions.githubusercontent.com

sha256sum --check SHA256SUMS
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  SHA256SUMS

gh attestation verify SHA256SUMS --repo varveio/swath
gh attestation verify "swath-${VERSION}.jar" --repo varveio/swath
```

The signed checksum file covers the JAR, application archives, SBOM, and other release
assets.

Verify a container by digest rather than by its mutable tag:

```bash
cosign verify \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  ghcr.io/varveio/swath@sha256:<digest>

gh attestation verify \
  oci://ghcr.io/varveio/swath@sha256:<digest> \
  --repo varveio/swath
```

The release workflow exercises these verification paths before publication.
