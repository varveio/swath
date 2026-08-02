# Install & quickstart

`swath` ships three ways: a Docker image, an uber-jar, and JDK-requiring
application-distribution archives. All three are built from the same Gradle
output by the tagged-release workflow, signed with
[sigstore](https://www.sigstore.dev/) keyless signing (GitHub OIDC, no key
custody), and attached to the
[releases page](https://github.com/varveio/swath/releases) — pick the newest
`vX.Y.Z`. Building from source (below) remains fully supported.

## Docker

```
docker run --rm -v "$PWD/out:/out" ghcr.io/varveio/swath \
  list s3://my-bucket/prefix/ --no-sign-request \
  --format parquet -o /out/data
```

Images are published to `ghcr.io/varveio/swath`; `:latest` and the `X.Y.Z`
semver tags are owned solely by releases, and a bare image name resolves to
`:latest`. Drop `--no-sign-request` and add credentials for a private bucket. See
[`packaging-and-docker.md`](packaging-and-docker.md) for the image's signal
handling, tag scheme, and how to pin a digest rather than a mutable tag.

## Uber-jar

Download `swath-X.Y.Z.jar` from the
[releases page](https://github.com/varveio/swath/releases), then:

```
java -jar swath-X.Y.Z.jar list s3://my-bucket/prefix/ --format parquet -o out/
```

A single self-contained file — nothing else needs to be on the classpath.
It needs a **JDK 25 runtime** (`ScopedValue`, JEP 506, is a shipped
non-preview API the engine depends on for runtime context propagation) and
runs with **no `--enable-preview` flag** — a plain `java -jar` invocation is
enough. See [`packaging-and-docker.md`](packaging-and-docker.md) for how the
jar is built and what it bundles.

## Application distribution archive

The tagged-release workflow attaches the
`distZip` and `distTar` application archives, a
`SHA256SUMS` file, an SPDX SBOM, and a keyless Sigstore bundle for every shipped
asset. The archives contain Gradle application launchers and require a
JDK 25 runtime; they are not native per-platform binaries. See
[Verifying a download](#verifying-a-download) below.

## Build from source

The fallback today, not the headline:

```
./gradlew build
./gradlew :swath-cli:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PATH"
swath list s3://my-bucket/prefix/ --format parquet -o out/
```

Needs a JDK; the Gradle wrapper (`./gradlew`) handles the rest, no system
Gradle install required. See [`packaging-and-docker.md`](packaging-and-docker.md)
for the full build/package reference (uber-jar, `installDist`, Docker) and
[`internals/build-and-modules.md`](internals/build-and-modules.md) for the
module graph.

## Quickstart

Once you have a `swath` binary (any of the above), point it at a bucket and
look at the result on your terminal — no flags beyond the target:

```
swath list s3://my-bucket/prefix/ --no-sign-request
```

On a terminal this prints an aligned table; piped to a file or another
program it switches to TSV instead (`--format auto`, the default). Drop
`--no-sign-request` to list a bucket you have credentials for — swath then
uses the standard AWS credential chain (environment, profile, instance role;
see [`faq.md`](faq.md)).

For real output you'll keep, write a Parquet dataset:

```
swath list s3://my-bucket/prefix/ --no-sign-request -o out/ --format parquet
```

This produces a directory dataset — `out/data/*.parquet` plus
`out/manifest.json` — that any Parquet reader (DuckDB, Athena, Spark, Trino)
queries directly, no merge step needed. If the run gets interrupted,
`swath resume out/` picks up from the last committed cursor instead of
re-listing from scratch. See [`usage.md`](usage.md) for the full flag
reference and [`configuration.md`](configuration.md) for every flag/knob's
default at a glance.

## Verifying a download

Every release asset is signed with keyless [Sigstore](https://www.sigstore.dev/). The jar,
the archives and `SHA256SUMS` additionally carry a SLSA build-provenance attestation, as does
the container image; the SBOM is signed but not separately attested, since `SHA256SUMS`
already covers it. The same commands below run inside the release pipeline against the
uploaded assets before it publishes, so a release that cannot be verified never goes out.

`IDENTITY` names the workflow that built the release — substituting a different tag or repo
is the point of the check, so paste it exactly.

```sh
TAG=v0.2.0   # the release you downloaded
IDENTITY="https://github.com/varveio/swath/.github/workflows/release.yml@refs/tags/${TAG}"
ISSUER=https://token.actions.githubusercontent.com

sha256sum --check SHA256SUMS

cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" SHA256SUMS

gh attestation verify SHA256SUMS --repo varveio/swath
```

Checking `SHA256SUMS` is enough for the whole release: its signature makes the file
trustworthy, and the file covers every other asset. For the container image, verify the
immutable digest rather than a tag:

```sh
cosign verify --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" \
  ghcr.io/varveio/swath@sha256:<digest>

gh attestation verify oci://ghcr.io/varveio/swath@sha256:<digest> --repo varveio/swath
```

## Running the container: two things that surprise people

**Write permissions.** The image runs as a non-root numeric UID (10001), so a host output
directory you own is *not* writable by it — the run fails with
`AccessDeniedException`. Rather than loosening permissions with `chmod 777`, run the
container as yourself, which also leaves the output owned by you:

```sh
mkdir -p /tmp/swath-out
docker run --rm --user "$(id -u):$(id -g)" -v /tmp/swath-out:/out \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ --no-sign-request --format parquet -o /out/data
```

The output path must be inside the mount: `-o /out/data` lands in `/tmp/swath-out/data`.
Anywhere else and the results stay in the container and vanish with `--rm`.

**A short run looks silent.** Progress goes to stderr, and off a terminal it is *appended*
every 30s rather than redrawn — so a run that finishes in ten seconds prints nothing until
the summary. Pass `--progress-interval 2s` for periodic records in a log, or `docker run -t`
to get the live redrawing display (a real terminal is required for that; `--progress` alone
cannot force control sequences onto a stream that cannot act on them).
