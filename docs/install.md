# Install & quickstart

`swath` ships three ways once a release exists: a Docker image, an uber-jar,
and JDK-requiring application-distribution archives. All three are planned to be built from the
same Gradle output and signed with [sigstore](https://www.sigstore.dev/)
keyless signing (GitHub OIDC, no key custody).
**None of that exists yet.** Tags in the current private repository run the
release build and upload its artifacts, but do not publish until the Phase 10
human acceptance gate makes the repository public and enables publication.
Until then,
**build from source** (below) is the only path; this page states the shape
the other install paths will take so we don't have to change this doc again
once they land.

## Docker

```
docker run --rm -v "$PWD/out:/out" <repo>/swath \
  list s3://my-bucket/prefix/ --no-sign-request \
  --format parquet -o /out/data
```

Replace `<repo>/swath` with the published image reference once one exists.
Drop `--no-sign-request` and add credentials for a private bucket. See
[`packaging-and-docker.md`](packaging-and-docker.md) for the image's signal
handling, tag scheme, and how to pin a digest rather than a mutable tag.

## Uber-jar

```
java -jar swath.jar list s3://my-bucket/prefix/ --format parquet -o out/
```

A single self-contained file — nothing else needs to be on the classpath.
It needs a **JDK 25 runtime** (`ScopedValue`, JEP 506, is a shipped
non-preview API the engine depends on for runtime context propagation) and
runs with **no `--enable-preview` flag** — a plain `java -jar` invocation is
enough. See [`packaging-and-docker.md`](packaging-and-docker.md) for how the
jar is built and what it bundles.

## Application distribution archive

After public-release activation, the tagged-release workflow attaches the
`distZip` and `distTar` application archives, a
`SHA256SUMS` file, an SPDX SBOM, and a keyless Sigstore bundle for every shipped
jar/archive. The archives contain Gradle application launchers and require a
JDK 25 runtime; they are not native per-platform binaries. Verify the checksum before use; with
[`cosign`](https://github.com/sigstore/cosign), verify an archive's adjacent
bundle with `cosign verify-blob --bundle <archive>.sigstore.json <archive>`.
The workflow also keylessly signs the immutable GHCR image digest.

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
