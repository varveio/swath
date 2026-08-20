# Packaging and release engineering

This is the contributor reference for building, packaging, testing, and publishing the
self-contained jar, application archives, and Docker image. It is not the installation or
container-usage guide. To run a release, use [Installation](install.md); for credentials,
output mounts, and production object-store access, use
[Operating swath](operating.md).

For the module and dependency graph behind the artifacts, see
[`build-and-modules.md`](internals/build-and-modules.md).

## 1. Overview

`swath` is a Java 25 object-store lister. One Gradle build produces three
equivalent ways to run it:

- an **uber-jar** (`swath.jar`) — `java -jar swath.jar ...`,
- an **`installDist` tree** (`bin/swath` + `lib/`) — a launcher script plus
  its dependency jars, and
- a **Docker image** — a container that runs the uber-jar.

All three are built from the same `swath-cli` module and the same compiled
classes; nothing is compiled twice.

## 2. Prerequisites

- **JDK 25.** The build targets JDK 25 as a hard floor because `ScopedValue`
  — used as a shipped (non-preview) API in the engine's runtime context
  propagation — only became final in JDK 25 (JEP 506). Shipped
  artifacts run with **no `--enable-preview` flag**; every distribution below
  is a plain, flagless `java` invocation.
- **The Gradle wrapper** — no system Gradle install is required. The
  repository pins **Gradle 9.0.0** (`gradle/wrapper/gradle-wrapper.properties`);
  always invoke the build via `./gradlew`.
- **Docker** — only needed to build/run the container image (§5); building the
  jar or `installDist` needs nothing beyond the JDK + wrapper.

## 3. Build from source

```
./gradlew build
```

This compiles every module, runs the full test suite — including the
Docker/LocalStack integration tests — and assembles all distributions. For a
faster, Docker-free inner loop (no Testcontainers/LocalStack), add
`-PnoIntegration`:

```
./gradlew build -PnoIntegration
```

See [`build-and-modules.md`](internals/build-and-modules.md) for the module graph
(`swath-model` → `swath-core` → `swath-s3`/`swath-replay-server` → `swath-cli`)
and dependency rules, and [`ops/dev/TESTING.md`](ops/dev/TESTING.md) for the
full set of test-tier gradle properties.

## 4. Distributions

Both distributions below are produced by the `swath-cli` module
(`swath-cli/build.gradle.kts`) and share the same compiled classes and
runtime classpath — they differ only in how that classpath is packaged.

### Uber-jar (`swath.jar`)

```
./gradlew :swath-cli:shadowJar
java -jar swath-cli/build/libs/swath.jar list s3://my-bucket/ --format parquet -o out/
```

Produces `swath-cli/build/libs/swath.jar` — a single self-contained,
runnable file. It is built with the
[Shadow](https://gradleup.com/shadow/) plugin, which:

- merges colliding `META-INF/services` entries (Hadoop `FileSystem` providers,
  AWS SDK providers) so ServiceLoader still resolves correctly once everything
  is shaded into one jar,
- strips signed-jar metadata (`META-INF/*.SF`/`.RSA`/`.DSA`) left over from
  merged dependency jars, which would otherwise fail manifest verification at
  JVM startup, and
- sets `Multi-Release: true` on the merged manifest (needed by a
  multi-release dependency jar whose `java.net.spi.InetAddressResolverProvider`
  implementation lives under a versioned `META-INF/versions/` directory).

It does **not** relocate or minimize classes: `sqlite-jdbc` extracts a native
library from its packaged resource path at runtime, and relocation would break
that lookup. This uber-jar is the same artifact the Docker image uses (§5).
Releases do not publish Java-library artifacts to Maven; whether to is a
separate release-engineering decision.

### installDist (`bin/swath`)

```
./gradlew :swath-cli:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PATH"
swath list s3://my-bucket/ --format parquet -o out/
```

Produces `swath-cli/build/install/swath/`: a `bin/swath` (+ `swath.bat`)
launcher script and a `lib/` directory of dependency jars. The launcher is a
standard Gradle `application`-plugin script; it honors `JAVA_OPTS` and
`SWATH_OPTS` environment variables for
passing extra JVM flags without editing the script. `distZip`/`distTar`
(and their `assembleDist` aggregate) package the same tree as a `.zip`/`.tar.gz`
for distribution without a JDK-execute step.

### Dependency slimming

`swath` only ever lists/reads/writes local Parquet — it never talks to HDFS,
YARN, or ZooKeeper — but `parquet-hadoop` transitively pulls in the
`hadoop-common`/`hadoop-mapreduce-client-core` stack, which drags along
Hadoop's HDFS-client, YARN, ZooKeeper, Curator, and Kerberos jars even though
none of that code is ever exercised. The shared `swath.java-conventions`
Gradle plugin (`build-logic/src/main/kotlin/swath.java-conventions.gradle.kts`)
excludes those modules at the `configurations.all { exclude(...) }` level, so
the exclusion applies to the whole resolved dependency graph regardless of
which transitive edge would otherwise reintroduce it. This keeps unused service
stacks out of every distribution. `hadoop-common` still brings some runtime
transitives; inspect the current distribution if artifact size matters to you.

## 5. Docker image

### Build

The root `Dockerfile` is a self-contained multi-stage build — `docker build .`
needs no prior host Gradle run:

```
docker build -t swath:dev .
```

For a local multi-arch build (`linux/amd64` + `linux/arm64`), loaded into the
local image store with no registry push:

```
just docker-build
```

which is equivalent to:

```
docker buildx create --name swath-builder --driver docker-container
docker buildx build --platform linux/amd64,linux/arm64 \
  --builder swath-builder -t swath:dev --load .
```

The build stage compiles with Gradle (pinned to `$BUILDPLATFORM`, the
*native* builder architecture — the uber-jar is arch-neutral bytecode, so
compiling needs to happen only once regardless of how many `--platform`
targets are requested) and runs `./gradlew :swath-cli:shadowJar`. The runtime
stage is `eclipse-temurin:25-jre-noble`; it only copies the uber-jar in — no
`RUN` step — and its entrypoint is:

```
ENTRYPOINT ["java", "-jar", "/opt/swath/swath.jar"]
```

i.e. the image ships the exact same `swath.jar` described in §4, not a
separately-packaged copy. Its legal bundle is available at
`/opt/swath/LICENSE`, `/opt/swath/NOTICE`, and `/opt/swath/THIRD_PARTY_NOTICES.md`.

### Runtime contract

The image runs as numeric UID/GID `10001:10001`, with `/opt/swath` as its working
directory and `java -jar /opt/swath/swath.jar` as its exec-form entrypoint. These are
packaging constraints: changing them affects writable mounts, Kubernetes
`runAsNonRoot`, signal delivery, and downstream images.

Do not duplicate end-user mount or credential commands here. The canonical examples are
in [Getting started](getting-started.md) and
[Credentials in Docker](operating.md#credentials-in-docker).

### Multi-arch

The image supports `linux/amd64` and `linux/arm64` with **no QEMU emulation**
required, even when building the non-native architecture on a single-arch CI
runner: the runtime stage is deliberately `RUN`-free (it only pulls the
per-arch JRE base image and `COPY`s the arch-neutral uber-jar onto it), so
there is nothing to *execute* in the foreign-arch rootfs during the build —
only pull, copy, and metadata operations, all of which work without
emulating the target CPU.

### Signals and JVM configuration

`java` runs as the container's PID 1 in exec form (`ENTRYPOINT ["java", ...]`,
not a shell wrapper), so `SIGTERM`/`SIGINT` reach the JVM — and swath's own
signal handling (see [`usage.md`](usage.md)'s exit-code table: `130` on SIGINT,
`143` on SIGTERM) — directly, without needing an init process for zombie
reaping (swath spawns no child processes).
To tune the JVM (heap, GC, etc.), set **`JAVA_TOOL_OPTIONS`**, which the JVM
reads automatically on startup:

```
docker run --rm -e JAVA_TOOL_OPTIONS="-Xmx2g" swath:dev list ...
```

Note this differs from the `installDist` launcher (§4), which reads
`JAVA_OPTS`/`SWATH_OPTS` — the Docker entrypoint invokes `java` directly with
no such script in between, so those variables have no effect here.

## 6. Container images (GHCR)

The tag-driven release workflow publishes a multi-arch image at
`ghcr.io/varveio/swath`. The exact image path and digest are printed in the
workflow run summary. Package visibility is controlled by the repository's GHCR
settings; build from source (§3–§5) if the package is not available to your
account.

Prefer pinning by **digest** over a mutable tag — the digest is the only
immutable reference:

```
docker pull ghcr.io/varveio/swath@sha256:<digest>
```

The release workflow prints the pushed manifest's digest to its run summary, so
a consumer can copy the exact digest to pin against; each GitHub release also
ships the signed, attested assets to verify against
([verify a release](install.md#verify-a-release)).

### Tags & versioning

Each release image carries these tags:

| Tag | Meaning | Mutable? |
|---|---|---|
| `<version>` (e.g. `0.1.0`) | The release tag's canonical Gradle version | Intended immutable |
| `latest` | Most recently published release | Moves |

The uber-jar ships as a versioned release asset, `swath-<version>.jar` (the build
output keeps the fixed `swath.jar` name — the image promotion depends on it).
Release tags must exactly match the canonical
Gradle version (`v0.1.0` ↔ `0.1.0`). **For anything that must not shift under
it — for example a downstream image built `FROM` this one — pin the digest, not
`latest`/`<version>`.**

## 7. CI image checks and release publication

Two jobs in `.github/workflows/ci.yml` handle CI images. **`docker-check`**
builds the multi-arch image (validating both `linux/amd64` and `linux/arm64`,
no QEMU per §5), loads the native-arch build, and smoke-tests it
(`docker run … --help`) — it never pushes. **`docker-publish`** builds, runs a
deep container smoke, and pushes dev images to GHCR. Tagged publication belongs
to `.github/workflows/release.yml`, behind the protected `public-release`
environment.

Which jobs run depends on the trigger:

| Event | fast-tests | integration-tests | deep-tests | docker-check | docker-publish |
|---|:--:|:--:|:--:|:--:|:--:|
| **Pull request** (feature branch) | ✅ | — | — | ✅ (build + smoke, no push) | — |
| **Push to `main`** (i.e. a merge) | ✅ | ✅ | ✅ | — | ✅ (`sha-<gitsha>` + `main`) |
| **Manual** (`workflow_dispatch`) | ✅ | ✅ | ✅ | — | ✅ (`sha-<gitsha>` only) |

`docker-check` is the **PR-only** build gate — it validates the multi-arch build
and runs `--help`, but never pushes. On a merge or dispatch it doesn't run,
because `docker-publish` builds the image itself (running both would just be a
redundant second multi-arch build).

A dispatch can run on any branch, so it publishes the immutable `sha-<gitsha>`
tag **only** — the mutable branch-name tag is restricted to `main`, keeping
internal branch names off the public package. Pull a branch build by its
`sha-` tag.

`docker-publish` is gated on all three test tiers, so no image is published from
code that failed any of them. The release publish job is separately protected by
the `public-release` environment and requires the explicit
`PUBLIC_RELEASE_ENABLED=true` repository variable, which acts as a publish
kill-switch.

The **CI image checks** have no scheduled run — they fire only on pull
requests, pushes to `main`, and manual dispatch. (A separate `nightly` workflow
runs the deep and perf test tiers on a daily schedule; it does not build or
publish images.) Top-level workflow concurrency cancels superseded runs of the
same PR; `main` and manual runs have unique top-level groups and are not
serialized there. The `docker-publish` job has its own global concurrency group,
which queues publishes so shared tags cannot race. The `push` trigger is scoped
to `main`, so
**pushing a feature branch by itself runs nothing** — a feature branch is
exercised through its **pull request**, which builds and smoke-tests the image
but does not push it. Ordinary merges never publish; manual dispatch is a
maintainer decision. Public tagged releases require the guarded release
workflow described above. PRs from forks never push and never touch registry
credentials.
