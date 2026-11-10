# Packaging and release engineering

This is the contributor reference for building, packaging, and testing the Java
implementation's self-contained jar, application archives, and container images. Releases no
longer publish them: from 0.4 a release ships the Go binaries and images
([`RELEASING.md`](../RELEASING.md), [Go install](../go/docs/operating.md#install-and-verify)),
and v0.3.2 was the last Java release. It is not the installation or container-usage guide. To
run the Java release, use [Installation](install.md); for credentials, output mounts, and
production object-store access, use [Operating swath](operating.md).

For the module and dependency graph behind the artifacts, see
[`build-and-modules.md`](internals/build-and-modules.md).

## 1. Overview

`swath` is a Java 25 object-store lister with a separately distributed replay toolkit. One
Gradle build produces three equivalent ways to run the lister:

- an **uber-jar** (`swath.jar`) — `java -jar swath.jar ...`,
- an **`installDist` tree** (`bin/swath` + `lib/`) — a launcher script plus
  its dependency jars, and
- a **Docker image** — a container that runs the uber-jar.

All three are built from the same `swath-cli` module and the same compiled
classes; nothing is compiled twice. The `swath-replay` module additionally produces an
`installDist` toolkit and a separate `ghcr.io/varveio/swath-replay` image. It is not folded
into the lister jar or image.

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
(`swath-model` → `swath-core` → `swath-s3`/`swath-replay` → `swath-cli`)
and dependency rules, and [`ops/dev/TESTING.md`](ops/dev/TESTING.md) for the
full set of test-tier gradle properties.

## 4. Distributions

The two CLI packaging forms below are produced by the `swath-cli` module
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

### Replay toolkit (`bin/swath-replay`)

```bash
./gradlew :swath-replay:installDist
swath-replay/build/install/swath-replay/bin/swath-replay --help
```

The replay distribution carries `swath-replay` and `swath-replay-conformance`. It is a separate developer toolkit, not
part of the `swath` CLI distribution. Its legal bundle includes a replay-specific
`THIRD_PARTY_NOTICES.md` generated from that distribution's runtime graph. See the
[replay toolkit guide](swath-replay.md).

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
stacks out of every distribution.

Netty is excluded on the same grounds and for both of its entry points: Hadoop's
IPC/HDFS paths, and the AWS SDK's `netty-nio-client`, which exists to serve
`S3AsyncClient` — swath builds only the synchronous client, over `apache-client`.
The SDK's adapter jar is excluded alongside `io.netty` itself, so no async HTTP
provider ships half-present.

What `hadoop-common` does still bring, it brings at the versions its own POM
names, which run years behind: Jackson 2.12, Guava 27, Avro 1.9. Those are
raised by a `constraints` block in the same convention plugin, with the floors
themselves in `gradle/libs.versions.toml` so Dependabot maintains them. A
constraint raises a floor without capping anything that legitimately needs
newer.

A guard keeps this honest: every application distribution is checked to ship
at most one version of each module (`verifyNoDuplicateModuleVersions`, part of
`check`) — the replay distribution packs a second dependency closure for its
conformance launcher into the same `lib/`, and before the floors were pinned it
shipped two complete Jackson generations at once. The nightly OSV scan
(`scripts/ci/osv-scan.sh`) no longer covers these distributions or the Java
images: no release ships them since v0.3.2, and it scans the Go release images
instead ([Go testing](../go/docs/testing.md#nightly)). The script can still scan
a Java distribution's `lib/` directory against the argued, expiring Java
exceptions in `config/osv/osv-scanner.toml`.

## 5. Docker images

### Build

The root `Dockerfile` is a self-contained multi-stage build — `docker build .`
needs no prior host Gradle run:

```
docker build -t swath:dev .
```

`Dockerfile.replay` builds the replay toolkit independently:

```bash
docker build -f Dockerfile.replay -t swath-replay:dev .
```

It runs the Gradle `installDist` launcher as numeric UID/GID `10001:10001` from
`/opt/swath-replay`. The published image contains no fixture; callers mount fixture data at
runtime as documented in the [replay toolkit guide](swath-replay.md#container-image).

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

Both images support `linux/amd64` and `linux/arm64` with **no QEMU emulation**
required, even when building the non-native architecture on a single-arch CI
runner: the runtime stage is deliberately `RUN`-free (it only pulls the
per-arch JRE base image and `COPY`s the arch-neutral uber-jar or replay installDist onto it), so
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

The replay image uses the Gradle launcher as its entrypoint; that script replaces itself with
the JVM, so signal delivery remains direct. It accepts `JAVA_OPTS` and `SWATH_REPLAY_OPTS` in
addition to the JVM-native `JAVA_TOOL_OPTIONS`.

## 6. Container images (GHCR)

Releases up to v0.3.2 published the Java images at `ghcr.io/varveio/swath` and
`ghcr.io/varveio/swath-replay`, with the uber-jar as the release asset
`swath-<version>.jar`. From 0.4 the same packages carry the Go images, built from
`Dockerfile.go`; [`RELEASING.md`](../RELEASING.md) owns their tags, assets and verification.
Pin a Java image by the digest its release printed, or by its version tag (`0.3.2`).

## 7. CI image checks and release publication

One job in `.github/workflows/ci.yml`, **`docker-check`**, handles both CI images on every
pull request, push to `main` and manual dispatch that touches Java. It builds each multi-arch
image from source (validating both `linux/amd64` and `linux/arm64`, no QEMU per §5), loads the
native-arch builds and smoke-tests them offline: `swath --version` (asserting the Gradle version
and the 12-character commit) and `--help` on the CLI image, `--help` on the replay image. It never
pushes: GHCR is written only by `.github/workflows/release.yml`, behind the protected
`public-release` environment (see [CI architecture](ops/dev/CI.md#registry-and-artifacts)). To
run an unreleased commit, build the image from its checkout with `docker build .`.

The Java images' deep smoke (Parquet write, read and resume against the public smoke bucket)
no longer runs: releases publish the Go images, which `release.yml` smokes against the pushed
digests ([`RELEASING.md`](../RELEASING.md#what-gates-a-release)).

Which jobs run depends on the trigger:

| Event | fast-tests | integration-tests | deep-tests | docker-check |
|---|:--:|:--:|:--:|:--:|
| **Pull request** (feature branch) | ✅ | ✅ | — | ✅ (build + smoke, no push) |
| **Push to `main`** (i.e. a merge) | ✅ | ✅ | ✅ | ✅ (build + smoke, no push) |
| **Manual** (`workflow_dispatch`) | ✅ | ✅ | ✅ | ✅ (build + smoke, no push) |

A change that touches only the Go port (`go/**`, `Dockerfile.go`) runs none of the Java
tiers and builds no Java image: the Java jobs report success with their steps skipped.

The release publish job requires the explicit `PUBLIC_RELEASE_ENABLED=true` repository variable,
which acts as a publish kill-switch.

A separate `nightly` workflow runs the deep and perf tiers and builds builder-native copies of
both runtime images solely for OSV scanning; it never publishes those images. The `push` trigger
is scoped to `main`, so **pushing a feature branch by itself runs nothing** — a feature branch is
exercised through its **pull request**. PRs from forks never push and never touch registry
credentials.
