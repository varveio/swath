# swath engine image — clean, cloud-agnostic.
#
# Self-contained multi-stage build: `docker build .` (or `docker buildx build
# --platform linux/amd64,linux/arm64 .`) works with no prior host Gradle run.
#
# TWO BUILD MODES, ONE RUNTIME LAYOUT: a local `docker build` compiles the CLI
# from the copied source. The publish workflow instead overrides the build stage
# with its previously tested uber-jar, so only published images carry those exact
# promoted bytes; both modes install the jar at the same runtime path.
#
# Build stage is pinned to $BUILDPLATFORM (the native builder arch), not the
# target arch: the uber-jar is arch-neutral Java bytecode (its native-dependency
# jars — sqlite-jdbc, zstd-jni — bundle libraries for every arch and pick the
# right one at runtime), so Gradle only needs to run once, natively, regardless
# of how many --platform targets this build produces. Only the runtime stage's
# base image varies per target platform.
#
# "Build once, use everywhere" for release provenance: this stage compiles from
# source so a bare `docker build .` is self-contained (an OSS user needs no prior
# Gradle run). CI's docker-publish job substitutes the already-built, already-tested
# uber-jar for this stage via a BuildKit build-context override
# (`--build-context build=<dir>` / `build-contexts: build=promote`), so the pushed
# image ships the exact bytes that passed the tests and skips this recompile — while
# a bare `docker build .` (and the PR docker-image job) still exercises this stage
# from source. The override dir mirrors this stage's output path
# (/src/swath-cli/build/libs/swath.jar); keep the runtime COPY below in sync with it.
# See .github/workflows/ci.yml.
FROM --platform=$BUILDPLATFORM eclipse-temurin:25-jdk-noble AS build
WORKDIR /src

# The checked-in legal notices verifier runs its renderer during shadowJar.
RUN apt-get update \
    && apt-get install --no-install-recommends -y python3 \
    && rm -rf /var/lib/apt/lists/*

# Warm the Gradle dependency cache before the full source lands, so unrelated
# source edits don't bust it. The --mount=type=cache keeps the downloaded
# dependency/wrapper cache across builds independent of layer invalidation, so a
# source edit doesn't re-download deps.
COPY gradlew ./
COPY gradle/ gradle/
COPY build-logic/ build-logic/
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY swath-model/build.gradle.kts swath-model/build.gradle.kts
COPY swath-core/build.gradle.kts swath-core/build.gradle.kts
COPY swath-s3/build.gradle.kts swath-s3/build.gradle.kts
COPY swath-cli/build.gradle.kts swath-cli/build.gradle.kts
COPY swath-replay-server/build.gradle.kts swath-replay-server/build.gradle.kts
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon help >/dev/null 2>&1 || true

COPY . .
# shadowJar (not installDist): the image ships the uber-jar itself, so it is the
# same artifact published elsewhere.
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon :swath-cli:shadowJar

# Runtime stage: arch-specific JRE — the only per-target-platform piece of this
# build. Deliberately RUN-free: it only pulls the base image and COPYs the
# arch-neutral uber-jar onto it, so there's nothing to execute in the
# foreign-arch rootfs and building the non-native platform needs no QEMU
# emulation.
#
# `java` runs as PID 1 in exec form, so SIGTERM/SIGINT reach the JVM (and swath's
# own handlers) directly; swath spawns no
# child processes, so no init/tini is needed for zombie reaping. The app sets no
# default JVM args (DEFAULT_JVM_OPTS is empty), so there's nothing to bake into
# the entrypoint; pass JVM flags at runtime via JAVA_TOOL_OPTIONS (the JVM reads
# it automatically) — this replaces the installDist launcher's JAVA_OPTS.
#
# Non-root via a NUMERIC UID (not a named user), on purpose:
#   - a name is not needed to drop root — the kernel only cares about the UID;
#   - a numeric UID lets Kubernetes verify `runAsNonRoot: true` at admission
#     (a named user can't be resolved pre-start and gets rejected), and works
#     under OpenShift's arbitrary-UID model;
#   - it keeps this stage RUN-free — a named user needs `RUN useradd`, which
#     would reintroduce foreign-arch emulation.
# `COPY --chown` sets ownership without a `chown` RUN. 10001 is a high UID
# chosen to avoid colliding with host UIDs on shared volumes.
FROM eclipse-temurin:25-jre-noble AS runtime

COPY --from=build --chown=10001:10001 /src/swath-cli/build/libs/swath.jar /opt/swath/swath.jar
COPY --from=build --chown=10001:10001 /src/LICENSE /opt/swath/LICENSE
COPY --from=build --chown=10001:10001 /src/NOTICE /opt/swath/NOTICE
COPY --from=build --chown=10001:10001 /src/THIRD_PARTY_NOTICES.md /opt/swath/THIRD_PARTY_NOTICES.md

USER 10001:10001
WORKDIR /opt/swath

ENTRYPOINT ["java", "-jar", "/opt/swath/swath.jar"]
