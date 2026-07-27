# swath development commands.
# Run `just --list` for all commands.
#
# Speed tiers (docs/ops/dev/TESTING.md): default `test`/`build` run everything
# except @Tag("perf"), including the Docker/LocalStack @Tag("integration") ITs.
# `-PnoIntegration` skips those for a fast, Docker-free inner loop.

# List available commands
help:
    @just --list

# Full build: compile + full test suite (incl. Docker/LocalStack integration tests).
# This is what CI's `integration-tests` job runs. Slowest, most complete.
build:
    ./gradlew build

# Fast build: compile + fast tests only, no Docker/LocalStack ITs.
# This is what CI's `fast-tests` job (the PR gate) runs.
build-fast:
    ./gradlew build -PnoIntegration

# Really quick: compile + package, skip the test task entirely.
# Use this when you just want to know "does it compile and link."
build-notest:
    ./gradlew assemble

# Fastest correctness check: compile main + test sources only, no packaging, no tests.
compile:
    ./gradlew compileJava compileTestJava

# Default test run: fast tier (no Docker/LocalStack). Use `test-all` for the full suite.
test:
    ./gradlew test -PnoIntegration

# Full test suite, including Docker/LocalStack integration tests.
test-all:
    ./gradlew test

# One test class in a given module. Usage: just test-one swath-core SeedStepTest
test-one MODULE CLASS:
    ./gradlew :{{MODULE}}:test --tests '{{CLASS}}'

# Opt-in heavy scale/throughput tier (PERF-1/2 etc.) — on-demand / at gates only.
test-perf:
    ./gradlew test -Pperf

# Docker-free quick inner loop, explicit alias for test-all minus integration.
test-no-integration:
    ./gradlew test -PnoIntegration

# Remove all build outputs.
clean:
    ./gradlew clean

# Run the CLI. Usage: just run -- --bucket my-bucket --output out.parquet
run *ARGS:
    ./gradlew :swath-cli:run --args="{{ARGS}}"

# Build the clean engine Docker image, multi-arch (linux/amd64 + linux/arm64),
# self-contained (no host Gradle run needed — see root Dockerfile). Local only,
# `--load`ed into the containerd image store; no registry push.
#
# Uses an isolated Docker config (no credsStore) instead of the ambient
# ~/.docker/config.json: this build only ever pulls public base images and
# loads locally, so it never needs registry auth, and staying off the host
# config sidesteps devcontainer credential-helper forwarding that depends on
# a live VS Code IPC connection (breaks in headless/CLI-only sessions with
# "error getting credentials - err: exit status 255").
docker-build:
    mkdir -p ~/.cache/swath-docker-build
    echo '{}' > ~/.cache/swath-docker-build/config.json
    DOCKER_CONFIG=~/.cache/swath-docker-build docker buildx inspect swath-builder >/dev/null 2>&1 || DOCKER_CONFIG=~/.cache/swath-docker-build docker buildx create --name swath-builder --driver docker-container
    DOCKER_CONFIG=~/.cache/swath-docker-build docker buildx build --platform linux/amd64,linux/arm64 --builder swath-builder -t swath:dev --load .

# Compile the JMH benchmark sources only (fast — verifies they build).
jmh-compile:
    ./gradlew :swath-core:compileJmhJava

# Run all JMH micro-benchmarks (never runs as part of build/test — opt-in only).
jmh:
    ./gradlew :swath-core:jmh

# The instrumentation-drift guard CI runs before the Gradle build (docs/internals/metrics-internals.md §5).
ci-drift:
    ./scripts/ci/check-instrumentation-drift.sh

# Prepare a release: set the canonical version, commit, and create the vX.Y.Z tag.
# Stable SemVer only (pre-release/build metadata are rejected, matching the release build).
# Does NOT push — review, then `git push origin main vX.Y.Z` to trigger release.yml.
# After it publishes, bump gradle.properties to the next -SNAPSHOT. See RELEASING.md.
# Usage: just release 0.2.0
release VERSION:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ ! "{{VERSION}}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-rc\.[1-9][0-9]*)?$ ]]; then
        echo "error: VERSION must be X.Y.Z or X.Y.Z-rc.N (no other pre-release forms, no build metadata)" >&2; exit 2
    fi
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "error: working tree is not clean; commit or stash first" >&2; exit 2
    fi
    sed -i -E 's/^version=.*/version={{VERSION}}/' gradle.properties
    git add gradle.properties
    git commit -m "Release v{{VERSION}}"
    git tag -a "v{{VERSION}}" -m "swath v{{VERSION}}"
    echo "Prepared v{{VERSION}}. Review, then: git push origin main v{{VERSION}}"
    if [[ "{{VERSION}}" == *-rc.* ]]; then
        echo "This is a PRE-RELEASE: it publishes the X.Y.Z-rc.N container tag only —"
        echo "no :latest, no :X.Y — and creates a GitHub pre-release. It is a rehearsal of"
        echo "the publish path; the final tag is a separate, fresh build."
    else
        echo "After it publishes, set gradle.properties to the next X.Y.Z-SNAPSHOT and commit."
    fi
