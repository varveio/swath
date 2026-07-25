# docs/

`docs/` is **current truth** — every file here describes swath as it ships and is safe
to read without archaeology. When reality changes, these files are **rewritten**, not
appended-to with correction banners.

The docs split into two tiers.

## Tier 1 — use it

What the average user needs, roughly in the order you'll want it:

- [`install.md`](install.md) — build and install the CLI.
- [`usage.md`](usage.md) — the `list` subcommand: flags, output formats, checkpoint/resume, `--max-duration`, exit codes, examples.
- [`configuration.md`](configuration.md) — tuning knobs, environment variables, defaults.
- [`operating.md`](operating.md) — running swath against real S3: credentials, the minimal IAM policy, S3-compatible endpoints, and what a run costs.
- [`performance.md`](performance.md) — concurrency, throughput expectations, sizing guidance.
- [`faq.md`](faq.md) — common questions and troubleshooting: credential/region errors, exit codes, `--sort` OOMs.
- [`packaging-and-docker.md`](packaging-and-docker.md) — the Docker image and release artifacts.
- [`metrics-and-observability.md`](metrics-and-observability.md) — the Micrometer meters, the `-v` progress line, and the JSON run-summary artifact.

## Tier 2 — how it works (internals)

You don't need this to use swath. It's for contributors, and for anyone curious how the
engine gets a no-gap/no-overlap listing without a client-chosen partition key.

- [`internals/overview.md`](internals/overview.md) — module map and where to start reading the engine.
- [`internals/architecture.md`](internals/architecture.md) — module boundaries, the AIMD/steal loop, the load-bearing invariants.
- [`internals/algorithms.md`](internals/algorithms.md) — the work-stealing scan: pivot synthesis, seeding, and the dense-tail placement mechanisms.
- [`internals/contracts.md`](internals/contracts.md) — the numbered invariants and contracts the engine holds.
- [`internals/walkthroughs.md`](internals/walkthroughs.md) — five step-by-step traces of the engine handling hard bucket shapes (deep tree, dense tail, skewed mass, saturated wide, crash/resume).
- [`internals/s3-implementation-compatibility.md`](internals/s3-implementation-compatibility.md) — deviations between real S3 and S3-compatible endpoints (LocalStack/MinIO) that swath designs around.
- [`internals/metrics-internals.md`](internals/metrics-internals.md) — the full steal-reason counter registry, JSON forensics fields, and run-trace format.
- [`internals/probe-budgets.md`](internals/probe-budgets.md) — how each call class's per-attempt timeout is sized (point vs scan), and the probe-timeout storm that motivated the split.
- [`internals/build-and-modules.md`](internals/build-and-modules.md) — the module graph, dependency rules, and shared build config.
- [`swath-replay-server.md`](swath-replay-server.md) — the S3-listing replay server used to test against fixture bucket shapes without hitting real S3.
- [`ops/dev/TESTING.md`](ops/dev/TESTING.md) — test tiers, speed, and the no-mass-populate rule.
- [`ops/dev/field-investigations.md`](ops/dev/field-investigations.md) — write-ups of runs against specific real buckets, and the changes they drove.
