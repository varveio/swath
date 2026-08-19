# swath 0.2.4

## User-facing changes

- S3 requests now identify themselves in the HTTP User-Agent as `swath/<version>` (plain
  `swath/development` outside a packaged build), prepended ahead of the AWS SDK's own
  `aws-sdk-java/…` and `api/S3#…` markers. No request behaviour changes; this is so
  swath's traffic is attributable in S3 access logs, CloudTrail, and any endpoint that
  logs its callers by User-Agent.
- Several summary and progress figures on a `--sort` run were silently computed over the
  *whole* run — merge and publish tail included — while being read as listing-phase
  numbers. All are now scoped to the phase they claim to describe:
  - `tail_occupancy.wall_share`'s window end and wall-time denominator now freeze at the
    listing→merge boundary instead of at gauge-read time, so the merge tail can no longer
    inflate it.
  - A new additive `listing_duration_ms` field (schema v2) reports the listing-and-staging
    span alone, so a listing-only rate no longer has to be reconstructed as
    `duration_ms - sort.merge_ms`.
  - A new additive `recovered_objects` field reports how many of a resumed run's rows were
    backfilled from an earlier attempt rather than listed by this process — needed to
    compute a corrected listing-phase rate from `summary.json` alone, without scraping the
    `-v` progress line.
  - `freeze_gate_checks` is now published as the denominator for `latency_freezes` and
    `growth_freezes`, so a healthy, saturated run (which returns before the freeze gates
    run at all) is distinguishable from one whose gates simply never engaged.
  - `regime.worker_page_latency_p50_ms`/`_p99_ms` now measure data-page latency only,
    rather than every API call class including cheap probes, so they read as an honest
    serial-baseline estimate.
  - A merge-only `--sort --resume` run — one that issues zero LIST calls — now attributes
    its merged row count to `recovered_objects` rather than to this process's own `objects`
    and `keys_per_sec`.
  - The stderr progress headline no longer labels a figure "listing" on a `--sort` run
    whose duration includes the merge/publish tail.
  - `keys_per_sec`, `cpu_efficiency`, and `avg_in_flight` are unchanged and deliberately
    keep whole-run semantics for compatibility — see Limits below.
- Fixed a rare, flaky failure in the sort engine's own test suite
  (`maxDurationWithProgress_staysPlainMaxDuration`); test-only, no production behaviour
  changed.

## Evidence

- `S3ClientFactoryTest#realApacheRequestPrependsSwathUserAgentToSdkMarkers` runs a real
  HTTP server and inspects the actual request header: the User-Agent starts with
  `swath/development aws-sdk-java/2.31.78 `, contains the SDK's `api/S3#2.31.78` marker,
  and contains exactly one `swath/` token — not a byte of the SDK's own value is
  disturbed.
- The size of the mislabelling this release fixes is measured, not assumed: the
  [PR #99 field-campaign table](../../performance.md#the-sorted-merge) recorded the merge tail
  at 20-39% of session wall time across three sorted buckets on the shipped-default merge
  arm (median ~32%), rising to as much as ~72% of session wall on the explicit-serial-merge
  arm. That is how much of "listing" was actually merge before this fix.
- Each fix is pinned by a dedicated test: `TailOccupancyListingScopeTest`,
  `ConcurrencyGaugeFreezeGateDenominatorTest`, `JsonRunSummaryWriterTest`,
  `ListRunnerObservabilityTest`, `SortMergeReentryContractTest`,
  `SortResumeListingContractTest`, plus the OTLP and simple-registry series-identity
  suites for the renamed/rescoped meters.
- The flaky-test fix was verified directly against the failure it targets: 500 in-JVM
  iterations of the original repro shape ran clean (the pre-fix code failed 3 times in
  300), and 15 Gradle invocations of the test task ran clean under load, including with 12
  spinner threads saturating the box (the pre-fix code failed 1 time in 8 Gradle runs).
- CI's fast tier, CodeQL, and the container-build check were green on each of the three
  merged pull requests (#101, #103, #104).

## Limits and known issues

- `listing_duration_ms` is not a bare API-calls-only span: on a `--sort` run it still
  includes the sort lane's final drain/encode of its staged segments, which happens after
  the last LIST response. It is the listing-*and-staging* span.
- `keys_per_sec`, `cpu_efficiency`, and `engine.avg_in_flight` are unchanged by this
  release and still divide by the whole-run `duration_ms` for backward compatibility — on
  a `--sort` run they remain diluted by the merge tail. Use the new
  `listing_duration_ms`/`recovered_objects` fields to recompute a listing-only rate
  yourself if that is what you need; the formula is in
  [`metrics-and-observability.md`](../../metrics-and-observability.md).
- The 20-39%/median ~32%/up to ~72% merge-share figures above are a measurement from a
  three-bucket field campaign, not a bound — the actual ratio on any given run depends on
  its segment count and the merge parallelism actually used.
- No public schema version bump: `listing_duration_ms` and `recovered_objects` are
  additive under the existing schema v2 contract, not a v3.
