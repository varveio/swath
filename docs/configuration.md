# Configuration and advanced controls

This is an operator and experiment reference, not a prerequisite for a first listing.
The defaults are intended for ordinary runs. For output, filtering, and resume choices,
use [Common workflows](usage.md).

The generated CLI help is the canonical list of visible flags and defaults:

```bash
swath list --help
swath resume --help
swath list --tune help
```

This page documents configuration sources that are not self-evident from help, plus the
expert and diagnostic controls.

## Output resolution at startup

For a fresh `swath list`, swath resolves format, destination kind, and compression before
it opens a checkpoint, contacts S3, or creates output. `swath resume` first opens its
checkpoint, then restores and echoes the recorded output without mutating the destination.
The resolution rules are deliberately independent:

| Input | Resolution |
| --- | --- |
| No `-o`, or `-o -` | stdout; `auto` is table on a terminal and TSV when redirected |
| `--format discard` | diagnostic sink that counts rows without writing them; rejects a real `-o PATH` destination and gzip or Zstandard compression |
| `-o` ending in `.tsv` or `.jsonl` | atomically published single file; the suffix supplies the format when `--format` is omitted |
| `-o` ending in `.parquet` | by default, a one-part, non-resumable Parquet dataset directory; `--output-type dir` overrides that destination-kind inference |
| A `.gz` or `.zst` outer suffix | stripped before format inference and implies gzip or Zstandard for text; for example, `rows.jsonl.gz` is a gzip JSONL file |
| Any other real `-o` path | directory dataset; `--format` is required because the path supplies none |
| `--output-type file` or `--output-type dir` | overrides only destination kind, never a path's implied format |

An explicit `--format` must agree with a recognized suffix. An explicit
`--compression` must agree with `.gz`/`.zst`; Parquet rejects either form because its
compression is internal. Table, TSV, and JSONL support optional compression as stdout
or single-file streams. Only TSV and JSONL support bounded directory datasets; those
datasets are non-resumable and require `--checkpoint none` in this release.

Discard is non-resumable. It runs the normal listing engine but writes no listing rows.
Use `--report PATH` to retain its summary.

Text directory datasets use `--text-writers` (default `3`, range `2..64`) and
`--text-part-size` (default `256mb`). `--writeback-size SIZE` is an off-by-default performance
experiment for TSV/JSONL/Parquet directory datasets, including sorted Parquet final files; it shapes
dirty-page writeback without rotating parts or changing the crash-recovery boundary. Positive values below `4mb` are rejected,
and unsupported formats are rejected rather than silently ignoring it. See
[Writeback shaping for large dataset parts](performance.md#writeback-shaping).
The independent time/row rotation triggers can still close a part before its size target; disable
them explicitly when the output contract requires size-only rotation.
At startup, unless stdout or `-q` suppresses it,
swath echoes the resolved format, destination kind, compression, and destination so the
effective choice is visible before listing begins.

<a id="choosing-concurrency"></a>

## Choosing `--concurrency`

`--concurrency` sets the maximum width of concurrent listing work; it is a ceiling, not a
target. swath starts with a smaller live limit, raises it while the endpoint is healthy,
and lowers it when the store pushes back or workers repeatedly time out without progress.

Start with the default of 64. Increase it only in repeated, otherwise identical runs, and
choose the smallest value at which throughput stops improving. A higher ceiling can consume
more CPU, memory, and connections without making the listing faster. Results from a short
prefix or local replay server do not establish the right value for real S3.

The output destination can be a separate bottleneck. `--format discard` can isolate listing
cost for diagnosis, but validate the final setting again with the output format and storage
you will use in production. The [performance guide](performance.md#find-the-limiting-stage)
explains which report fields to compare.

## Precedence

An explicit CLI option wins over an environment value. AWS SDK environment values win
over shared profile or compute-role discovery according to the SDK's normal provider
chains. A resumed run restores its persisted identity and connection context; explicitly
re-passed soft context can override it, but identity-changing output, filter, and run-shape
settings are refused.

`--bearer-token-command` is deliberately never stored in a checkpoint. A checkpoint must
not decide what shell command a later process executes, and a literal token embedded in a
command must not be persisted. Re-pass the command when resuming a bearer-authenticated
endpoint.

## Environment variables

### AWS SDK

| Variable | Purpose | Explicit override |
| --- | --- | --- |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | Region when no CLI region is set | `--region` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Static credentials | `--profile`, `--no-sign-request`, or bearer-token auth |
| `AWS_PROFILE` | Shared-config profile | `--profile` |
| `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` | Auto-refreshed OIDC/web-identity credentials | `--profile`, `--no-sign-request`, or bearer-token auth |
| `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` | SDK endpoint default | `--endpoint-url` |

swath bundles the AWS STS module, so the standard web-identity flow works in EKS, GKE,
and similar environments.

### swath, OpenTelemetry, Java, and the terminal

| Variable | Default | Purpose |
| --- | --- | --- |
| `SWATH_OTLP_ENDPOINT` | unset | OTLP metrics destination; overridden by `--metrics-endpoint`, disabled by `--no-metrics` |
| `SWATH_OTLP_INTERVAL` | `5s` | OTLP export step |
| `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME` | unset | Standard OpenTelemetry resource attributes |
| `SWATH_OPTS`, `JAVA_OPTS` | unset | JVM flags read by the `installDist` launcher only |
| `JAVA_TOOL_OPTIONS` | unset | JVM flags read by Java itself; works with the jar, launcher, and Docker image |
| `NO_COLOR` | unset | Disables automatic color when set to any value |
| `TERM=dumb` | environment | Disables automatic color and terminal redraws |
| `CLICOLOR_FORCE` | unset | Forces color even when stderr is not connected to a terminal |

An explicit `--color=always` or `--color=never` wins over terminal environment signals.

## Tuning (`--tune`)

`--tune KEY=VALUE` holds typed expert settings that should not crowd the everyday flag
surface. It is repeatable. Invalid values fail before swath opens a checkpoint or contacts
the object store. Ask the running binary for all keys or one key:

```bash
swath list --tune help
swath list --tune seed.mode=?
```

The following table is machine-checked against the code registry.

| Key | Type / range | Default | Stability | Resume class | Applies to | Effect |
| --- | --- | --- | --- | --- | --- | --- |
| `engine.readahead` | `on or off` | `off` | experimental | free | fresh list | Speculatively fetch ahead on sustained dense tails; can trade API calls and memory for lower wall time. |
| `seed.mode` | `shallow, none, or hints` | `shallow` | stable (`hints` reserved) | identity | fresh list | Choose initial keyspace discovery. `hints` is reserved but not implemented. |
| `parquet.writers` | `integer 2..64 (heap-admitted above 4)` | `3` | stable | free | fresh list | Set the writer count for unsorted Parquet output. For counts above four, swath checks that the configured heap is large enough. Counts 2–4 are the tested range. Higher counts can use more memory and create more small parts; benchmark before adopting them. Unless `--output-type dir` overrides the default inference, a path ending in `.parquet` uses one writer. `--sort` does not use this writer pool, so the setting has no effect under `--sort`. |
| `summary.interval` | `positive duration` | `--progress-interval`, otherwise `30s` | stable | free | fresh list | Set `_swath_summary.json` heartbeat cadence; accepts values such as `2s`, `500ms`, or `PT2S`. |
| `sort.merge-parallelism` | `integer 1..16` | core-derived, capped at `8` | stable | free | fresh list and resume | Set the maximum number of contiguous key ranges, or pipeline encoders when `sort.finalization=pipeline`, in the final sorted merge. Runtime heap, fan-in, staged-size, and file-descriptor gates may lower it. Each engaged range produces at least one final file, so benchmark merge wall, peak memory, and consumer-visible file count together. A pre-publication resume may change this value because partial finals are disposable staging files and the merge is rerun from durable PageRuns. |
| `sort.finalization` | `ranges, or pipeline` | `ranges` | experimental | free | fresh list and resume | Choose the final sorted merge architecture. `ranges` is the shipped key-range implementation. `pipeline` is a default-off spike with bounded header cursors, one page-reference router, and striped complete-part encoders reading shared segment channels positionally; it does not use seek-derived range boundaries. Use the same value when comparing repeated runs, and keep part geometry and wait metrics with the result. |
| `sort.merge-boundary-policy` | `distinct, or rows` | `distinct` | experimental | free | fresh list and resume | Choose parallel-merge split points. `distinct` preserves the shipped evenly spaced selection over the bounded distinct page-minimum sample. `rows` uses validated type-2/type-3 cumulative entry mass to approximate row quantiles; any extensionless, type-1, invalid, or mixed input falls back to `distinct` for the whole merge. Boundaries remain strictly increasing raw keys, so equal keys remain in one range. The default does not change without a separate benchmark decision. |
| `sort.keep-staging` | `on or off` | `off` | diagnostic | free | fresh list and resume | Retain exactly the original checkpoint-tracked page-run staging segments and the co-located checkpoint after a successful sorted publish. Every other staging entry, including cascade intermediates and temporary/range files, remains disposable. This is diagnostic-only and increases retained disk; the tested zero-LIST merge-replay procedure is in [Performance](performance.md#diagnostic-zero-list-merge-replay). |
| `sort.ignore-disk-check` | `on or off` | `off` | diagnostic | free | fresh list and resume | Bypass sorted-output free-space checks. Size the staging volume independently first. |

`seed.mode` contributes to the run identity and cannot change on resume.
`sort.merge-parallelism`, `sort.finalization`, `sort.merge-boundary-policy`, `sort.keep-staging`, and
`sort.ignore-disk-check` apply to `swath resume`; the other keys affect fresh-list construction or
output lanes.

## Diagnostic engine toggles

`--engine-toggle NAME=VALUE` exists for controlled A/B experiments and rollback, not
routine performance tuning. Every non-default run identifies its effective toggles in
the startup log and JSON report. The output key set is unchanged; these controls alter
scheduling and pivot choices.

The supported default is all values below. The one supported non-default rollback is:

```bash
--engine-toggle rate_anchored_sensing=off \
--engine-toggle tail_floor=current
```

That pair restores the pre-0.2.0 sensing and owner-tail-floor behavior. Other deviations
are diagnostic.

| Name | Default | What a non-default value changes |
| --- | --- | --- |
| `owner_split` | `on` | `off` removes proactive owner-side carving, leaving idle-worker stealing. |
| `density_ewma` | `on` | `off` replaces the observed density fraction with its fixed dense-case ceiling. |
| `radix_bands` | `on` | `off` disables seed-time radix subdivision of dense flat regions. |
| `structure_probes` | `on` | `off` disables demand-driven `delimiter=/` discovery during stealing. |
| `far_ahead` | `on` | `off` pins bounded-range pivots to a plain midpoint. |
| `alphabet_pivots` | `on` | `off` stops using the observed key alphabet during interpolation. |
| `reflect` | `on` | `off` disables density-reflected placement and also prevents `reflect_lift`. |
| `confetti_feedback` | `on` | `off` disables feedback that suppresses repeatedly tiny owner-split children. |
| `reflect_lift` | `on` | `off` disables only the degenerate-pivot lift while keeping other reflection active. |
| `fanout_tiling` | `on` | `off` disables zero-probe tiling of truncated `key=value/` fan-outs. |
| `mass_aware_seed` | `on` | `off` disables sampling that distinguishes one heavy seed subtree from many tiny leaves. |
| `rate_anchored_sensing` | `on` | `off` selects the legacy remaining-work estimate. |
| `tail_floor` | `reach_floored` | Accepts `current`, `est_direct`, or `reach_floored` for the owner-split tail gate. |
| `readahead` | `off` | `on` enables speculative dense-tail fetches; prefer `--tune engine.readahead=on`. |

Exact mechanisms and engagement counters are in the
[algorithm reference](internals/algorithms.md) and
[instrument registry](internals/metrics-internals.md#5-instrumentation-discipline--post-hoc-classification-why-swath-emits-so-much).

## Sorted-output JVM properties

These development-tier controls are Java system properties, not CLI flags. Pass them
with `java -D... -jar`, `JAVA_TOOL_OPTIONS`, or the launcher-specific `JAVA_OPTS` /
`SWATH_OPTS`. Keep the invocation with the report: most are not echoed into the summary.

| Property | Default | Purpose |
| --- | --- | --- |
| `swath.sort.heap-fraction` | `0.08` | Derive staging-segment and merge budgets from `-Xmx`. |
| `swath.sort.segment-bytes` | `max(64 MiB, heap-fraction × -Xmx)` | Override the adaptive staging flush threshold. |
| `swath.sort.segment-entries` | effectively unbounded | Secondary segment entry cap. |
| `swath.sort.buffers` | `2`, minimum `2` | Fill buffer plus bounded off-thread encode buffers. |
| `swath.sort.segment-codec` | `LZ4` | Staging payload codec: `NONE`, `LZ4`, or `ZSTD1`. |
| `swath.sort.fan-in` | `10000` | Per-range merge-stream ceiling, further limited by the planning budget and open files. |
| `swath.sort.merge-budget-bytes` | same adaptive shape as `segment-bytes` | Runtime capacity budget for page-run merge residency. Planning charges two encoded bodies plus the type-3 decoded maximum per normal stream, then the page-aware merger charges every additional retained overlap body/raw payload before allocation. A truthful minimum width that cannot fit is a resumable merge-pending refusal. |
| `swath.sort.merge-per-stream-bytes` | `64 KiB` | Configured per-stream floor. Current type-3 trailers can raise it through exact encoded/decoded maxima; legacy inputs remain readable and their actual header claims are bounded by the runtime aggregate guard. |
| `swath.sort.merge-parallelism` | `max(1, min(8, availableProcessors / 2))` | Maximum contiguous key ranges, or pipeline encoders under `sort.finalization=pipeline`, in the final merge; `1` forces serial. Prefer `--tune sort.merge-parallelism=N` for an operator-selected value; the typed CLI value wins over this property. |
| `swath.sort.finalization` | `ranges` | Experimental finalization architecture: `ranges` or `pipeline`. Prefer `--tune sort.finalization=...`; the typed CLI value wins over this property. The pipeline uses `sort.merge-parallelism` as its encoder count. |
| `swath.sort.merge-boundary-policy` | `distinct` | Experimental parallel-range boundary policy. Prefer `--tune sort.merge-boundary-policy=distinct\|rows`; the typed CLI value wins over this property. `rows` is default-off and falls back to `distinct` unless every original input carries a validated type-2 or type-3 page index. |
| `swath.sort.keep-staging` | `off` | Diagnostic retention of exactly the original checkpoint-tracked page-run staging and the co-located checkpoint after successful sorted publication. `--tune sort.keep-staging=on` wins over this property. Every other immediate staging entry is deleted. |
| `swath.sort.min-parallel-staged-bytes` | `256 MiB` | Keep smaller merges serial. |
| `swath.sort.final-file-bytes` | `1 GiB` | Soft roll target for final sorted parts; an equal-key group is never split across files. The experimental pipeline may roll earlier at its 16,384 refs-per-plan memory cap. |
| `swath.sort.final-row-group-bytes` | `8 MiB` | Final Parquet seek granularity. |
| `swath.sort.final-page-rows` | `1024` | Maximum rows per final-file data page; the within-row-group seek granularity. |
| `swath.git.sha` | unset | Optional commit value included in the run fingerprint. |

The realized merge width is constrained by the staged segment count, heap budget, and
file-descriptor budget. The report's `sort.effective_fan_in` and engagement reasons show
what actually bound. See [the sorted merge](performance.md#the-sorted-merge) before
changing these values.

## Sensitive diagnostics

`--trace PATH` writes key bounds and pivots to JSONL. `_swath_summary.json` can include
the target URI, arguments, filters, slow-range bounds, and key samples. Neither is a
sanitized telemetry envelope; review and redact it before sharing.

OTLP export is off unless `--metrics-endpoint` or `SWATH_OTLP_ENDPOINT` configures it.
`--no-metrics` disables export even when the environment sets an endpoint. See
[Metrics and observability](metrics-and-observability.md) for meter semantics.
