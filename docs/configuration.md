# Configuration reference

One place to see every environment variable, flag, `--tune` knob, and `-D`
system property, with its default. This is a lookup table, not a tutorial — see
[`usage.md`](usage.md) for what each one actually does and why.

## Environment variables

### AWS SDK (standard, not swath-specific)

swath authenticates and connects through the AWS SDK's own default chains, so
it honors the same environment variables any AWS SDK v2 client does.

| Variable | Effect | Overridden by |
| --- | --- | --- |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | Region, if `--region` is unset | `--region` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Static credentials | `--profile`, `--no-sign-request` |
| `AWS_PROFILE` | Default credentials profile | `--profile` |
| `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` | Web-identity credentials (OIDC, e.g. GKE/EKS workload identity), auto-refreshed | `--profile`, `--no-sign-request` |
| `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` | S3 endpoint override (SDK-level default; applies only when `--endpoint-url` is not passed) | `--endpoint-url` |

### swath's own

| Variable | Default | Effect | Overridden by |
| --- | --- | --- | --- |
| `SWATH_OTLP_ENDPOINT` | unset (no export) | OTLP metrics collector URL | `--metrics-endpoint`, `--no-metrics` |
| `SWATH_OTLP_INTERVAL` | 5s step | OTLP export cadence | — |
| `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME` | unset | OTel-standard resource attributes merged onto exported metrics | — |
| `SWATH_OPTS`, `JAVA_OPTS` | unset | Extra JVM flags for the `installDist` launcher script only (no effect on the uber-jar or Docker image) | — |
| `JAVA_TOOL_OPTIONS` | unset | Extra JVM flags read by the JVM itself (works for the uber-jar, `installDist`, and Docker) | — |

## Flags and defaults

### S3 connection

| Flag | Default |
| --- | --- |
| `--region` | AWS SDK default region chain |
| `--profile` | AWS SDK default credential chain |
| `--no-sign-request` | off |
| `--endpoint-url` | unset |
| `--force-path-style` | on when `--endpoint-url` is set |
| `--fetch-owner` | off |
| `--requester-pays` | off |

### Output

| Flag | Default |
| --- | --- |
| `-o, --output` | stdout |
| `--output-type` | inferred from `-o` |
| `--format` | `auto` (table on a TTY, TSV piped) |
| `--parquet-part-size` | `256mb` |
| `--part-rotation-interval` | `30s` |
| `--part-rotation-max-rows` | `2000000` |
| `--sort` / `--no-sort` | `--no-sort` |
| `--report` | `<output>/_swath_summary.json` for every non-stdout Parquet destination (including FILE-kind `*.parquet`), else none |

### Filters

Every filter is unset by default (no filtering). They compose as an AND chain:
a row must pass all of them. Changing any of them between runs is refused on
`swath resume` — use `--restart`.

| Flag | Default |
| --- | --- |
| `--include REGEX` | unset (every key kept) |
| `--exclude REGEX` | unset (nothing dropped) |
| `--min-size SIZE` | unset (no lower bound); accepts size suffixes, e.g. `1k`, `256mb` |
| `--max-size SIZE` | unset (no upper bound); must be `>= --min-size` |
| `--modified-since DATE` | unset (no lower bound); UTC when the value carries no offset |
| `--modified-until DATE` | unset (no upper bound); must be `>= --modified-since` |
| `--storage-class CLASS[,CLASS]` | unset (every class); comma-separated, e.g. `STANDARD,GLACIER` |

`--include`/`--exclude` take Java regexes and are **substring** matches on the
key's UTF-8 view — anchor with `^`/`$` yourself for a whole-key match.
`--storage-class` is case-insensitive. The size, mtime, and storage-class
filters key on the *row type*, not on a missing value: a common prefix carries
none of the three fields and passes all three unconditionally, while a delete
marker passes the size and storage-class bounds but **is judged by mtime**
whenever it carries a timestamp. The one asymmetry is storage class on an object
row — an object whose storage class the listing did not return is **dropped** by
`--storage-class`, not waved through.

### Concurrency and liveness

| Flag | Default |
| --- | --- |
| `--concurrency` | `64` (AIMD ceiling; live value adapts within `[1, T]`) |
| `--object-listing-queue-size` | `50000` |
| `--request-rate` | unset (uncapped) |
| `--progress-interval` | `30s` |
| `--max-duration` | unset (no timebox) |
| `--idle-timeout` | `120s` |
| `--no-progress-timeout` | `10m` |

### Checkpoint and resume

| Flag | Default |
| --- | --- |
| `--checkpoint` | `auto` |
| `--restart` | off |
| `--overwrite` / `--force` | off |

`auto` is durable only for a DIRECTORY-dataset Parquet output, where it creates
`<dir>/.swath/checkpoint.sqlite`; for stdout it uses ephemeral state. FILE-kind
text and Parquet destinations reject `auto` and explicit checkpoint paths and
require `--checkpoint none`. Both `none` and ephemeral `auto` still run the
work-stealing engine; they simply cannot be resumed. `swath resume <dir>` opens
the managed co-located checkpoint and does not accept an arbitrary SQLite path.

### Metrics and diagnostics

| Flag | Default |
| --- | --- |
| `--metrics-endpoint` | unset (or `SWATH_OTLP_ENDPOINT`) |
| `--no-metrics` | off |
| `--trace` | off |
| `--engine-toggle` | every toggle defaults `on` except `readahead` (`off`); see [`usage.md`](usage.md#diagnostic-tier-ablation---engine-toggle) — diagnostic tier, not a supported configuration |

`--trace` is available with every checkpoint mode, including `--checkpoint
none`. Trace JSONL contains real key bounds and pivots; the JSON run summary can
also contain targets, arguments, filters, and sampled keyspace bounds. Treat
both as sensitive run artifacts.

### Global

| Flag | Default |
| --- | --- |
| `-v` / `-vv` / `-vvv` | off (INFO / DEBUG / TRACE) |
| `-q, --quiet` | off |
| `-h, --help` | — (prints help and exits) |
| `-V, --version` | — (prints version and exits) |

## `--tune` registry

`--tune KEY=VALUE` (repeatable) is the expert-settings namespace; run
`swath list ... --tune help` for the live registry.

| Key | Default | Meaning |
| --- | --- | --- |
| `engine.readahead` | `off` | Speculative dense-tail readahead |
| `seed.mode` | `shallow` | Initial keyspace discovery strategy (`shallow`, `none`, `hints` reserved) |
| `parquet.writers` | `3` | Bounded Parquet writer pool size (`2..4`) |
| `summary.interval` | `--progress-interval` | JSON run-summary flush cadence |
| `sort.ignore-disk-check` | `off` | Skip `--sort`'s pre-run and periodic disk-space guard |

See [`usage.md`](usage.md#tuning---tune) for what each knob actually changes and
the resume-applicability rules.

## JVM system properties

Dev-tier knobs, set with `-D` on the JVM (`java -Dswath.sort.fan-in=64 -jar
swath.jar …`, or via `JAVA_TOOL_OPTIONS`) — **not** CLI flags, so they do not
appear in `--help`. The `swath.sort.*` set governs `--sort` only. **Only the
binding merge width is echoed back:** the JSON run-summary's `sort` block reports
the runtime-clamped fan-in as `sort.effective_fan_in`, and none of the other
properties below are recorded anywhere in the summary — so keep the invocation
if you want to know what a tuned run was tuned with. Watch the near-collision
while reading one: summary `sort.segment_bytes` is the total staging bytes
*written* across all segments, not the `swath.sort.segment-bytes` roll
threshold. All sizes are bytes.

| Property | Default |
| --- | --- |
| `swath.sort.segment-bytes` | heap-adaptive: `max(64 MiB, heap-fraction × -Xmx)` |
| `swath.sort.segment-entries` | unbounded (the bytes gate governs) |
| `swath.sort.heap-fraction` | `0.08` |
| `swath.sort.buffers` | `2` (must be `>= 2`) |
| `swath.sort.fan-in` | `10000` (further clamped at runtime by the budget and `ulimit -n`) |
| `swath.sort.segment-codec` | `LZ4` (`NONE`, `LZ4`, or `ZSTD1`) |
| `swath.sort.final-file-bytes` | `1 GiB` (the roll threshold for multi-file sorted output) |
| `swath.sort.final-row-group-bytes` | `8 MiB` (the published file's seek granularity) |
| `swath.sort.segment-row-group-bytes` | `1 MiB` (columnar-Parquet staging only) |
| `swath.sort.merge-budget-bytes` | heap-adaptive, same shape as `segment-bytes` |
| `swath.sort.merge-per-stream-bytes` | `64 KiB` (the divisor that bounds the effective fan-in) |
| `swath.sort.merge-parallelism` | `1` (serial merge; `>1` is off-by-default and unreleased) |
| `swath.git.sha` | unset (falls back to the jar's implementation version) — the commit stamped into the summary's `shape.fingerprint` |

See [`usage.md`](usage.md#sorted-output---sort) for which knob actually binds and
how to size the staging volume.
