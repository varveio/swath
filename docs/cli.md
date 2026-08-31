# Supported CLI surface

`list` and `resume` are swath's supported commands. The tables and standard-control note
below name every option visible in their installed help. Most are supported operational
controls; the expert and diagnostic section marks entry points whose stability is
narrower. The installed command remains the source of truth for syntax, accepted values,
and current defaults:

```bash
swath --help
swath list --help
swath resume --help
```

The standard `--help`, `--version`, `-v`, `--quiet`, and `--color` controls are also
supported. Where a long option has a short alias, the alias is a convenience; for
example, `-o` is `--output` and `-q` is `--quiet`.

## Listing and output

| Flag | Purpose |
| --- | --- |
| `--format` | Choose table, TSV, JSONL, Parquet, automatic terminal output, or the diagnostic discard sink. |
| `--output` | Write to stdout, a file, or a directory dataset. |
| `--output-type` | Resolve an otherwise ambiguous file or directory destination. |
| `--compression` | Compress table, TSV, or JSONL output. |
| `--parquet-part-size` | Set the target size of Parquet dataset parts. |
| `--text-part-size` | Set the target uncompressed size of text dataset parts. |
| `--text-writers` | Set the writer count for a TSV or JSONL directory dataset. |
| `--part-rotation-interval` | Rotate dataset parts after an elapsed interval. |
| `--part-rotation-max-rows` | Rotate dataset parts after a row count. |
| `--writeback-size` | Shape filesystem writeback without changing part rotation or crash recovery. |
| `--sort` | Produce globally key-sorted managed Parquet output. |
| `--report` | Write the machine-readable run report to another path. |
| `--stats` | Control the end-of-run summary. |
| `--progress` | Control live progress records. |

Output layouts, durability, and resume behavior are described in [Using swath](usage.md).

## S3 connection and scope

| Flag | Purpose |
| --- | --- |
| `--region` | Select the AWS region when normal resolution is not sufficient. |
| `--profile` | Select an AWS credentials profile. |
| `--endpoint-url` | Use a compatible S3 endpoint such as LocalStack or MinIO. |
| `--force-path-style` | Control path-style addressing. |
| `--no-sign-request` | List a public bucket anonymously. |
| `--bearer-token-command` | Obtain OAuth bearer tokens from a command instead of using AWS SigV4 signing. |
| `--bearer-token-refresh-interval` | Set how often the bearer-token command is run again. |
| `--requester-pays` | Send the S3 requester-pays header. |
| `--fetch-owner` | Request owner fields from S3. |
| `--concurrency` | Set the adaptive listing-concurrency ceiling. |
| `--object-listing-queue-size` | Set the bounded listing queue's entry budget. |
| `--request-rate` | Cap aggregate S3 requests per second. |

Credentials, endpoints, and request cost are covered in [Operating swath](operating.md).

## Filters

| Flag | Purpose |
| --- | --- |
| `--include` | Keep keys matching a Java regular expression. |
| `--exclude` | Drop keys matching a Java regular expression. |
| `--min-size` | Keep objects at or above a size. |
| `--max-size` | Keep objects at or below a size. |
| `--modified-since` | Keep objects modified at or after a time. |
| `--modified-until` | Keep objects modified at or before a time. |
| `--storage-class` | Keep objects in selected storage classes. |

Filters reduce emitted rows, not S3 LIST requests.

## Run lifetime and resume

| Flag | Purpose |
| --- | --- |
| `--max-duration` | Stop after a duration while leaving durable work resumable. |
| `--idle-timeout` | Abort after the run has no activity for a duration. |
| `--no-progress-timeout` | Abort after the run has no committed progress for a duration. |
| `--progress-interval` | Set how often progress is reported. |
| `--checkpoint` | Select automatic, explicit, or ephemeral checkpointing. |
| `--restart` | Discard an unfinished checkpoint and start again. |
| `--overwrite`, `--force` | Replace a completed destination deliberately. |

`swath resume <output-directory>` resumes a managed Parquet run. Relevant shared controls
remain available there; for example, `--progress` and `--stats` control reporting, bearer
token flags can be re-passed, and `--tune` accepts resume-applicable keys. Its installed
help shows the current syntax.

## Observability and diagnostics

| Flag | Purpose |
| --- | --- |
| `--metrics-endpoint` | Export OTLP metrics to a collector endpoint. |
| `--no-metrics` | Disable metrics export, including environment configuration. |
| `--trace` | Write a diagnostic JSONL run trace. |

Metrics, progress, reports, and traces are described in
[Metrics and observability](metrics-and-observability.md).

## Expert controls

`--tune KEY=VALUE` is the visible, repeatable entry point for typed expert settings. Use
`--tune help` to list the keys. Stability belongs to each key—not to `--tune` as a
whole—and is reported as stable, experimental, or diagnostic by the running binary and
in [Tuning (`--tune`)](configuration.md#tuning---tune).

Options hidden from ordinary help and shell completion still parse for controlled
investigations and compatibility with existing commands. The diagnostic
`--engine-toggle` option is documented in
[Advanced configuration](configuration.md#diagnostic-engine-toggles), and the hidden
`dump-run` command remains a read-only staging-segment inspector.
