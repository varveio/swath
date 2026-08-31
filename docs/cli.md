# Supported CLI surface

swath's supported command-line contract is the `list` and `resume` commands plus the
flags below. This page names the stable surface; the installed command remains the source
of truth for syntax, accepted values, and current defaults:

```bash
swath --help
swath list --help
swath resume --help
```

The standard `--help`, `--version`, `-v`, `--quiet`, and `--color` controls are also
supported. Short names are conveniences for their long form; for example, `-o` is
`--output`.

## Listing and output

| Flag | Purpose |
| --- | --- |
| `--format` | Choose table, TSV, JSONL, Parquet, automatic terminal output, or the diagnostic discard sink. |
| `--output` | Write to stdout, a file, or a directory dataset. |
| `--output-type` | Resolve an otherwise ambiguous file or directory destination. |
| `--compression` | Compress table, TSV, or JSONL output. |
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
| `--requester-pays` | Send the S3 requester-pays header. |
| `--fetch-owner` | Request owner fields from S3. |
| `--concurrency` | Set the adaptive listing-concurrency ceiling. |
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
| `--checkpoint` | Select automatic, explicit, or ephemeral checkpointing. |
| `--restart` | Discard an unfinished checkpoint and start again. |
| `--overwrite` | Replace a completed destination deliberately. |

`swath resume <output-directory>` resumes a managed Parquet run. It accepts `--progress`
and `--stats` from the supported surface above.

## Expert and diagnostic controls

Options hidden from ordinary help and shell completion still parse for controlled
investigations and compatibility with existing commands. `--tune` and
`--engine-toggle` are documented in [Advanced configuration](configuration.md), and the
hidden `dump-run` command remains a read-only staging-segment inspector. These are not part
of the stable CLI contract.
