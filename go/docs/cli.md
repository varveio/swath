# CLI

```text
swath list SOURCE --output DIR [flags]   list SOURCE into a sorted dataset in DIR
swath resume DIR [runtime flags]         continue the interrupted run in DIR
swath show [--journal] DIR               show a dataset or an interrupted run
swath diag list SOURCE [flags]           list with diagnostic flags (comparisons, policy toggles)
```

SOURCE is a local directory or `s3://bucket[/prefix]`. `swath list` writes a dataset to `--output` (`-o`)
([internals/parquet-output.md](internals/parquet-output.md)), or with `--count-only` only lists. It prints one
line, or the run report (`run.json`) with `--report-format json`. `swath resume DIR` replays the command the
journal recorded; only the runtime flags (`--workers`, `--encoders`, `--memory`, `--spill-max`,
`--spill-reserve`, `--spill-compress`, `--report-format`) may be given again, anything else is a usage error.
`swath show DIR` prints a committed dataset's manifest summary (format, schema, source, scope, coverage, parts,
rows, key range) or an interrupted run's recorded command, frozen identity, committed parts and the key
`swath resume` would start after; `--journal` dumps the journal as JSON lines. `swath diag list` takes every
`list` flag plus the diagnostic ones: `--sequential`, `--probe`, `--max-keys` (S3), `--no-journal` and
`--no-spill`; they exist for comparisons and tests, not production runs. Flags take one or two dashes. The
Java distribution keeps its own `swath` launcher; the two are separate tools.

## Flags

Generated from the flag set (`TestCLIDocFlags` fails when this block is stale; `UPDATE=1 go test ./cmd/swath`
rewrites it):

<!-- flags:begin -->
```text
usage: swath list SOURCE --output DIR [flags]
  SOURCE is a local directory or s3://bucket[/prefix]

  -anonymous
    	S3: send unsigned requests, ignoring any credentials
  -count-only
    	walk and count without writing a dataset
  -dualstack
    	S3: use the dual-stack (IPv6) endpoint
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS)
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fips
    	S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  -memory bytes
    	bytes of rows held between listing and encoding; sets the reorder bound (default 268435456)
  -metadata tier
    	local: metadata tier per entry: names (no stat), stat (the default), statx (adds birth_time; Linux)
  -no-dirs
    	local: omit kind=DIR rows
  -no-link-target
    	local: do not read symlink targets
  -o string
    	short for --output
  -output directory
    	output dataset directory (required unless --count-only)
  -overwrite
    	replace a previous dataset in --output
  -part-size bytes
    	target bytes per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB) (default 67108864)
  -path-style
    	S3: path-style addressing
  -profile profile
    	S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  -region string
    	S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
  -report-format text
    	text (one line) or json (the run report, as in run.json) (default "text")
  -request-timeout duration
    	S3: per-request timeout; a request that exceeds it is retried (default 30s)
  -seed
    	S3: tile the keyspace from one delimiter page before splitting
  -spill-compress
    	compress spill files (zstd level 1)
  -spill-max bytes
    	cap on spill bytes on disk (0: no cap); past it, listing waits instead of spilling
  -spill-reserve bytes
    	free bytes spill leaves on its filesystem, for the parts still to be written (default 1073741824)
  -workers int
    	listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, memory, spill-max, spill-reserve, spill-compress, report-format may change.
swath show [--journal] DIR shows a dataset or an interrupted run.

Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.

usage: swath diag list SOURCE [flags]   (the list flags plus diagnostic ones)
  SOURCE is a local directory or s3://bucket[/prefix]

  -anonymous
    	S3: send unsigned requests, ignoring any credentials
  -count-only
    	walk and count without writing a dataset
  -dualstack
    	S3: use the dual-stack (IPv6) endpoint
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS)
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fips
    	S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  -max-keys int
    	S3: with -sequential, stop after this many objects (experiments; the dataset is marked incomplete)
  -memory bytes
    	bytes of rows held between listing and encoding; sets the reorder bound (default 268435456)
  -metadata tier
    	local: metadata tier per entry: names (no stat), stat (the default), statx (adds birth_time; Linux)
  -no-dirs
    	local: omit kind=DIR rows
  -no-journal
    	diagnostic: publish parts only at the end, without the journal that swath resume needs
  -no-link-target
    	local: do not read symlink targets
  -no-spill
    	diagnostic: wait on the reorder budget instead of spilling rows ahead of the frontier to disk
  -o string
    	short for --output
  -output directory
    	output dataset directory (required unless --count-only)
  -overwrite
    	replace a previous dataset in --output
  -part-size bytes
    	target bytes per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB) (default 67108864)
  -path-style
    	S3: path-style addressing
  -probe
    	S3: before each split, a 1-key request checks the upper child is not empty (off: it costs more than it saves, REPORT.md Step 5a)
  -profile profile
    	S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  -region string
    	S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
  -report-format text
    	text (one line) or json (the run report, as in run.json) (default "text")
  -request-timeout duration
    	S3: per-request timeout; a request that exceeds it is retried (default 30s)
  -seed
    	S3: tile the keyspace from one delimiter page before splitting
  -sequential
    	S3: list with one continuation-token chain (no ranges; the reference)
  -spill-compress
    	compress spill files (zstd level 1)
  -spill-max bytes
    	cap on spill bytes on disk (0: no cap); past it, listing waits instead of spilling
  -spill-reserve bytes
    	free bytes spill leaves on its filesystem, for the parts still to be written (default 1073741824)
  -workers int
    	listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, memory, spill-max, spill-reserve, spill-compress, report-format may change.
swath show [--journal] DIR shows a dataset or an interrupted run.

Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.
```
<!-- flags:end -->

## Semantics worth stating

- **Sizes** accept `KiB`, `MiB`, `GiB` and `KB`, `MB`, `GB` suffixes.
- **`--memory`** is one budget: half of it bounds the bytes of rows in flight (reorder storage and encoder
  queues); see
  [internals/ordering.md](internals/ordering.md).
- **`--output`** is refused when non-empty unless `--overwrite`, which replaces only a previous dataset of this
  tool and never anything inside the listed root.
- **S3 modes.** The default is the parallel core; `--seed` tiles the scope from one delimiter page first;
  `swath diag list --sequential` is the one-chain oracle and the only mode where `--max-keys` stops early (the
  dataset is then marked incomplete). `--endpoint` and `--path-style` select S3-compatible stores.
- **S3 credentials:** `--anonymous` sends unsigned requests whatever the environment holds. Otherwise the AWS
  SDK's default credential chain applies: environment, shared files and profiles (`--profile`), SSO, web
  identity, credential processes, AssumeRole, container and instance roles. With nothing found, requests go
  unsigned. Secrets are never taken from flags (they would show in `ps`). Details:
  [internals/providers.md](internals/providers.md).
- **Spill:** when the reorder budget is full, rows of ranges ahead of the frontier go to disposable files
  under `DIR/.swath/spill/` instead of stalling the workers; `swath diag list --no-spill` turns it off,
  `--spill-compress` compresses them. Spill stops (and listing waits instead) at `--spill-max` bytes on disk or
  when the filesystem would drop below `--spill-reserve` free bytes (default 1 GiB)
  ([internals/ordering.md](internals/ordering.md)).
- **Resume:** an S3 run keeps a journal in `DIR/.swath/` with the command that started it; after a crash or
  interruption, `swath resume DIR` continues it ([internals/durability.md](internals/durability.md)).
  `swath diag list --no-journal` turns it off (parts are then published only at the end).
- **Exit status** is 2 for a malformed command line, 1 for any other error; a failed or interrupted run (SIGINT, SIGTERM) leaves no `manifest.json`.
  An interrupted run gets 15 s to stop cleanly, and is then exited with status 130; an S3 run can be resumed.
- **Environment:** `SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` name pprof output files;
  `SWATH_LOCALFS_ENGINE=portable` selects the portable filesystem engine; `SWATH_SPLIT_TRACE` traces S3 splits.
