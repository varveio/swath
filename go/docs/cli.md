# CLI

```text
swath list SOURCE --output DIR [flags]   list SOURCE into a sorted dataset in DIR
swath resume DIR [runtime flags]         continue the interrupted run in DIR
swath show [--journal] DIR               show a dataset or an interrupted run
swath diag list SOURCE [flags]           list with diagnostic flags (comparisons, policy toggles)
swath --version                          print the build (tool, module version, VCS revision)
```

SOURCE is a local directory or `s3://bucket[/prefix]`. `swath list` writes a dataset to `--output` (`-o`)
([internals/parquet-output.md](internals/parquet-output.md)), or with `--count-only` only lists. It prints one
line, or the run report (`run.json`) with `--report-format json`. `swath resume DIR` replays the command the
journal recorded; only the runtime flags (`--workers`, `--encoders`, `--memory`, `--spill-max`,
`--spill-reserve`, `--spill-compress`, `--report-format`) may be given again, anything else is a usage error.
`swath show DIR` prints a committed dataset's manifest summary (format, schema, source, scope, coverage, parts,
rows, key range) or an interrupted run's recorded command, frozen identity, committed parts and the key
`swath resume` would start after; `--journal` dumps the journal as JSON lines. `swath diag list` takes every
`list` flag plus the diagnostic ones: `--sequential`, `--probe`, `--floors`, `--max-keys` (range-engine sources), `--no-journal` and
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
  -max-duration duration
    	stop the run after this duration (exit 124; a journaled run is resumable); 0: no limit
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
  -request-payer
    	S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)
  -request-timeout duration
    	S3: per-request timeout; a request that exceeds it is retried (default 30s)
  -seed
    	ranges: tile the keyspace from delimiter probes of the folder hierarchy before splitting
  -spill-compress
    	compress spill files (zstd level 1)
  -spill-max bytes
    	cap on spill bytes on disk (0: no cap); past it, listing waits instead of spilling
  -spill-reserve bytes
    	free bytes spill leaves on its filesystem, for the parts still to be written (default 1073741824)
  -stall-timeout duration
    	stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
  -workers int
    	listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, memory, spill-max, spill-reserve, spill-compress, report-format, max-duration, stall-timeout may change.
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
  -floors
    	ranges: the gate-chain floors: never split a range with at most 4 pages of keys left, nor leave the child past the pivot below 2 pages
  -max-duration duration
    	stop the run after this duration (exit 124; a journaled run is resumable); 0: no limit
  -max-keys N
    	ranges: with -sequential, stop after N objects (experiments; the dataset is marked incomplete)
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
    	ranges: before each split, a 1-key request checks the upper child is not empty (off: it costs more than it saves, REPORT.md Step 5a)
  -profile profile
    	S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  -region string
    	S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
  -report-format text
    	text (one line) or json (the run report, as in run.json) (default "text")
  -request-payer
    	S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)
  -request-timeout duration
    	S3: per-request timeout; a request that exceeds it is retried (default 30s)
  -seed
    	ranges: tile the keyspace from delimiter probes of the folder hierarchy before splitting
  -seed-descent
    	ranges: with -seed, walk the folder hierarchy (false: one delimiter page) (default true)
  -sequential
    	ranges: list with one continuation-token chain (no ranges; the reference)
  -spill-compress
    	compress spill files (zstd level 1)
  -spill-max bytes
    	cap on spill bytes on disk (0: no cap); past it, listing waits instead of spilling
  -spill-reserve bytes
    	free bytes spill leaves on its filesystem, for the parts still to be written (default 1073741824)
  -stall-timeout duration
    	stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
  -structure
    	ranges: structure probes: pivot on folder boundaries listed alongside a range's pages
  -workers int
    	listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, memory, spill-max, spill-reserve, spill-compress, report-format, max-duration, stall-timeout may change.
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
  dataset is then marked incomplete; such a run keeps no journal and cannot be resumed). `--endpoint` and `--path-style` select S3-compatible stores.
- **Requester pays:** `--request-payer` sends `x-amz-request-payer: requester` (signed) on every request, so
  a requester-pays bucket can be listed at the caller's charge.
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
- **Exit status and outcome.** A supervisor reads the exit code and, for a run that did not complete,
  `DIR/error.json` (`class`, `exit_code`, `message`, `resumable`, and `cause`: the provider's failure class such
  as `throttle` or `access_denied`, or `no_space` for a full disk); it never needs stderr. `error.json` and the
  progress file are written only while the run holds the output lock (a run refused by another owner writes
  neither), and `error.json` is removed when a later run or resume completes.

  | Code | Class | Meaning | Resumable |
  |---|---|---|---|
  | 0 | `completed` | the dataset is committed (or a dry run finished) | – |
  | 1 | `fatal`, `incomplete` | a cause a resume will not fix (access denied, no such bucket, an invariant), or a dataset committed with partial coverage (a key limit), so a partial dataset never exits 0; a full disk (`cause` `no_space`) is `fatal` too | no; `no_space`: yes, journal intact, once there is space |
  | 2 | `usage` | a malformed command line | no |
  | 75 | `stuck` | no progress for `--stall-timeout` (default 10 min; a warning at half) | yes, journal intact |
  | 124 | `deadline` | `--max-duration` reached with the output unfinished | yes |
  | 130 | `interrupted` | SIGINT (also after the 15 s shutdown grace) | yes |
  | 143 | `terminated` | SIGTERM, a graceful stop | yes |

  "Resumable" holds when the output has an intact journal (S3); a filesystem run starts over. A source that
  stops early (a key limit) never exits 0 with a partial manifest unless its coverage says so.
- **Liveness.** The command alone decides that a run cannot go on: its watchdog watches the run's progress
  (rows listed, ordered, encoded, parts published; any rise counts) and stops it as stuck after
  `--stall-timeout`. Nothing in the pipeline gives up on a slow source (S3 requeues a page after its retries).
- **Progress file.** While a run goes, `DIR/.swath/progress.json` is rewritten every 2 s by an atomic rename:
  `objects` (rows listed so far), `ordered`, `encoded`, `parts`, `advance` (their sum, which rises in every
  phase), `started_at`, `as_of`, `duration_ms`. A supervisor watches that file; `DIR/run.json` follows the
  commit. `--max-duration` and `--stall-timeout` are runtime flags: `swath resume` replays the recorded
  values unless given again.
- **Environment:** `SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` name pprof output files;
  `SWATH_LOCALFS_ENGINE=portable` selects the portable filesystem engine; `SWATH_SPLIT_TRACE` traces S3 splits.
