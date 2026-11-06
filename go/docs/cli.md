# CLI

```text
swath list SOURCE --output DIR [flags]   list SOURCE into a sorted dataset in DIR
swath resume DIR [runtime flags]         continue the interrupted run in DIR
swath show [--journal] DIR               show a dataset or an interrupted run
swath diag list SOURCE [flags]           list with diagnostic flags (comparisons, policy toggles)
swath --version                          print the build (tool, module version, VCS revision)
```

SOURCE is a local directory, `s3://bucket[/prefix]` or `gs://bucket[/prefix]`. `swath list` writes a dataset to `--output` (`-o`)
([internals/parquet-output.md](internals/parquet-output.md)), or with `--count-only` only lists. It prints one
line, or the run report (`run.json`) with `--report-format json`; `--report FILE` also writes that report to
FILE when the attempt ends, on failure too. `swath resume DIR` replays the command the journal recorded; only
the runtime flags (`--workers`, `--encoders`, `--memory`, `--spill-max`, `--spill-reserve`, `--spill-compress`,
`--part-interval`, `--report-format`, `--report`, `--max-duration`, `--stall-timeout`) and the operational ones, how requests are
authenticated, timed and admitted (`--profile`, `--anonymous`, `--request-timeout`, `--gcs-anonymous`,
`--gcs-credentials`, `--gcs-request-timeout`, `--limiter`, `--limiter-slow-start`, `--request-rate`), may be given again (the last value
wins: `--anonymous=false` undoes a recorded `--anonymous`); anything else is a usage error. The source and output
identity stays frozen: a resume whose overrides resolve another route (a profile with another region, say) is
refused by the identity check. `swath resume` on a committed dataset with complete coverage has nothing to do
and exits 0 (`DIR is already complete: nothing to resume`; the report's outcome is `completed`, `complete`
true), so a supervisor may resume until exit 0; one committed with partial coverage stays `incomplete`, exit 1.
`swath show DIR` prints a committed dataset's manifest summary (format, schema, source, scope, coverage, parts,
rows, key range) or an interrupted run's recorded command, frozen identity, committed parts and the key
`swath resume` would start after; `--journal` dumps the journal as JSON lines. `swath diag list` takes every
`list` flag plus the diagnostic ones: `--sequential`, `--probe`, `--floors`, `--max-keys` (range-engine sources), `--no-journal` and
`--no-spill`; they exist for comparisons and tests, not production runs. Flags take one or two dashes. The
Java distribution keeps its own `swath` launcher; the two are separate tools.
Every Java CLI option, output mode, exit code and report field against this tool: [cli-inventory.md](cli-inventory.md).
Running it in production (sizing, watching, resuming, reading the output): [operating.md](operating.md).

## Flags

Generated from the flag set (`TestCLIDocFlags` fails when this block is stale; `UPDATE=1 go test ./cmd/swath`
rewrites it):

<!-- flags:begin -->
```text
usage: swath list SOURCE --output DIR [flags]
  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]

  -anonymous
    	S3: send unsigned requests, ignoring any credentials
  -count-only
    	walk and count without writing a dataset
  -dualstack
    	S3: use the dual-stack (IPv6) endpoint
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS); fewer are admitted when --memory or the descriptor limit cannot carry them, never fewer than one
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fips
    	S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  -gcs-anonymous
    	GCS: send no credentials, ignoring any in the environment
  -gcs-credentials file
    	GCS: service-account or authorized-user JSON file (default: Application Default Credentials)
  -gcs-endpoint URL
    	GCS: JSON API base URL (default https://storage.googleapis.com/storage/v1/)
  -gcs-request-timeout duration
    	GCS: per-request timeout; a request that exceeds it is retried (default 30s)
  -gcs-user-project project
    	GCS: bill a requester-pays bucket's requests to this project
  -limiter
    	requests: adaptive concurrency: a throttle answer lowers the requests in flight (x0.7), growth returns one per second after 10 s without one; false: a fixed --workers in flight (default true)
  -limiter-slow-start
    	requests: with -limiter, start at 4 requests in flight and double each second up to --workers until the first throttle answer or timeout (Java swath's slow start); applies to swath resume too
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
  -part-interval duration
    	close an open part with rows after this duration although not full, bounding the age of the durable progress; 0: parts close by size only (default 30s)
  -part-size bytes
    	target bytes per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB) (default 67108864)
  -path-style
    	S3: path-style addressing
  -profile profile
    	S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  -region string
    	S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
  -report file
    	also write the run report (JSON, with the outcome and the attempt id) to this file when the attempt ends, on failure too
  -report-format text
    	text (one line) or json (the run report, as in run.json) (default "text")
  -request-payer
    	S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)
  -request-rate N
    	requests: cap every HTTP attempt (pages, probes, retries, requeues) at N per second, after the limiter's permit; refused when the spacing exceeds half --stall-timeout; 0: no cap
  -request-timeout duration
    	S3: per-request timeout; a request that exceeds it is retried (default 30s)
  -seed
    	ranges: tile the keyspace from delimiter probes of the folder hierarchy before splitting
  -spill-compress
    	compress spill files (zstd level 1)
  -spill-max bytes
    	cap on spill bytes on disk (0: half the free space on the spill filesystem at start, after --spill-reserve); past it, listing waits instead of spilling
  -spill-reserve bytes
    	free bytes spill leaves on its filesystem, for the parts still to be written (default 1073741824)
  -stall-timeout duration
    	stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
  -workers int
    	listing goroutines (default 64, whatever the CPU count)

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, memory, spill-max, spill-reserve, spill-compress, part-interval, report-format, report, max-duration, stall-timeout (runtime) and
profile, anonymous, request-timeout, gcs-anonymous, gcs-credentials, gcs-request-timeout, limiter, limiter-slow-start, request-rate (operational) may change.
swath show [--journal] DIR shows a dataset or an interrupted run.

Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.

usage: swath diag list SOURCE [flags]   (the list flags plus diagnostic ones)
  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]

  -anonymous
    	S3: send unsigned requests, ignoring any credentials
  -count-only
    	walk and count without writing a dataset
  -dualstack
    	S3: use the dual-stack (IPv6) endpoint
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS); fewer are admitted when --memory or the descriptor limit cannot carry them, never fewer than one
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fips
    	S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  -floors
    	ranges: the gate-chain floors: never split a range with at most 4 pages of keys left, nor leave the child past the pivot below 2 pages
  -gcs-anonymous
    	GCS: send no credentials, ignoring any in the environment
  -gcs-credentials file
    	GCS: service-account or authorized-user JSON file (default: Application Default Credentials)
  -gcs-endpoint URL
    	GCS: JSON API base URL (default https://storage.googleapis.com/storage/v1/)
  -gcs-request-timeout duration
    	GCS: per-request timeout; a request that exceeds it is retried (default 30s)
  -gcs-user-project project
    	GCS: bill a requester-pays bucket's requests to this project
  -limiter
    	requests: adaptive concurrency: a throttle answer lowers the requests in flight (x0.7), growth returns one per second after 10 s without one; false: a fixed --workers in flight (default true)
  -limiter-slow-start
    	requests: with -limiter, start at 4 requests in flight and double each second up to --workers until the first throttle answer or timeout (Java swath's slow start); applies to swath resume too
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
  -part-interval duration
    	close an open part with rows after this duration although not full, bounding the age of the durable progress; 0: parts close by size only (default 30s)
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
  -report file
    	also write the run report (JSON, with the outcome and the attempt id) to this file when the attempt ends, on failure too
  -report-format text
    	text (one line) or json (the run report, as in run.json) (default "text")
  -request-payer
    	S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)
  -request-rate N
    	requests: cap every HTTP attempt (pages, probes, retries, requeues) at N per second, after the limiter's permit; refused when the spacing exceeds half --stall-timeout; 0: no cap
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
    	cap on spill bytes on disk (0: half the free space on the spill filesystem at start, after --spill-reserve); past it, listing waits instead of spilling
  -spill-reserve bytes
    	free bytes spill leaves on its filesystem, for the parts still to be written (default 1073741824)
  -stall-timeout duration
    	stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
  -structure
    	ranges: structure probes: pivot on folder boundaries listed alongside a range's pages
  -workers int
    	listing goroutines (default 64, whatever the CPU count)

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, memory, spill-max, spill-reserve, spill-compress, part-interval, report-format, report, max-duration, stall-timeout (runtime) and
profile, anonymous, request-timeout, gcs-anonymous, gcs-credentials, gcs-request-timeout, limiter, limiter-slow-start, request-rate (operational) may change.
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
  dataset is then marked incomplete; such a run keeps no journal and cannot be resumed). `--endpoint` and `--path-style` select S3-compatible stores. S3 directory buckets (Express One Zone, names ending in
  `--x-s3`) are refused before any request (exit 2, class `refused`).
- **Workers:** `--workers` (default 64 whatever the CPU count, Java's default; listing waits on the service,
  not the CPU) is the listing goroutines and the request limiter's start; `--encoders` stays CPU-derived.
- **Request admission.** By default (S3 and GCS) every request takes a permit from an adaptive limit that
  starts at `--workers`, falls by 0.7 on each throttle answer and grows back by one per second after 10 s
  without one (Java swath's rules; [internals/providers.md](internals/providers.md)); `--limiter=false` keeps a
  fixed `--workers` requests in flight. `--limiter-slow-start` (off by default) starts the limit at 4 instead
  and doubles it each second up to `--workers` until the first throttle answer or timeout (Java's slow start);
  a resume replays it from the recorded command and may add or undo it, since a resumed client starts a new
  limiter (counters `slow_start.*`, feature `slow_start`, on only with the limiter). `--request-rate N` (S3 and GCS, default 0: none) also caps the run's
  HTTP attempts at N per second (at least 0.001), every attempt counted (pages, probes, client retries, engine
  requeues): a token bucket of one token, taken after the limiter's permit, so the k-th token of a burst is due
  (k-1)/N seconds after the first, as with Java's `--request-rate`. Tokens are scheduled, not the sends: a caller
  delayed after its token can send together with the next, so the cap holds over a window but sends are not
  strictly spaced. A quota shared across a fleet is the use. Counters `rate.waits`, `rate.wait_ns`.
- **Requester pays:** `--request-payer` sends `x-amz-request-payer: requester` (signed) on every request, so
  a requester-pays bucket can be listed at the caller's charge.
- **S3 credentials:** `--anonymous` sends unsigned requests whatever the environment holds. Otherwise the AWS
  SDK's default credential chain applies: environment, shared files and profiles (`--profile`), SSO, web
  identity, credential processes, AssumeRole, container and instance roles. Requests go unsigned only when
  the chain is empty (the environment and the shared files name no source, and the machine shows no sign of
  EC2) and the lookup finds nothing; a configured source that fails stops the run with cause `auth`, and one
  that is slow or failing transiently is waited for and retried, never replaced by anonymity. Secrets are never taken from flags (they would show in `ps`). Details:
  [internals/providers.md](internals/providers.md).
- **Spill:** when the reorder budget is full, rows of ranges ahead of the frontier go to disposable files
  under `DIR/.swath/spill/` instead of stalling the workers; `swath diag list --no-spill` turns it off,
  `--spill-compress` compresses them. Spill stops (and listing waits instead) at `--spill-max` bytes on disk
  (default 0: half the space free on the spill filesystem when the run starts, after the reserve) or when the
  filesystem would drop below `--spill-reserve` free bytes (default 1 GiB)
  ([internals/ordering.md](internals/ordering.md)).
- **Encoders:** `--encoders` (default GOMAXPROCS) is a ceiling: each encoder is admitted against the half of
  `--memory` outside the row budget (one writer's working set each) and the descriptor limit (after the spill
  readers, the workers' connections and a reserve), never fewer than one; a warning names the shortfall.
- **Durable-progress age:** `--part-interval` (default 30 s, Java's rotation interval; 0: off) closes an open part that holds rows once
  it is that old, although not full, so the journal's recovery point (what a resume keeps) is never older than
  about that on a slow frontier; with 0, parts close by `--part-size` only and their boundaries do not depend on
  timing. It may change on `swath resume`.
- **Resume:** an S3 run keeps a journal in `DIR/.swath/` with the command that started it; after a crash or
  interruption, `swath resume DIR` continues it ([internals/durability.md](internals/durability.md)).
  `swath diag list --no-journal` turns it off (parts are then published only at the end).
- **Exit status and outcome.** A supervisor's outcome of record is the exit code plus the report at the path
  it names with `--report` (outside the output directory, a fresh path per attempt, or removed before the
  attempt starts): `outcome` (`class`, `exit_code`, `resumable`, `cause`, `unrecorded`; `status`, `complete` and
  `stop_reason` follow the class: a run that did not commit is `complete` false with its class as the
  `stop_reason`, and a committed run's agree with the manifest's `coverage`) and `attempt` (`id`,
  random, and `started`). For a run that did not complete, `DIR/error.json` holds the same outcome in the
  dataset (`class`, `exit_code`, `message`, `resumable`, `cause`: the provider's failure class such as
  `throttle` or `access_denied`, or `no_space` for a full disk; and the same `attempt`); it never needs stderr.
  The command classifies the outcome once, before any account is written, and that classification is frozen:
  `DIR/run.json` (a committed dataset), `error.json`, the report, the exit code and a forced exit's outcome agree
  on `class`, `exit_code`, `resumable` and `cause`, also when a stop cause (the deadline, a signal) arrives
  after the run settled (a dataset committed with partial coverage is `incomplete`, exit 1, in all of them).
  The report's `output` counts the parts that stay in the output on failure too: after a failure of a
  journaled run, `parts_written`, `bytes` and `rows_durable` are the parts the journal acknowledged (the append
  returned, a failed rename after it included: the resume renames them), the recovered ones included in
  `bytes` and `rows_durable`.
  `error.json` is a convenience: it is current only when its `attempt.id` is the report's. No program can
  promise a new file on storage that refuses writes, so an attempt whose `error.json` write fails leaves the
  previous one in place, possibly with the same class and exit code; its report names another attempt and says
  why in `outcome.unrecorded`. With no report (it could not be written either; a usage error, exit 2, before
  the flags were read), only the exit code is current. `error.json` and the
  progress file are written only while the run holds the output lock, and `error.json` is removed when a later
  run or resume completes. `error.json` is always the last attempt's outcome: a resume (or `--overwrite`) that
  fails before it runs (credentials that do not load, a damaged journal, a failed recovery) takes the lock and
  records its own outcome over the interrupted run's, if that run is still the one it read (its journal and
  `error.json` unchanged) and the dataset still uncommitted once it holds the lock. An attempt that cannot
  record it (another process owns the output, whose outcome stands; another process completed or replaced the
  run first; a refusal that leaves the directory as it was, such as a directory without a
  journal, where nothing is created, or an unusable output; a failed write) prints it on stderr as one line, `swath: outcome: {...}`, after the
  reason; the JSON report carries the reason as `outcome.unrecorded`, which is never empty when the outcome is
  not in `error.json`.

  | Code | Class | Meaning | Resumable |
  |---|---|---|---|
  | 0 | `completed` | the dataset is committed (or a dry run finished), also by an earlier attempt: `swath resume` on a complete dataset | – |
  | 1 | `fatal`, `incomplete` | a run failure a resume will not fix (access denied, no such bucket, an invariant, another process owning the output; a protocol violation, `cause` `protocol`, which outranks a stop cause and which the journal records, so `swath resume` refuses the run), or a dataset committed with partial coverage (a key limit), so a partial dataset never exits 0 | no |
  | 2 | `usage`, `refused` | `usage`: a malformed command line or an unknown scheme; `refused`: a refusal that is the caller's to fix, before anything ran: a non-empty `--output` without `--overwrite`, `--overwrite` on a directory that is not this tool's dataset, a new run over an interrupted one, an output that is the listed root (or inside it, replaced), a resume whose identity changed, a resume with nothing to resume, an S3 directory bucket | `usage`: no; `refused`: the output is as it was, so yes when it holds an interrupted run |
  | 74 | `disk_full` | a part, a journal record or the manifest could not be written: its filesystem is full (`cause` `no_space`; spill waits instead of failing) | yes, journal intact, once there is space |
  | 75 | `stuck` | no progress and no answered request for `--stall-timeout` (default 10 min; a warning at half; also after the 15 s shutdown grace); `cause` names what rose meanwhile: `stuck_throttle` (throttle answers), `stuck_timeouts` (attempts past the request timeout, no throttle) or `stuck_no_rows` (neither: failing or hung requests, or a stopped pipeline) | yes, journal intact |
  | 124 | `deadline` | `--max-duration` reached with the output unfinished (also after the 15 s shutdown grace) | yes |
  | 130 | `interrupted` | SIGINT (also after the 15 s shutdown grace) | yes |
  | 143 | `terminated` | SIGTERM, a graceful stop (also after the 15 s shutdown grace) | yes |

  "Resumable" holds when the output has an intact journal (S3); a filesystem run starts over. A source that
  stops early (a key limit) never exits 0 with a partial manifest unless its coverage says so.
- **Liveness.** The command alone decides that a run cannot go on: its watchdog watches the run's progress
  (rows listed, ordered, encoded, parts published) and the requests the service answered usably (pages without
  rows and probes included); a rise in either counts, and it stops the run as stuck after `--stall-timeout`
  without one. So work queued behind a low request limit, or a stretch of empty continuing pages, is alive while
  the service answers; a source that hangs, or only fails or throttles, is stuck. Nothing in the pipeline gives
  up on a slow source (S3 requeues a page after its retries). As with Java swath, a service that answers forever
  without rows keeps a run alive; `--max-duration` bounds it. A run of empty continuing pages must not repeat
  any of its last 1,024 tokens (an invalid page: exit 1, `cause` `protocol`, the run marked unresumable); a chain of ever fresh tokens is followed in bounded
  memory until the deadline. A stop (a signal, `--max-duration`, `--stall-timeout`) is graceful, and the process
  exits with its code 15 s later if the run has not stopped by then (I/O that does not return, say), first
  writing (bounded at 2 s, as the dump below) the same outcome the graceful stop would have to `error.json` and a
  minimal report without the run's statistics, its `message` naming the forced exit. The exit does not wait for
  any of that beyond 5 s after the grace (20 s after the stop in all): each line to stderr waits at most 250 ms
  and is dropped past it, so a stderr pipe nobody drains cannot hold the process (warnings are written the same
  way), and the process exits at that bound whatever still blocks. **Forensics:** at a stall, before the run is cancelled, and at a forced
  exit, every goroutine's stack (at most 1 MiB) and the progress counts are written to
  `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt` (replaced each time) while the run owns the output,
  else, or when that write fails or blocks for 2 s, to stderr.
- **What a hosted runner needs.** A finite `--max-duration` on every attempt (the default, 0, has no limit, and
  only the deadline stops a service that answers forever without rows), and its own process deadline above
  `--max-duration` plus 20 s, which kills the process (that attempt's report may then be missing: it is treated
  as unavailable, never replaced by an older `error.json`). Exit 75 is retried with `swath resume`. Exit 74:
  retry with a larger workspace (`swath resume` on the same output, moved to a larger disk or given space). Exit 124 is a
  bounded stop: the run is resumable but did not finish in the time given, so it is retried only under a
  separate budget (a count of attempts or a total time), and past that budget it needs a person: a source that
  keeps the run alive without rows hits 124 on every attempt.
- **Progress file.** While a run goes, `DIR/.swath/progress.json` is rewritten every 2 s by an atomic rename:
  `objects` (rows listed so far), `ordered`, `encoded`, `parts`, `advance` (their sum, which rises in every
  phase), `answered` (requests the service answered usably: liveness, not progress), `throttles` and
  `timeouts` (throttle answers and attempts past the request timeout), `mutex_wait_ms` and `mutex_max_hold_ms`
  (S3 and GCS: the range engine's decision mutex, the goroutines' summed wait for it and its longest hold;
  also the counters `mutex.wait_ns` and `mutex.max_hold_ns`), `started_at`, `as_of`,
  `duration_ms`. A supervisor watches that file; `DIR/run.json` follows the
  commit. `--max-duration` and `--stall-timeout` are runtime flags: `swath resume` replays the recorded
  values unless given again.
- **Environment:** `SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` name pprof output files;
  `SWATH_LOCALFS_ENGINE=portable` selects the portable filesystem engine; `SWATH_SPLIT_TRACE` traces S3 splits.
