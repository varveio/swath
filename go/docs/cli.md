# CLI

```text
swath list SOURCE --output DIR [flags]   list SOURCE into a sorted dataset in DIR
swath list SOURCE [-o FILE|-] [flags]    list SOURCE as text rows (TSV, JSONL, a table) to a file or stdout
swath resume DIR [runtime flags]         continue the interrupted run in DIR
swath show [--journal] DIR               show a dataset or an interrupted run
swath diag list SOURCE [flags]           list with diagnostic flags (comparisons, policy toggles)
swath --version                          print the build (tool, module version, VCS revision)
```

SOURCE is a local directory, `s3://bucket[/prefix]` or `gs://bucket[/prefix]`. `swath list` writes a dataset to `--output` (`-o`)
([internals/parquet-output.md](internals/parquet-output.md)), text rows to a file or stdout (**Text output**
below), or with `--count-only` only lists. It prints one
line, or the run report (`run.json`) with `--report-format json`; `--report FILE` also writes that report to
FILE when the attempt ends, on failure too. `swath resume DIR` replays the command the journal recorded; only
the runtime flags (`--workers`, `--encoders`, `--row-budget`, `--spill-max`, the diagnostic `--spill-reserve` and `--spill-compress`,
`--part-interval`, `--report-format`, `--report`, `--max-duration`, `--stall-timeout`, and the terminal flags
`--progress`, `--no-progress`, `--progress-interval`, `--stats`, `--no-stats`, `-v`, `-q`, `--color`) and the
operational ones, how requests are
authenticated, timed and admitted (`--profile`, `--anonymous`, `--request-timeout`, `--gcs-anonymous`,
`--gcs-credentials`, `--gcs-request-timeout`, `--limiter`, `--limiter-slow-start`, `--request-rate`,
`--bearer-token-command`, `--bearer-token-refresh-interval`), may be given again (the last value
wins: `--anonymous=false` undoes a recorded `--anonymous`); anything else is a usage error. The source and output
identity stays frozen: a resume whose overrides resolve another route (a profile with another region, say) is
refused by the identity check. `swath resume` on a committed dataset with complete coverage has nothing to do
and exits 0 (`DIR is already complete: nothing to resume`; the report's outcome is `completed`, `complete`
true), so a supervisor may resume until exit 0; one committed with partial coverage stays `incomplete`, exit 1.
`swath show DIR` prints a committed dataset's manifest summary (format, schema, source, scope, coverage, parts,
rows, key range) or an interrupted run's recorded command, frozen identity, committed parts and the key
`swath resume` would start after; `--journal` dumps the journal as JSON lines. `swath diag list` takes every
`list` flag plus the diagnostic ones: `--sequential`, `--floors`, `--max-keys` (range-engine sources), `--no-journal`,
`--no-spill`, `--spill-reserve` and `--spill-compress`; they exist for comparisons and tests, not production runs. Flags take one or two dashes. The
Java distribution keeps its own `swath` launcher; the two are separate tools.
Every Java CLI option, output mode, exit code and report field against this tool: [cli-inventory.md](cli-inventory.md).
Running it in production (sizing, watching, resuming, reading the output): [operating.md](operating.md).

## Flags

Generated from the flag set (`TestCLIDocFlags` fails when this block is stale; `UPDATE=1 go test ./cmd/swath`
rewrites it):

<!-- flags:begin -->
```text
usage: swath list SOURCE [--output DIR|FILE|-] [flags]
  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]

  -anonymous
    	S3: send unsigned requests, ignoring any credentials
  -bearer-token-command command
    	S3: shell command whose stdout is a bearer token, sent instead of SigV4 signatures (GCS's XML API: 'gcloud auth print-access-token'); never recorded, so swath resume needs it again
  -bearer-token-refresh-interval duration
    	S3: run --bearer-token-command again after this duration (default 45m)
  -color string
    	colour the progress line and the statistics: auto, always or never (auto: a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it) (default "auto")
  -count-only
    	walk and count without writing a dataset
  -dualstack
    	S3: use the dual-stack (IPv6) endpoint
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS, lowered to what --row-budget and the descriptor limit carry, never below one); an explicit count is kept, with a warning when they cannot carry it
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store (path-style addressing unless --path-style=false)
  -endpoint-url string
    	S3: Java swath's name for --endpoint
  -exclude regex
    	drop keys in which this regex (Go RE2 syntax) matches anywhere
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fips
    	S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  -force
    	Java swath's name for --overwrite
  -format string
    	stdout rows: auto, table, tsv or jsonl (auto: the table on a terminal, TSV otherwise); a -o file's name says its format
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
  -include regex
    	keep keys in which this regex (Go RE2 syntax) matches anywhere; ^ with a literal prefix also narrows the listed prefix
  -limiter
    	requests: adaptive concurrency: a throttle answer lowers the requests in flight (x0.7), growth returns one per second after 10 s without one; false: a fixed --workers in flight (default true)
  -limiter-slow-start
    	requests: with -limiter, start at 4 requests in flight and double each second up to --workers until the first throttle answer or timeout (Java swath's slow start); applies to swath resume too
  -max-duration duration
    	stop the run after this duration (exit 124; a journaled run is resumable); 0: no limit
  -max-size size
    	keep objects of at most this size
  -metadata tier
    	local: metadata tier per entry: names (no stat), stat (the default), statx (adds birth_time; Linux)
  -min-size size
    	keep objects of at least this size (1k, 256mb, 1.5GiB: binary units)
  -modified-since time
    	keep objects modified at or after this time (2026-01-01, 2026-01-01T12:00:00, an offset or Z; UTC without one)
  -modified-until time
    	keep objects modified at or before this time
  -no-dirs
    	local: omit kind=DIR rows
  -no-link-target
    	local: do not read symlink targets
  -no-progress
    	no live progress on stderr
  -no-stats
    	no end-of-run statistics on stderr
  -o string
    	short for --output
  -output directory
    	output: a dataset directory, a .tsv or .jsonl file (optionally .gz or .zst), or - for stdout (the default without --count-only)
  -overwrite
    	replace a previous dataset in --output
  -part-interval duration
    	close an open part with rows after this duration although not full, bounding the age of the durable progress; 0: parts close by size only (default 30s)
  -part-rotation-max-rows N
    	also close a part at N rows (Java's flag; 0: by size and --part-interval only); part of the resume identity
  -part-size bytes
    	target bytes per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB) (default 67108864)
  -path-style
    	S3: path-style addressing (default: on with --endpoint, as Java's --force-path-style)
  -profile profile
    	S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  -progress
    	print live progress on stderr (default: when stderr is a terminal and neither -q nor -v is given; --progress-interval implies it)
  -progress-interval interval
    	progress interval (default 1s while the line redraws on a terminal, 30s for appended records; at least 1s)
  -q	quiet: no warnings, no automatic progress or statistics (-qq: no error line either; the exit code and the reports stand)
  -qq
    	short for -q -q
  -quiet
    	short for -q
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
  -row-budget bytes
    	bytes of rows held between listing and encoding (reorder storage and the encoders' queues); not a cap on resident memory (default 134217728)
  -seed
    	ranges: tile the keyspace from delimiter probes of the folder hierarchy before splitting
  -spill-max bytes
    	the spill disk budget: cap on spill bytes on disk (0: half the free space on the output filesystem at start, after a reserve of one part per encoder and at least 1 GiB); past it, or at the reserve, listing waits instead of spilling
  -stall-timeout duration
    	stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
  -stats
    	print the end-of-run statistics on stderr (default: for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe)
  -storage-class classes
    	keep objects in these storage classes (STANDARD,GLACIER; repeatable)
  -v	verbose: a start line, and progress as appended records (-vv, -vvv: the same)
  -vv
    	short for -v -v
  -vvv
    	short for -v -v -v
  -workers int
    	listing goroutines (default 64, whatever the CPU count)

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, row-budget, spill-max, spill-reserve, spill-compress, part-interval, report-format, report, max-duration, stall-timeout, progress, no-progress, progress-interval, stats, no-stats, v, vv, vvv, q, qq, quiet, color (runtime) and
profile, anonymous, request-timeout, gcs-anonymous, gcs-credentials, gcs-request-timeout, limiter, limiter-slow-start, request-rate, bearer-token-command, bearer-token-refresh-interval (operational) may change.
swath show [--journal] DIR shows a dataset or an interrupted run.

Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.

usage: swath diag list SOURCE [flags]   (the list flags plus diagnostic ones)
  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]

  -anonymous
    	S3: send unsigned requests, ignoring any credentials
  -bearer-token-command command
    	S3: shell command whose stdout is a bearer token, sent instead of SigV4 signatures (GCS's XML API: 'gcloud auth print-access-token'); never recorded, so swath resume needs it again
  -bearer-token-refresh-interval duration
    	S3: run --bearer-token-command again after this duration (default 45m)
  -color string
    	colour the progress line and the statistics: auto, always or never (auto: a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it) (default "auto")
  -count-only
    	walk and count without writing a dataset
  -dualstack
    	S3: use the dual-stack (IPv6) endpoint
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS, lowered to what --row-budget and the descriptor limit carry, never below one); an explicit count is kept, with a warning when they cannot carry it
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store (path-style addressing unless --path-style=false)
  -endpoint-url string
    	S3: Java swath's name for --endpoint
  -exclude regex
    	drop keys in which this regex (Go RE2 syntax) matches anywhere
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fips
    	S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  -floors
    	ranges: the gate-chain floors: never split a range with at most 4 pages of keys left, nor leave the child past the pivot below 2 pages
  -force
    	Java swath's name for --overwrite
  -format string
    	stdout rows: auto, table, tsv or jsonl (auto: the table on a terminal, TSV otherwise); a -o file's name says its format
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
  -include regex
    	keep keys in which this regex (Go RE2 syntax) matches anywhere; ^ with a literal prefix also narrows the listed prefix
  -limiter
    	requests: adaptive concurrency: a throttle answer lowers the requests in flight (x0.7), growth returns one per second after 10 s without one; false: a fixed --workers in flight (default true)
  -limiter-slow-start
    	requests: with -limiter, start at 4 requests in flight and double each second up to --workers until the first throttle answer or timeout (Java swath's slow start); applies to swath resume too
  -max-duration duration
    	stop the run after this duration (exit 124; a journaled run is resumable); 0: no limit
  -max-keys N
    	ranges: with -sequential, stop after N objects (experiments; the dataset is marked incomplete)
  -max-size size
    	keep objects of at most this size
  -metadata tier
    	local: metadata tier per entry: names (no stat), stat (the default), statx (adds birth_time; Linux)
  -min-size size
    	keep objects of at least this size (1k, 256mb, 1.5GiB: binary units)
  -modified-since time
    	keep objects modified at or after this time (2026-01-01, 2026-01-01T12:00:00, an offset or Z; UTC without one)
  -modified-until time
    	keep objects modified at or before this time
  -no-dirs
    	local: omit kind=DIR rows
  -no-journal
    	diagnostic: publish parts only at the end, without the journal that swath resume needs
  -no-link-target
    	local: do not read symlink targets
  -no-progress
    	no live progress on stderr
  -no-spill
    	diagnostic: wait on the reorder budget instead of spilling rows ahead of the frontier to disk
  -no-stats
    	no end-of-run statistics on stderr
  -o string
    	short for --output
  -output directory
    	output: a dataset directory, a .tsv or .jsonl file (optionally .gz or .zst), or - for stdout (the default without --count-only)
  -overwrite
    	replace a previous dataset in --output
  -part-interval duration
    	close an open part with rows after this duration although not full, bounding the age of the durable progress; 0: parts close by size only (default 30s)
  -part-rotation-max-rows N
    	also close a part at N rows (Java's flag; 0: by size and --part-interval only); part of the resume identity
  -part-size bytes
    	target bytes per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB) (default 67108864)
  -path-style
    	S3: path-style addressing (default: on with --endpoint, as Java's --force-path-style)
  -profile profile
    	S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  -progress
    	print live progress on stderr (default: when stderr is a terminal and neither -q nor -v is given; --progress-interval implies it)
  -progress-interval interval
    	progress interval (default 1s while the line redraws on a terminal, 30s for appended records; at least 1s)
  -q	quiet: no warnings, no automatic progress or statistics (-qq: no error line either; the exit code and the reports stand)
  -qq
    	short for -q -q
  -quiet
    	short for -q
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
  -row-budget bytes
    	bytes of rows held between listing and encoding (reorder storage and the encoders' queues); not a cap on resident memory (default 134217728)
  -seed
    	ranges: tile the keyspace from delimiter probes of the folder hierarchy before splitting
  -sequential
    	ranges: list with one continuation-token chain (no ranges; the reference)
  -spill-compress
    	diagnostic: compress spill files (zstd level 1)
  -spill-max bytes
    	the spill disk budget: cap on spill bytes on disk (0: half the free space on the output filesystem at start, after a reserve of one part per encoder and at least 1 GiB); past it, or at the reserve, listing waits instead of spilling
  -spill-reserve bytes
    	diagnostic: free bytes spill leaves on its filesystem (0: derived, one part per encoder and at least 1 GiB)
  -stall-timeout duration
    	stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
  -stats
    	print the end-of-run statistics on stderr (default: for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe)
  -storage-class classes
    	keep objects in these storage classes (STANDARD,GLACIER; repeatable)
  -structure
    	ranges: structure probes: pivot on folder boundaries listed alongside a range's pages
  -v	verbose: a start line, and progress as appended records (-vv, -vvv: the same)
  -vv
    	short for -v -v
  -vvv
    	short for -v -v -v
  -workers int
    	listing goroutines (default 64, whatever the CPU count)

swath resume DIR [--workers N ...] continues an interrupted run with its recorded command;
only workers, encoders, row-budget, spill-max, spill-reserve, spill-compress, part-interval, report-format, report, max-duration, stall-timeout, progress, no-progress, progress-interval, stats, no-stats, v, vv, vvv, q, qq, quiet, color (runtime) and
profile, anonymous, request-timeout, gcs-anonymous, gcs-credentials, gcs-request-timeout, limiter, limiter-slow-start, request-rate, bearer-token-command, bearer-token-refresh-interval (operational) may change.
swath show [--journal] DIR shows a dataset or an interrupted run.

Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.
```
<!-- flags:end -->

## Semantics worth stating

- **Sizes** accept `KiB`, `MiB`, `GiB` and `KB`, `MB`, `GB` suffixes.
- **`--row-budget`** bounds the bytes of rows and scheduler metadata in flight (reorder storage and encoder
  queues); it is not a cap on resident memory. See [internals/ordering.md](internals/ordering.md).
- **`--output`** is refused when non-empty unless `--overwrite` (or Java's alias `--force`), which replaces only
  a previous dataset of this tool and never anything inside the listed root.
- **Text output** (S3 and GCS sources; a local tree's keys are bytes, which JSON cannot carry, so it needs the
  dataset). The `-o` name decides: `NAME.tsv` or `NAME.jsonl`, optionally `.gz` or `.zst`, is one file in that
  format and compression; any other name is the Parquet dataset; no `-o`, or `-o -`, is stdout, where `--format
  auto|table|tsv|jsonl` chooses (`auto`, the default: the table on a terminal, TSV otherwise) and a pipe
  compresses (`| zstd`). `--format` with a `-o` file or directory is refused, because the name already says it.
  Rows are the S3 API's object (ListObjectsV2 `Contents`), so they are read with what already reads S3 listings:
  - JSONL, one object per line, the API's names in its order: `Key`, `LastModified`, `ETag`,
    `ChecksumAlgorithm` (a list), `ChecksumType`, `Size`, `StorageClass`, `Owner` (`DisplayName`, `ID`; only with
    `--fetch-owner`). A field without a value, or one the provider lacks (GCS has no checksum or owner fields), is
    left out, not null. A line is byte for byte what `aws s3api list-objects-v2 ... | jq -c '.Contents[]'`
    prints for the object: `ETag` keeps its quotes (the dataset stores it bare), `LastModified` is printed as the
    AWS CLI does (UTC, `+00:00`, a microsecond fraction only when not zero), strings are escaped as jq does.

    ```text
    {"Key":"logs/2023/app.log","LastModified":"2023-11-14T22:13:20+00:00","ETag":"\"fba9dede5f27731c9771645a39863328\"","ChecksumAlgorithm":["CRC64NVME"],"ChecksumType":"FULL_OBJECT","Size":434234,"StorageClass":"STANDARD"}
    ```
  - TSV: the same fields as columns in the same order under one header line, a column for each field the
    provider has (S3: all nine, `Owner.DisplayName` and `Owner.ID` flattened; GCS: `Key LastModified ETag Size
    StorageClass`), an absent value empty, `ChecksumAlgorithm` its one value.

    ```text
    Key	LastModified	ETag	ChecksumAlgorithm	ChecksumType	Size	StorageClass	Owner.DisplayName	Owner.ID
    logs/2023/app.log	2023-11-14T22:13:20+00:00	"fba9dede5f27731c9771645a39863328"	CRC64NVME	FULL_OBJECT	434234	STANDARD		
    ```
  - The table: the `aws s3 ls --recursive` line, the time to the second, the size right-aligned in 10 columns,
    the key, no header; in UTC (aws prints local time), so two runs' rows compare equal wherever they ran.

    ```text
    2023-11-14 22:13:20     434234 logs/2023/app.log
    ```

  One escaping rule for all three, JSON's string escapes: `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, any other byte
  below 0x20 and 0x7f as `\u00xx`, and `\"` inside JSON only. It is reversible, keeps a row on one line and a
  TSV field between its tabs, and never sends a terminal a control byte. Other provider columns (GCS
  `generation`, `md5`, `crc32c`, `content_type`) have no S3 field and are not written: they are in the dataset.
  A file is written to a temporary beside it and installed only when complete (synced, then renamed, the
  directory synced), replacing a previous file; a failed run leaves the destination as it was. A reader that closes
  stdout (`| head`) ends the run at once with exit 0, the report's outcome `stopped`, `stop_reason`
  `downstream_closed`, `complete` false. On stdout the one-line summary is not printed (stdout carries the
  rows; `--stats` prints the account on stderr) and `--report-format json` is refused (use `--report FILE`).
  Text runs keep no journal: `swath resume` refuses a text destination, and an interrupted text run is run
  again. `output.text` in the report names the format, destination, compression, rows and bytes.
- **Filters** (Java's seven): `--include REGEX` and `--exclude REGEX` keep or drop keys in which the expression
  matches anywhere (a substring search, as Java's `find`), `--min-size` and `--max-size` keep sizes within the
  inclusive bounds (Java's units: `1k`, `256mb`, `1.5GiB`, binary, case-insensitive), `--modified-since` and
  `--modified-until` keep modification times within the inclusive bounds (`2026-01-01`, `2026-01-01T12:00:00`
  or with `Z` or an offset; UTC without one; a row without a time is kept, as Java's), `--storage-class
  A,B` (repeatable) keeps the listed classes (case-insensitive). They reduce the rows written, not the requests
  made, with one exception: an `--include` anchored at the start with a literal (`^logs/2026/`) narrows the listed
  prefix to it (the same rows, fewer requests; the report's `invocation.source` names the narrowed root). The
  dropped rows are counted in `output.rows_filtered` (`rows_listed` is what passed). The filters are part of the
  resume identity (and of `args_hash`): a resume keeps them and refuses others. A filtered dataset says so in its
  manifest: `coverage.filters` holds Java's descriptions (`include=…`, `exclude=…`, `min_size=…`, `max_size=…`,
  `modified_after=…`, `modified_before=…`, `storage_class=…`), and a reader that needs the whole source refuses a
  dataset whose `coverage.filters` is not empty ([upgrade.md](upgrade.md)). **Regular expressions are Go's RE2,
  not Java's:** the common syntax is the same, but backreferences (`\1`), lookahead and lookbehind, possessive
  quantifiers and atomic groups do not exist and are refused (exit 2); `\Z` is not supported (`\z` is), and
  some character-class and Unicode-property details differ. RE2 runs in time linear in the key, so no pattern
  can stall a listing.
- **S3 modes.** The default is the parallel core; `--seed` tiles the scope from one delimiter page first;
  `swath diag list --sequential` is the one-chain oracle and the only mode where `--max-keys` stops early (the
  dataset is then marked incomplete; such a run keeps no journal and cannot be resumed). `--endpoint` (or Java's
  name `--endpoint-url`) selects an S3-compatible store, addressed path-style by default, as Java's
  `--force-path-style` default with a custom endpoint; `--path-style=false` uses virtual-host addressing there. S3 directory buckets (Express One Zone, names ending in
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
- **Bearer tokens** (S3 client, Java's flags): `--bearer-token-command CMD` runs `CMD` with `/bin/sh -c`; its
  stdout (trimmed, one line) is a bearer token sent as `Authorization: Bearer TOKEN` on every request instead of a
  SigV4 signature, and no AWS credential is looked up (it excludes `--anonymous` and `--profile`). The use is an
  S3-compatible endpoint that takes OAuth tokens, such as GCS's XML API: `swath list s3://bucket --endpoint-url
  https://storage.googleapis.com --bearer-token-command 'gcloud auth print-access-token' -o DIR` (the native
  `gs://` provider is the alternative). The token is fetched again after `--bearer-token-refresh-interval`
  (default 45 m, under a one-hour token lifetime), one run of the command at a time however many requests need
  it; a command that exits non-zero, runs past 30 s, prints nothing or more than one line fails the request
  with cause `auth` (fail closed: a stale token is never reused); its message carries the exit code and up to
  4,096 characters of its stderr, control characters escaped. The command is never recorded: the journal,
  `run.json`, the report, `error.json` and the stderr outcome line show `--bearer-token-command=[redacted]`, so
  `swath resume DIR` must be given it again (`swath resume DIR --bearer-token-command CMD`) and refuses with a
  message saying so otherwise (exit 2).
- **Spill:** when the reorder budget is full, rows of ranges ahead of the frontier go to disposable files
  under `DIR/.swath/spill/` instead of stalling the workers. `--spill-max` is the one disk budget: spill stops
  (and listing waits instead) at that many bytes on disk (default 0: half the space free on the spill
  filesystem when the run starts, after the reserve) or when the filesystem would drop below the reserve, one
  part per encoder at full size and at least 1 GiB (room for the parts being written on the same filesystem).
  `swath diag list --no-spill` turns spill off, `--spill-reserve` sets the reserve and `--spill-compress`
  compresses the files
  ([internals/ordering.md](internals/ordering.md)).
- **Encoders:** the default count is GOMAXPROCS lowered to what the resources carry: one writer's working set
  (24 MiB) each from an allowance equal to `--row-budget`, apart from the rows, and the descriptor limit (after the spill readers,
  the workers' connections and a reserve), never fewer than one. An explicit `--encoders N` is kept as given;
  when the resources cannot carry it, a warning names the shortfall.
- **Row bound per part:** `--part-rotation-max-rows N` (default 0: off; Java's default was 2,000,000 with
  256 MB parts) also closes a part at N rows. The cut depends on the rows only, so it is part of the resume
  identity, a resume reproduces the boundaries, and a row-cut part does not calibrate the size ratio (only a part
  closed by size does); `resources.part_row_cuts` counts them.
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
  exit, every goroutine's stack and the progress counts are written whole to
  `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt` (replaced each time) while the run owns the output,
  else, or when that write fails or blocks for 2 s, to stderr, cut at 1 MiB.
- **What a hosted runner needs.** A finite `--max-duration` on every attempt (the default, 0, has no limit, and
  only the deadline stops a service that answers forever without rows), and its own process deadline above
  `--max-duration` plus 20 s, which kills the process (that attempt's report may then be missing: it is treated
  as unavailable, never replaced by an older `error.json`). Exit 75 is retried with `swath resume`. Exit 74:
  retry with a larger workspace (`swath resume` on the same output, moved to a larger disk or given space). Exit 124 is a
  bounded stop: the run is resumable but did not finish in the time given, so it is retried only under a
  separate budget (a count of attempts or a total time), and past that budget it needs a person: a source that
  keeps the run alive without rows hits 124 on every attempt.
- **Progress file.** While a run goes, `DIR/.swath/progress.json` is rewritten every 2 s by an atomic rename
  (the terminal display, `--progress`, samples the same snapshot, every second while it redraws)
  (while the run holds the output). This is its one schema (`listing.ProgressSnapshot`; `TestProgressSchemaDoc`
  keeps the two in step):

  | Field | Meaning |
  |---|---|
  | `objects` | rows listed so far |
  | `ordered`, `encoded` | rows handed to the encoders, rows written into parts |
  | `parts` | parts published |
  | `verified_bytes` | a resume's bytes of retained parts checked so far |
  | `advance` | the sum of the above; rises in every phase |
  | `answered` | requests the service answered usably (liveness, not progress) |
  | `throttles`, `timeouts` | throttle answers; attempts past the request timeout |
  | `part_age_ms` | time since the last part was published (or since the start): the recovery point's age |
  | `mutex_wait_ms`, `mutex_max_hold_ms` | the range engine's decision mutex: summed wait, longest hold |
  | `rows_retained` | rows a resume kept from the interrupted run (not in `objects`) |
  | `requests`, `retries` | HTTP attempts sent (pages, probes, client retries, engine requeues); attempts repeated (S3, GCS) |
  | `inflight`, `limit` | attempts in flight now; the adaptive limiter's current limit (0 without one) |
  | `cost_usd`, `cost_rate_per_1k_usd` | `requests` at the service's reference list rate, and that rate (absent on a custom endpoint) |
  | `order.nodes`, `order.markers`, `order.spill_readers`, `order.spill_live_bytes` | the order stage's live resources, each with a `peak_` twin (`peak_spill_live_bytes` for spill bytes) |
  | `started_at`, `as_of`, `duration_ms` | timing |

  A supervisor watches that file; `DIR/run.json` follows the
  commit. `--max-duration` and `--stall-timeout` are runtime flags: `swath resume` replays the recorded
  values unless given again.
- **Terminal** (Java's stderr surface). A live progress record on stderr, from the snapshot the progress file
  is written from: the phase (`starting`, `verifying` with a resume's bytes checked, `listing`), objects listed
  (with `+N recovered`: rows a resume kept), live and average keys/s, parts published, requests in flight,
  elapsed time since the command began, API calls and, on a known tariff, the labelled estimate; no percentage
  or ETA (a listing has no honest denominator), no key text. On a terminal it is redrawn in place (carriage
  return, `ESC[2K`; on Windows after enabling virtual-terminal processing, else plain lines), its trailing
  fields dropped whole to fit the width, every second; elsewhere it is appended as plain lines every 30 s.
  Shown by default when stderr is a terminal and neither `-q` nor `-v` is given; `--progress` forces it,
  `--no-progress` suppresses it, `--progress-interval D` (at least 1 s) sets the cadence and implies it.
  Warnings written while the line is drawn erase it and repaint it after. At the end, `--stats` prints Java's
  block on stderr: a disposition (`INCOMPLETE (class)` with `resume: swath resume DIR` for a resumable stop; a
  refused run prints only this line), `N objects in T · K keys/s` (with the whole invocation's time when it is
  over a second longer), `N API calls · R per 1k objects · in flight avg A · peak P`, throttles, retries and
  entry errors when any, the estimate, and `files · bytes written · peak RSS`. Without `--stats` it is printed
  for a run over 1.5 s, a run that wrote parts, and a run that did not complete; not with `-q`, not after a
  closed stdout reader; `--no-stats` never. The one-line summary stays on stdout. `-v` (`-vv`, `-vvv`: the same;
  Go has no finer log levels) prints a start line and shows progress as appended records; `-q` drops warnings
  and the automatic progress and statistics; `-qq` also drops the final error line (the exit code, `error.json`
  and `--report` stand). `--color auto|always|never` colours the progress line and the statistics (headline
  cyan, the rest dim, `INCOMPLETE` red): auto is on for a terminal unless `NO_COLOR` is set and not empty or
  `TERM=dumb`, and `CLICOLOR_FORCE` forces it. TTY detection and width: `golang.org/x/term`.
- **Report continuity** (Java's report fields with consumers). `cost.api_calls` is every HTTP attempt this
  invocation sent (pages, probes, client retries, engine requeues; the field the cloud importer reads as the
  request count); `cost.cost_usd` is that count at `cost.basis.rate_per_1k_usd` (0.005, `source`
  `aws-list-reference-rate` for AWS S3, `gcs-class-a-reference-rate` for GCS's own endpoint), and both are
  `null` on a custom endpoint, whose tariff is unknown. `efficiency.keys_per_sec` is `output.rows_listed` over
  `timings.listing_ms` (listing and ordering; not publication, not the time before the run, not retained rows);
  `efficiency.api_calls_per_1k_objects` is `null` without a row. Clocks: `timings.wall_ms` runs from
  `timings.started_at` (the run's start, after the command line was read) to `finished_at`;
  `timings.session_ms` is the whole invocation, from `attempt.started` (credential lookup, a resume's
  verification and publication included); `listing_ms` ends with the order stage. `args_hash` (the field the
  cloud importer reads as the configuration digest) is SHA-256 over the run's frozen identity (tool, format,
  schema, key cap, writer, part size, row bound, provider, the source's identity fields such as bucket, prefix,
  route and region, and the filters): the same for a run and its resumes, never a credential, and not equal to
  Java's (which covered the listing scope only). `outcome.stop_source` says what stopped a stopped run:
  `sigint`, `sigterm`, `liveness_watchdog` (`--stall-timeout`), `timebox` (`--max-duration`), with
  `+forced_exit` when it did not stop within the grace (`forced_exit` alone if it came first); `error.json`
  carries it too. A hosted runner keeps reading the live `objects` from `progress.json`.
- **Environment:** `SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` name pprof output files;
  `SWATH_LOCALFS_ENGINE=portable` selects the portable filesystem engine; `SWATH_SPLIT_TRACE` traces S3 splits.
