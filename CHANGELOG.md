# Changelog

Notable changes per release. The human summary of the current release is in the
[release notes](docs/ops/dev/RELEASE_NOTES.md).

## Unreleased

## 0.4.0

0.4.0 replaces the 0.3 tool with a new one under the same name: one static binary per platform that lists S3
and S3-compatible stores, Google Cloud Storage, Azure Blob Storage and local or mounted filesystems in parallel,
totals their sizes (`swath du`), and writes rows as text or a key-sorted Parquet dataset (`swath list`) that
`swath show` summarizes, `swath du --from-dataset` totals and `swath resume` continues. The command line and
the dataset format are new: 0.4.0 does not read 0.3 datasets or resume 0.3 runs, and the 0.3 documentation is
at the [v0.3.2 tag](https://github.com/varveio/swath/tree/v0.3.2/docs); the 0.3 source tree is at the tag
`java-final`. The entries below describe 0.4.0 as released.

### Command line

- Commands: `swath list` (also `ls`), `swath du`, `swath show`, `swath resume`, `swath completion bash|zsh|fish`,
  and the diagnostic `swath diag list` and `swath diag count` (#394).
- Options are parsed GNU-style: short options bundle (`-vq`) and take attached values (`-d1`), a long option
  takes two dashes, options and operands interleave, and a default-on switch is turned off by `--no-NAME`.
  `-h` prints a command's short help and `--help` all of it; `swath` alone prints its short help. `-h` among
  other arguments (`swath du -h DIR`) is refused with exit 2: sizes are already human-readable.
- An option one provider has carries its prefix (`--s3-region`, `--s3-profile`, `--s3-requester-pays`,
  `--gcs-credentials-file`, `--gcs-billing-project`, `--azure-sas-token`, `--azure-assume-flat`); the options
  every object store has are bare (`--endpoint-url`, `--anonymous`, `--request-timeout` (default 30 s),
  `--ca-bundle`, `--no-verify-ssl`), and help lists them by registration, shared groups first (#338). An
  option given for another provider's source is refused.
- An unknown option names the nearest one, 0.3 spellings and GNU `du`'s included: `--region` suggests
  `--s3-region`, `--force-path-style` `--s3-path-style`, `--idle-timeout` `--stall-timeout`, `--compression`
  the `-o` name, `--format json` `jsonl`, `--format parquet` a dataset (`-o DIR`), and `swath du -s` the
  default `--depth 0`. `swath du` takes `--max-depth N` (as `--depth N`) and `-x` (also on `swath list`) for
  `--one-file-system`. There are no aliases.
- Every value error reads `--flag "VALUE": want ...` (sizes name their units), and every usage error of
  `swath list` and `swath du` ends with the help to read. In a SIZE, `K`, `M`, `KiB`, `MiB` and the like are
  binary and `KB`, `MB` decimal; a DURATION is Go's form with `d` and `w` (`90s`, `1h30m`, `2d`).
- SOURCE is `s3://`, `gs://`, `az://ACCOUNT/CONTAINER` (or its `https://ACCOUNT.blob.core.windows.net/` URL)
  or a path. `s3:/bkt` (one slash) is refused naming `s3://bkt`; an unsupported scheme names the forms SOURCE
  takes, and an S3 or GCS https URL (`https://bkt.s3.amazonaws.com/p`, `https://storage.googleapis.com/bkt/p`),
  `s3a://`, `gcs://` or `file://` names the source meant.
- `--include` and `--exclude` take globs, and regular expressions (Go's RE2) take `--include-regex` and
  `--exclude-regex`; both match the key below SOURCE on every source. A glob that carries a sign of a regular
  expression (a leading `^`, a final `$`, `\d` and its kin, `.*` with `\.`, `(a|b)`) is refused with exit 2
  before any listing, naming the regex option, so a 0.3 command line cannot silently match nothing (#526);
  `*\.parquet` (a quoted dot) stays a glob.
- `--modified-since` and `--modified-until` read a time without an offset as local time (`TZ`), as the tables
  print it and `find -newermt` reads it, and take the tables' form `2026-11-15 07:45[:00]`; `Z` or an offset
  is honored as given, and the run records the UTC time it names.
- `-o` chooses the output by its name: a dataset directory, `NAME.tsv` or `NAME.jsonl` optionally followed by
  `.gz` or `.zst` (in any case), `-` for stdout, or a bucket prefix with `--work-dir`. A name with anything
  else after `.tsv` or `.jsonl` (`rows.jsonl.zstd`: did you mean `.zst`) and a name ending in `.parquet` are
  refused; an existing file or a device as a dataset directory is refused with exit 2, naming what to use
  instead. A text file is always replaced, `--overwrite` or not. `--part-size`, `--part-interval` and
  `--part-max-rows` shape a dataset and are refused with text rows.
- `--report FILE` may name a device, a descriptor or a named pipe (`/dev/stderr`, `/dev/fd/3`, a FIFO), which
  is written as it is; a name whose directory does not exist is refused with exit 2 before listing.
- `swath list --progress` with rows on the same terminal is refused.

### Sources

- **S3 and S3-compatible stores.** General-purpose buckets; `--all-versions` lists versions and delete
  markers. Credentials come from the AWS SDK's default chain; those that expire (an assumed role, web
  identity, SSO, `credential_process`, container credentials, an instance role) are refreshed ahead of their
  expiry, by the smaller of 5 minutes and a quarter of their lifetime, for the source and for a bucket
  destination, and an expired-token answer renews them once and retries the request; keys that do not refresh
  stop the run with cause `auth`, saying so (#523). The report counts `credentials.refreshes` and
  `credentials.expired_refreshes`.
- **Google Cloud Storage** through the JSON API; `--all-versions` lists live and noncurrent generations.
  An error answer without a body names its HTTP status text and path.
- **Azure Blob Storage**, flat namespaces (`az://ACCOUNT/CONTAINER[/PREFIX]`), listed in byte order: names at
  U+E000 and above, which Azure lists in UTF-16 order, are reordered by an adapter whose cost the report's
  `order_*` counters show. Listings ask for the Arrow format and read XML wherever the service answers XML
  (accounts without Arrow listing, Premium Page Blob, Azurite, swath-replay), checking an Arrow page before
  decoding it; requests name service version `2026-06-06`, and the report counts pages per format
  (`list_format.*`) (#440, #451). On a 468,557-blob prefix a listing took 3.4 s and 3.9 CPU-seconds against
  29.5 s and 58 for XML ([measurements](docs/internals/measurements.md#arrow-listing-format-2026-11-10)). A page
  is 1,000 entries, not the service's 5,000: about half the time and memory on a 1.84M-blob container, for
  three times the requests ([measurements](docs/internals/measurements.md#azure-page-size-2026-11-10), #448).
  A flat namespace is established, anonymously or with a container SAS too, by the Data Lake host's 409
  `EndpointUnsupportedAccountFeatures` answer (the report's `namespace` reads `filesystem endpoint blocked`),
  so public open-data containers list without `--azure-assume-flat` (#457).
- **Azure `--all-versions`** lists blob versions, snapshots and soft-deleted blobs beside the current blob
  (`include=versions,snapshots,deleted`, no permission beyond listing), a name's rows consecutive, in Azure's
  order, and never split across parts or ranges; a resume restarts after the last complete name. Versions and
  snapshots are tested against recorded and synthetic listings only, soft-deleted blobs also against a public
  container. Rows carry every property List Blobs returns, at no extra request; `--azure-fetch-metadata`,
  `--azure-fetch-tags` (the SAS `t` permission or an Entra ID role that reads tags; a listing refused for it
  stops naming the permission) and `--azure-fetch-immutability` fill `metadata`, `tags` (JSON objects) and the
  immutability and legal hold columns.
- **Local and mounted filesystems.** A `--metadata statx` listing records a field the filesystem does not
  return (blocks on some network filesystems) as null, not 0.
- **Every object store.** Credentials that will not load or are refused are reported on one line, the source
  and the first line of its cause, `--anonymous` suggested only when swath was discovering credentials (#447);
  `-v` and the report keep the whole cause. An `--endpoint-url` with user information, a query or a fragment,
  and a SAS naming a request parameter (`comp`, `restype`, `prefix`, `marker`, ...), are refused before any
  request; endpoints and credentials are redacted in errors, the report and journal displays. A run in which
  nothing has answered (connections refused, a host not found) stops after 30 s (at most half
  `--stall-timeout`), exit 75, cause `unreachable`, naming the endpoint; once anything has answered,
  failures are retried as an outage, and the persistent-failure warning says how long retrying lasts. A
  listing of a prefix with no key
  warns `no key starts with "PREFIX"` (exit 0). The S3, GCS and Azure clients check the requests they send
  against a wire snapshot (#434, #446).

### Text output and the terminal

- Text rows (table, TSV, JSONL) use the dataset's column names for every source, chosen with `--columns`.
  `--no-header` leaves out the TSV header line of `swath list` and `swath du`. A JSONL row of a local tree
  always has `key` and `key_base64` (and `link_target`, `link_target_base64`): the text, null when not UTF-8,
  and the base64 bytes, null when UTF-8.
- `swath list`'s table puts the size first, heads a local tree's keys `path`, sizes `kind` to the usual kinds,
  ends a symlink's row with ` -> TARGET`, heads its local times with the zone's abbreviation
  (`last_modified (CET)`) and prints `-` for a missing value.
- `swath list -o DIR` and `-o FILE` print nothing on stdout, except the report with `--report -`.
- The progress line reads objects, bytes so far, elapsed, rate, then parts and (for an object store) in flight
  and API calls; while every request fails it shows `retrying: CAUSE`. `swath du` shows it on a terminal while
  it lists and erases it before its output.
- A run's closing lines state the result: `complete: 60,010 objects, 5.55 TiB in 4.2s · 14,391 keys/s → ds1 (1
  part, 279 KiB)`, then `next: swath show ds1 · swath du --from-dataset ds1` (or `swath resume DIR`). No rate
  under 2 s; a filtered run says `3 objects kept, 1 filtered out`, an interrupted one `7,003 saved, 12,003
  listed`, a resume the dataset's total with the rows from earlier attempts, `swath du` its total row's counts.
  API calls, in flight, throttles, retries, the estimate and peak RSS print with `--stats`, only those that
  happened. The verdict reads the same in the closing lines, `swath show` and `swath du --from-dataset`:
  `complete`, `partial: 1 unreadable entry`, `partial: --max-keys reached`, `stopped: --max-duration reached`,
  `interrupted`; the warnings that explain it print before it, and no error line repeats it (one remains under
  `-q`, which prints no closing line).
- The warning that names entries that could not be read gives their paths relative to SOURCE. A stop by
  SIGTERM says `stopped by SIGTERM`. `-v`'s start line shows the attempt id's first 8 characters. The tmpfs
  spill warning is printed at a run's first spill.
- A name from the source that reaches stderr (a path, a key, a provider error, a dataset's recorded source)
  cannot forge a `swath:` line or send terminal control sequences: text lines write control characters, line
  separators and invalid UTF-8 as visible escapes, JSON log lines as `\u` escapes (#521).
- `swath show` prints the verdict first, then labelled lines in reading order: source, when it was listed (local
  time), contents with the object bytes and grouped counts, coverage with the filters' rules, storage last;
  each layer is named (the SeekParquet format and draft, the schema id, the swath metadata version).
  `swath show MISSING` says `no such file or directory`. `swath show --journal` prints the journal's private
  format, a diagnostic that may change in any release.

### swath du

- `swath du SOURCE` on a terminal, without `-d`, `--sort` or `--format`, draws the largest prefixes and objects
  at whatever depth they are, filling the terminal: a tree, largest first, each line with its size, its count
  (`12.0k`), its share of the total, a bar (20 cells from 100 columns), the date of its newest object (local,
  the zone named once) and its path. As the terminal narrows the bar goes, then the share, then the date; a
  long path is cut in the middle, so no line wraps. Each parent's other children fold into one `(N more)` line
  carrying their sums and newest date, so every level adds up. A `!` after the size marks a line whose sums
  leave out a directory that could not be read, and one line under the view says so (`-v` names them). Every
  figure is exact, and memory stays small: a complete prefix is kept only among the 100 largest of its depth,
  and a prefix cut by a range's edge completes once the ranges on both sides are listed. Color follows
  `--color` and `NO_COLOR`. The report's `diagnostics.totals_view` counts what the view kept, dropped and
  merged. The tree with shares is `dust`'s and `pdu`'s idea (both Apache-2.0), implemented from their
  documented behavior.
- A pipe, `-d N`, `--sort` and an explicit `--format` print rows per prefix. Each row carries both byte
  figures under the dataset's names, `size` (the lengths) and `allocated_bytes` (a local file's blocks; null
  for a bucket), with `size_missing` and `allocated_bytes_missing` (a figure no counted object had is null,
  one some had is their sum beside the count of the others, an empty prefix's is 0), `max_last_modified` (the
  newest time under the prefix; a hard-linked file counted under another name adds none), `row_type` (`total`
  or `prefix`), `primary`, `sort` and `links_once` (whether the listing reads links). `--allocated` puts the
  allocated bytes first and makes `--sort size` order by them; `--apparent-size` is the default, and the two
  together are refused. The report's `output.totals` takes the same names.
- The table shows one figure, `size` (or `on disk` with `--allocated`), the counts with grouped digits, each
  prefix's newest time in local time to the second under a heading naming the zone, and each path indented
  two blanks a level; an unknown figure is `-`, a partial one `>= ` its lower bound, a directory that could not
  be read `-`. The total row is named by SOURCE as typed (`s3://bkt/logs/2026/0 (total)`); TSV and JSONL give
  it the key prefix SOURCE names. For a local tree a line on stderr gives the totals of both figures
  (`100 MiB in 6 files · 36.4 MiB on disk`); `-q` leaves it out. `swath du DIR` on a dataset directory prints
  a hint naming `--from-dataset`.
- `swath du --from-dataset DIR [SOURCE]` totals a committed local dataset instead of listing again: the same
  view, rows, depths, orders, formats and filters as `swath du` of the source, read in one parallel pass over
  the columns the totals need (#392). SOURCE is the dataset's source or a path or URL below it, a file
  included; any other SOURCE is refused naming the dataset's source, and a symbolic link there is refused.
  Stderr names the dataset's source, when it was listed (local time) and its coverage, and JSONL rows carry
  it as a `dataset` member with the filters; a dataset that is not complete prints its totals and exits 3. A
  dataset listed with `--metadata names` totals as the live run does. Options that shape a listing, a dataset
  in a bucket and an interrupted run are refused.

### The dataset

- A dataset is a SeekParquet dataset, format 1 draft revision 1 (`format_version` 0, `draft_revision` 1),
  with the swath dataset profile ([docs/dataset.md](docs/dataset.md)): `manifest.json`, installed last, commits
  the parts, and `extensions.swath` carries `"version": 1` first; `swath show`, `--seed-from` and `reader`
  refuse a manifest without it. SeekParquet is pinned at its release `go/v0.2.0`, which fixes the format.
- Schema ids follow `swath/<row-contract>/vN`: `swath/s3-objects/v1`, `swath/s3-versions/v1`,
  `swath/gcs-objects/v1`, `swath/gcs-generations/v1` (with a required `is_latest`), `swath/azure-objects/v1`,
  `swath/azure-versions/v1` and `swath/local-entries/v1`. One concept has one name in every schema:
  `last_modified`, `created_at`, `deleted_at`, `accessed_at` for times, `md5`, and `storage_class` for Azure's
  access tier too (with `storage_class_inferred` and `storage_class_changed_at`); a local `kind` is lower case
  (`file`, `dir`, `symlink`, ...). Every part has every column of its schema id.
- One provider token on every machine surface: `s3`, `gcs`, `azure`, `local` (the manifest's `source.kind`, the
  report's `source.name`, the journal's provider).
- The manifest's `coverage.complete` is true when nothing is missing: the listing reached the end of its scope
  and read every entry; filters do not make a dataset partial. `coverage.unreadable` counts the entries that
  could not be read. `coverage.filters` is a list of `{"filter": NAME, "value": VALUE}` named for the options
  (`modified_since`, `one_file_system`, `no_dirs`, ...; `null` for a switch). `collected` records the metadata
  tier and the optional columns the run asked for, and `committed_object_bytes` is absent when an object's size
  was not read (`--metadata names`).
- Every committed sorted dataset carries `cuts.txt` beside its manifest: its key distribution in the cut file
  format (a cut at key page ends, at most 262,144 lines and 16 MiB), written last, after the run report, a
  resumed and an overwritten run's included. A run that cannot write it (in a bucket, within a time bound of
  its own) warns and stays committed.
- `swath list -o s3://`, `gs://` or `az://` prefix with `--work-dir DIR` writes the dataset to a bucket; the
  destination takes its own credentials, region and endpoint (`--output-*`).

### Runs: outcome, resume and refresh

- `complete` means nothing is missing, everywhere: the outcome, the report's `outcome.complete`, the
  manifest, `swath show` and `swath du --from-dataset`. The outcome class of a run that did what it was asked
  is `ok` (a `| head` run is `ok` with `complete` false); `stop_reason` is `end_of_scope`, `max_keys` or
  `downstream_closed`, absent for a run that wrote no output; a partial dataset (exit 3) gets no `error.json`.
- A full disk or a full quota (EDQUOT) exits 74 (`disk_full`, cause `no_space`), and spill waits on either.
  An exit-1 run's `resumable` says what `swath resume` does: true for a journaled run stopped by `auth`,
  `access_denied` or a disk error, once the cause is fixed; false for `not_found`, `region_redirect` and
  another process's output (#517, #518).
- SIGHUP (a closed terminal or SSH session) stops a run as SIGTERM does: its outcome is recorded (class
  `terminated`, `stop_source` `sighup`) and the process dies by the signal (129); under `nohup` it stays
  ignored. Exit 2 with no outcome is a crash (a panic, SIGQUIT), resumed as a kill. A stuck run without an
  output directory writes its goroutine dump to a file in the temporary directory and names it.
- A dataset run that no request ever answered (exit 75, cause `unreachable`) and that published no part removes
  its journal and work files, so the corrected command runs on the same `-o`.
- `--overwrite` keeps the previous dataset readable until the new one commits: the new run lists into
  `.swath/next/` and its commit installs the new dataset in place of the old one, whose parts are removed
  after. A run that fails or is stopped leaves the previous dataset whole, and a stopped object-store run waits
  there for `swath resume DIR`. The report's `output.replaced` says whether the commit replaced a dataset. A
  directory an interrupted 0.3 run left (`.swath-state.json` or `.swath/checkpoint.sqlite`) is replaced by
  `--overwrite`, and `swath resume` refuses it as written by 0.3.
- `swath resume DIR` writes a report only when given `--report`.
- Sorted output needs no merge pass: rows listed ahead of the output wait in memory (`--sort-buffer`, by
  default 1/16 of the least of physical memory, the cgroup limit and `GOMEMLIMIT`, at most 1 GiB) and then in
  spill. Without `--spill-budget` spill may use the spill filesystem's free space after the reserve: all of it
  for a dataset in a bucket, half of it otherwise; a frame the cap would refuse rereads the free space, and so
  does the run every 2 s while the cap holds it back, so a disk grown under a live run is used without a
  restart. `--spill-budget` is a fixed cap. `diagnostics.resources.spill` says what set the cap (`max_by`),
  the cap at the start and the end, and how often it rose.
- A sorted run whose spill cap is far too small says so: once spill has stood refused with workers waiting on
  it for 2 minutes, the error event `spill_pressure` names the wait, spill and its cap, the output's rate and
  the remedy (with the default cap, grow the spill disk or free space on it; with `--spill-budget`, stop and
  resume with more), and repeats every 30 minutes (`order.spill_pressure_since` in `progress.json`).
  `--spill-wait-timeout DURATION` (default 0: wait) stops such a run instead: exit 74, cause `spill_budget`,
  `stop_source` `spill_wait`, resumable with more disk or a larger cap (#535).
- `--seed-from DATASET` (or a cut file) divides a relisting along the previous dataset's keys, from its
  `cuts.txt` when it matches the manifest, else its parts' page index; a bucket dataset's `cuts.txt` can be
  copied and passed as a cut file. A counted prior is listed front to back through a frontier window (8 million
  rows, fewer when `--spill-budget` and `--sort-buffer` hold less), so rows listed ahead of the output stay
  near the window and such a relist needs a fraction of a cold run's spill; with durable spill the window
  evicts drained certificates while live spill is past half the cap (#535). The report's `invocation.prior`
  names the prior (`cuts_file`, `cuts_refused`), `diagnostics.resources.window` the window, and the range
  engine counts `seed.from.merged` and `seed.from.plan_errors`; `swath diag list --seed-from-tile-pages` sets a
  tile's pages (32) and `--seed-from-window-rows` the window's rows.
- A resume replays durable-spill certificates through the parallel tail: on 28 production key lists a resume
  took 0.67 to 0.82 times a fresh run's wall (10 of those lists), and its parts are the uninterrupted run's
  (#288). The certificate limits scale with the spill cap, one slot per 64 KiB, up to 262,144 certificates
  and 512 MiB of their metadata, with a ledger history of four records per slot, at least 40,000; a resume
  counts the certified files it kept as free space (#513). `progress.json` counts the bytes a resume's
  certificate scans read (`scanned_bytes`), so a long scan is not stopped as stuck.
- The report says where a sorted run's workers waited and for how long (publish, admission, idle, refused
  spill; live in `progress.json`), and the range engine's `awaited.*` counters why the range the output waited
  for was or was not split (#535).

### Run report and logs

- The run report (`run.json` beside a dataset, `--report FILE`, `--report -`) is `report_version` 1: a stable
  core (`build`, `attempt`, `run`, `invocation`, `outcome`, `timings`, `process`, `output`, `args_hash`,
  `requests`, `cost`) and one `diagnostics` member, whose names follow the algorithm and may change in any
  release. A name says its kind of extreme: `peak_` a high-water mark over time, `max_` a limit or the largest
  value over rows or items. `build.version` and `--version` print `0.4.0`, never `v0.4.0`.
- A run has an id, `run.id`: the same for a run and every resume of it, also in `error.json`, `progress.json`
  and the log events' `run_id`.
- Every time in a JSON file or event (report, `run.json`, `error.json`, `progress.json`, the manifest, the
  journal, the log's `ts`, `coverage.filters`, `swath du`'s `max_last_modified` and provenance, the trace) is
  RFC 3339 in UTC with exactly six fraction digits: `2026-11-15T12:00:00.137198Z`.
- `progress.json` (`progress_version` 1) has `attempt_id`, `run_id` and `state` (`listing`, `draining`,
  `publishing`, `done`, `failed`), is written a last time with the final counts, and names its counts as the
  report does (`rows_fetched`, `rows_emitted`, `object_bytes`, `parts_written`, `http_attempts`, ...).
  `output.object_bytes` and `committed_object_bytes` are absent when an object's size is unknown.
- `--log-format json` writes stderr as events: the envelope has `attempt_id` and `run_id`; every `warning` has a
  `code`; `cause` is always a label (a retry's failure class, `http_STATUS`, `timeout`, `connection` or
  `unreachable`) and the free text is `error`; durations are integers (`latency_us`); `start.destination` is
  `-` for stdout and `start.source` is spelled as the manifest's `source.uri`. The `outcome` event carries
  `status`, `complete`, `stop_reason` and `unreadable`.

### Libraries

- The nested modules `github.com/varveio/swath/reader` and `github.com/varveio/swath/objfile` are tagged with
  swath's version (`reader/v0.4.0`, `objfile/v0.4.0`) at the release commit, so
  `go get github.com/varveio/swath/reader@v0.4.0` works; both are at version 0, with no API promise.
- `reader` opens and reads; it does not judge a dataset. `Open` and `OpenDir` take
  `seekparquet.OpenOptions` and expose the manifest's source, scope, coverage and `collected` through
  `Metadata()`; part digests are not checked on read, and `IsRefusal` tells the data's refusals from other
  errors. It keeps the schema registry (with version-column roles as `VersionSpec`), `CheckSchema` and
  concrete typed decoders (`DecodeS3Object`, `DecodeAzureObject`, `DecodeLocalEntry`, which keeps every
  filesystem column and its nulls, and the rest); `CheckSchema` prints an unannotated column's logical type as
  `NONE`. A local `CountRows` and `Sum(size)` include directories and their own `st_size`, and sums skip null
  sizes: request `CountNulls` and `CountAbsent` to detect unknown bytes under `--metadata names`;
  scanning, aggregation and browse pages go through SeekParquet (`Dataset.SeekParquet()`: `Plan.ScanParallel`,
  `Dataset.Aggregate`, `Dataset.Page`, `Dataset.Scan`), which owns read statistics, limits and their errors
  ([reader guide](reader/README.md)).
- `objfile` pins objects across opens and restarts (`Pin`, `Restore`, `Pinned.Snapshot`), parses dataset roots
  (`ParseLocation`), and checks every ranged read against the offset served. `objfile/remote.OpenWith(ctx,
  location, remote.Options{...})` opens a location with a GCS credentials file, an endpoint, an S3 region and
  profile, an Azure SAS and a request timeout (default `DefaultRequestTimeout`, 2 minutes), so an Azure
  container on Azurite or the replay server can be read (#384); `remote.Open` is `OpenWith` with the zero
  `Options`, and `OpenDestination` takes the same struct, its write fields (`GCSWrite`, `S3Write`,
  `AzureWrite`) the destination's only. GCS Application Default Credentials keep the auth library's own
  clients, so an X.509 workload identity configuration gets its mTLS client.

### Release

- One archive per target (`linux-amd64`, `linux-arm64`, `darwin-amd64`, `darwin-arm64`, `windows-amd64`)
  holding static `swath` and `swath-replay` binaries with `LICENSE`, `NOTICE` and `THIRD_PARTY_NOTICES`, an
  SPDX SBOM, `SHA256SUMS`, keyless cosign signatures and build provenance; multi-arch images
  `ghcr.io/varveio/swath` (distroless static) and `ghcr.io/varveio/swath-replay` (`scratch`), running as
  `65532:65532`. `THIRD_PARTY_NOTICES` is generated by `notices.sh`, which fails the build on a linked module
  whose license is not recognized or not allowed.
- Built with Go 1.27.4. azblob 1.8 links `apache/arrow-go` into the binary (about +4.8 MB).
- `just release` creates the three module tags, and the release workflow refuses a release whose module tags
  are missing or name another commit.
- Documentation at [swath.varve.io](https://swath.varve.io/): the user guide by the question asked, one
  generated reference page per command, and a field guide at `/field-guide/` on how the engine divides,
  orders and resumes a listing, drawn from the home page's run, whose report is at `/runs/noaa-gestofs-pds/`.

## 0.3.2 — 2026-09-03

### Fixed

- The listing pipeline's shared channel no longer wakes every parked fetch worker for each page the
  output stage drains. With 2,048 workers behind a 50,000-entry budget that broadcast cost the single
  consumer more time per page than the sink write itself and capped billion-object TSV, JSONL,
  Parquet and `--sort` runs at roughly 2,600 pages/s regardless of writer count; the channel now
  relays one wakeup per released page and the admitted sender relays onward while budget remains
  (#206). Backpressure semantics (`--object-listing-queue-size`) are unchanged.
- Per-page row tallies (objects, common prefixes, delete markers, object bytes) are computed on the
  fetch worker that built the page and merged by the output stage in constant time, instead of being
  walked entry by entry on the single consumer thread.

### Added

- `client_cost[]` gains a `channel_receive` span (`swath.channel.receive.latency`): the consumer
  stage's own wait to take each envelope off the shared channel, the complement of `emit` on the
  consumer's timeline.

## 0.3.1 — 2026-09-01

### Changed

- The `swath-replay` sorted `delimiter=/` skip-scan reopens its key cursor through the Parquet
  page index when a common-prefix successor provably lies beyond the current data page, instead of
  decoding every key in the skipped subtree; a successor that can still occur on the current page
  keeps the forward cursor. Responses are byte-identical; wide directory probes over skewed
  fixtures drop from hundreds of milliseconds to tens.
- Replay meters gained `swath.replay.delimiter.skipscan.decoded_key_rows`, `.page_reseeks`, and
  `swath.replay.delimiter.reader_pool.readers_opened`; `.skipscan.row_group_opens` now counts
  cursor opens as operations, since a reseek can reopen the same physical row group.
- Sorted-serving documentation now states the order-checking scope explicitly: request-time
  cursors verify the rows they decode and refuse on disorder, while page-index and routing-index
  shortcuts deliberately leave other rows unread. The behavior is unchanged.
- The `swath` CLI is unchanged in this release.

## 0.3.0 — 2026-09-01

### Changed

- Parquet output now exposes `key` as a STRING logical column while retaining its byte-identical
  BINARY storage. Downstream DuckDB, Spark, pandas, and similar queries therefore see the column
  type change from BLOB to VARCHAR and may need to update blob comparisons, casts, or functions.
- Parquet output now rejects malformed UTF-8 key bytes with a typed output error. Legacy captures
  containing non-UTF-8 keys remain readable, but they can no longer be re-published as Parquet or
  sorted into a new dataset by `swath-replay sort-fixture`. A `swath list` run, with or without
  `--sort`, cannot produce such a key.
- Sorted output finalizes through a single reference-routed pipeline: page-run staging, catalog
  validation with heap and file-descriptor admission, a bounded cascade, header-only segment scans,
  one router assigning complete part plans with dense ordinals, and admitted parallel encoders. Part
  geometry is reproducible across encoder counts, and a budget too small for the work refuses
  resumably before the pass it cannot run opens its channels or writers. The range-parallel
  finalization layer is gone.
- Sorted-merge parallelism moved from `-Dswath.sort.merge-parallelism` to
  `--tune sort.merge-parallelism` and now caps final encoders rather than splitting the keyspace
  into contiguous ranges. Part count follows the data, `final-file-bytes`, and the heap-admitted
  reference cap rather than parallelism. The `swath.sort.min-parallel-staged-bytes` floor that kept
  small sorted runs serial and the `swath.sort.segment-row-group-bytes` property are removed with
  their mechanisms; `swath.sort.final-row-group-bytes` now also prices encoder heap admission; and
  the new `swath.sort.final-page-rows` sets the final file's page granularity.
- Sorted finalization is separated from dataset publication, with a distinct publisher and
  committer. Until the committer returns, the previously published dataset and all
  checkpoint-owned staging are untouched, so an interruption, a crash, or a capacity refusal
  before publication is resumable and only disposable staging is swept. A deterministic
  finalization failure remains fatal.
- The page-run staging container is version 4 with no legacy read path. A sorted run interrupted
  under 0.2.4 cannot be resumed by 0.3.0 and must be restarted. Published datasets are unaffected.
- Sorted parts are numbered from zero (`part-00000.parquet`). The footer's `swath.sort.file_index`
  stays one-based. Final sorted files now pin their physical layout — 1,024-row data pages, an
  8 KiB dictionary page, and 1,024-byte column-index truncation — instead of taking the Parquet
  library's defaults.
- `ZSTD1` is the default staging segment codec for sorted output, holding about 29% less staging
  disk than `LZ4` on the measured corpus.
- The `_swath_summary.json` `sort` block gained the finalization pipeline's evidence, dropped
  `merge_boundaries_ms`, and pins `buffer_sort_fallbacks` to `0`. The
  `swath.sort.merge.range.latency` and `swath.sort.merge.boundaries.latency` meters are replaced
  by the `swath.sort.pipeline.*` family. `schema_version` remains `2`; every other report change
  this release is additive.
- The supported CLI surface is documented in `docs/cli.md` and enforced by a coverage test.
  `--engine-toggle` and `dump-run` are hidden, and `--tune help` declares each key as stable,
  experimental, or diagnostic. The stabilization itself renamed and hid nothing that was
  supported and changed no option's behavior.
- `--tune parquet.writers` accepts 2 through 64; counts above 4 are admitted against available
  heap.
- Dataset manifests are published only at completion, publication ownership is centralized, and
  part digests are computed while writing.
- A fresh run whose seed request cannot reach the endpoint fails immediately with
  `seed_endpoint_unreachable` instead of spending the transient retry budget. Directory fsync
  degrades to a no-op only for filesystems on an explicit allowlist; other failures are fatal.
- The shipped dependency closure is pinned through version constraints and no longer contains
  netty. A build check asserts that each application distribution ships at most one version of
  each module, and the container images move to digest-pinned Temurin 25 bases.

### Added

- `--compression` applies gzip or Zstandard to table, TSV, and JSONL output, to a stream or a
  file. TSV and JSONL can additionally publish bounded multi-writer directory datasets with
  manifests and `_SUCCESS` markers.
- `--format discard` runs the listing pipeline without materializing rows, for separating
  object-store cost from output cost.
- `--writeback-size` periodically forces already-emitted bytes for open dataset parts and sorted
  Parquet final files. It is a performance control and does not change the durability boundary.
- `--tune sort.keep-staging=on` retains checkpoint-tracked staging segments after a successful
  sorted publish, for diagnostic merge replay.
- Direct Parquet and partitioned text output share a resource-admitted writer pool, and run
  reports expose writer-pool saturation, per-lane work, finalization, and publication evidence.
- `swath-replay` is published as a named toolkit with release packaging and runtime attestation.
- `docs/cli.md` documents the supported CLI surface.

### Fixed

- Replay honors S3 continuation-token precedence over `start-after` and renders timestamps
  correctly outside UTC.
- An oversized sorted-output overlap component spills its reference coordinates to staging instead
  of exhausting merge memory.
- Writer shutdown admission no longer stalls, and dataset publication cleans up correctly on
  failure and cancellation paths.
- `--bearer-token-command` output, stream close, and process teardown are bounded.

### Performance

- S3 responses are streamed, canonical timestamps parse through a faster path, redundant checksum
  conversion was removed, and partitioned TSV writes UTF-8 bytes directly.
- Sorted finalization matched or beat the range merge at every parallelism measured, and handles a
  heavily overlapping keyspace the range merge could not partition.
- Replay serves sorted fixtures and cold pages at a cost proportional to the answer.
