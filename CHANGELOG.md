# Changelog

Notable changes per release. The full human summary for the current release is in
[release notes](docs/ops/dev/RELEASE_NOTES.md).

## Unreleased

- Docs: the user guide is split by the question a reader asks. `running.md` keeps one run's command, outcome and
  limits and links to Size a run, Watch and tune a run, Stop, resume and refresh, and List every version; known
  limits moved to Troubleshooting. Query an inventory (DuckDB, pandas, Spark, verifying) is apart from the dataset
  format; `production.md` covers scheduled runs and bucket destinations only; `swath du`'s and `swath resume`'s
  explanations moved from their command pages to Total a source and Stop, resume and refresh. Old anchors still
  resolve.
- Site: the field guide has its pictures back, drawn for the Go engine: an animated miniature model of a
  listing (seed, owner-side splits, the sorted stream, commit before emit; still frames with reduced motion), a
  diagram per section, and two figures from the home page's run (its 257 starting ranges, and requests in flight
  over time on the range that held 64.5% of the bucket). Its claims are corrected against the code.
- Go port: with the default spill cap, the `spill_pressure` event's remedy is to grow the spill disk, or free space
  on it, as listing then continues; with a `--spill-budget` it still says to stop and resume with more.
- Go port: the diagnostic `swath diag list --segment-workers` is removed; the tail's worker count keeps its
  rule (one per two processors, at least 8, within the encoders, descriptors and row budget).
- Go port: `--include '*\.parquet'` (a quoted dot, a valid glob) is accepted; it was refused as a regular
  expression. `.*\.parquet` is still refused, naming `--include-regex`.
- Go port: `swath du --from-dataset DIR /tree/file`, a file in the dataset's tree, totals that file as a live
  `swath du /tree/file` does; it sought `file/` and printed zeros. A symbolic link there is refused, naming it.
- Go port: a resumed `swath list ... -o DIR --overwrite` writes `DIR/cuts.txt` again; the parts it kept from
  the interrupted attempt were read where they lay before the installation, so the cut file was skipped with a
  warning.
- Site: a field guide at `/field-guide/` on how the Go engine divides, orders and resumes a listing, on the home
  page's run, and that run's report at `/runs/noaa-gestofs-pds/`, linked from the replay. The explainer's report
  names its run in its title and description and has a `main` landmark and a skip link.
- Go port: a run's closing lines state the result: `complete: 60,010 objects, 5.55 TiB in 4.2s · 14,391 keys/s
  → ds1 (1 part, 279 KiB)`, then `next: swath show ds1 · swath du --from-dataset ds1` (or `swath resume DIR`).
  No rate under 2 s, the rate over the time printed; filtered runs say `3 objects kept, 1 filtered out`, an
  interrupted one `7,003 saved, 12,003 listed`, a resume the dataset's total with the rows from earlier attempts,
  `swath du` its total row's counts. API calls, in flight, throttles, retries, the estimate and peak RSS print
  only with `--stats`, and only those that happened.
- Go port: the progress line reads objects, bytes so far, elapsed, rate, then parts and (for an object store)
  in flight and API calls; while every request fails it shows `retrying: CAUSE` instead of `0 keys/s`.
  `progress.json` and the JSON `progress` event carry the bytes so far as `object_bytes`, the report's
  `output.object_bytes` while the run lists. The report's `output.object_bytes` is absent when an object's
  size is unknown (`--metadata names`), as `committed_object_bytes` is; it was 0.
- Go port: `swath list -o FILE --overwrite` (or `-o -`) is accepted: a text file is always replaced, which
  `--overwrite` asks for anyway (it stopped with `invalid configuration: replace needs an output directory`).
- Go port: `swath show` prints the verdict first, then labelled lines in reading order (source, listed in local
  time, contents with the object bytes and grouped counts, coverage with the filters' rules, storage last);
  `swath show MISSING` says `no such file or directory`.
- Go port: `swath list`'s table puts the size first, as `swath du`'s does, heads a local tree's keys `path`,
  sizes `kind` to the usual kinds, and ends a symlink's row with ` -> TARGET`.
- Go port: `swath du`'s table names its total row by SOURCE as typed (`s3://bkt/logs/2026/0 (total)`), marks a
  directory it could not read `-`, and TSV/JSONL give the total the key prefix SOURCE names (`logs/2026/0`).
  `swath du DIR` on a dataset directory prints a hint naming `--from-dataset`.
- Go port: a GCS error answer without an error body names its HTTP status text and path; `-v`'s start line
  shows the attempt id's first 8 characters; `swath du --from-dataset`'s label gives the listing time in local
  time.
- Releases tag the nested Go modules with swath's version: `reader/vX.Y.Z` and `objfile/vX.Y.Z` beside `vX.Y.Z`,
  all at one commit, so `go get github.com/varveio/swath/reader@v0.4.0` works. Both modules are at version 0, with
  no API promise. `just release` creates the three tags; the release workflow refuses a release whose module tags
  are missing or name another commit.
- Go port: the run report has a stable core and one `diagnostics` member. `report_version` 1 covers
  `build`, `attempt`, `run`, `invocation`, `outcome`, `timings`, `process`, `output`, `args_hash`, `requests` and `cost`;
  everything that describes how the pipeline ran (`resources`, `source` with the provider's counters,
  `efficiency`, `history`, `profiles`, `sort_buffer`, `seed_ms`, a text run's sink) moved under `diagnostics`,
  whose names follow the algorithm and may change in any release. `resources.peak_rss_bytes` is
  `process.peak_rss_bytes`.
- Go port: a run has an id: the report's `run.id`, the same for a run and every resume of it (the journal's
  run UUID; a run without a journal has its attempt's id), also in `error.json`, `progress.json` and the log
  events' `run_id`.
- Go port: every time in a JSON file or event (report, `run.json`, `error.json`, `progress.json`, the manifest,
  the journal, the log's `ts`, the times in `coverage.filters`, `swath du`'s
  `totals.max_last_modified` and `--from-dataset` provenance, the trace's `run_start.time`) is RFC 3339 in UTC with
  exactly six fraction digits: `2026-11-15T12:00:00.137198Z`, never `…28.1371984Z` or `…28Z`.
- Go port: report and log names, one vocabulary: `attempt.started` is `attempt.started_at`;
  `timings.listing_done_at` is `timings.provider_finished_at`; `invocation.fields` is `invocation.metadata`;
  `peak_` is a high-water mark over time, `max_` a limit or the largest value over rows or items
  (`max_buffered_bytes` is `peak_buffered_bytes`, `frontier_stuck_max_ns` is `max_frontier_stuck_ns`, segments'
  `max_held_bytes` is `peak_held_bytes`, the counters `max_active`, `max_queued`, `inflight.max_active`,
  `limiter.max_depth` and Azure's `order_*_max` are `peak_active`, `peak_queued`, `inflight.peak_active`,
  `limiter.peak_depth` and `order_peak_*`; the spill cap's `max_raised`, `max_rechecks` and
  `max_recheck_raised` are `cap_raises`, `cap_rechecks` and `cap_recheck_raises`); `build.version` and `--version` print `0.4.0`, never
  `v0.4.0`.
- Go port: `progress.json` has `progress_version`, `attempt_id`, `run_id` and `state` (`listing`, `draining`,
  `publishing`, `done`, `failed`), is written a last time with the final counts when the run ends, and names its
  counts as the report does: `rows_fetched`, `rows_emitted`, `rows_encoded`, `parts_written`, `http_attempts`,
  `rate_per_1k_usd`, `order.spill_bytes`; `mutex_wait_ms` and `mutex_max_hold_ms` (floats) are integer
  `mutex_wait_ns` and `max_mutex_hold_ns`.
- Go port: `swath show --journal` is a diagnostic: it prints the journal's private format, which may change in
  any release.
- Go port: `--log-format json`: the envelope's `attempt` is `attempt_id`, with `run_id` beside it; every
  `warning` has a `code`; `cause` is always a label (a retry's failure class, a request's `http_STATUS`,
  `timeout`, `connection` or `unreachable`) and the free text is `error`; `retry.class` is gone (it is
  `cause`); a request's `latency_ms` (a float) is integer `latency_us`; a persistent failure's `attempts` is
  `http_attempts`; `start.destination` is `-` for stdout and `start.source` is spelled as the manifest's
  `source.uri` (a local path absolute).
- Go port: `--overwrite` of a committed dataset keeps it readable until the new one commits. The new run
  lists into the output's workspace (`.swath/next/`) and its commit installs the new dataset in place of the
  old one, whose parts are removed after; a run that fails or is stopped leaves the previous dataset whole, and
  a stopped object-store run waits there for `swath resume DIR` (`swath show DIR` names it). Before,
  `--overwrite` removed the previous dataset before listing anything, so a typo in the source or endpoint lost
  the last good inventory. The run report's `output.replaced` is true when the commit replaced a previous
  dataset.
- Go port: a dataset run that no request ever answered (exit 75, cause `unreachable`: a mistyped endpoint, a
  service not up) and that published no part removes its journal and work files, so the corrected command runs
  on the same `-o`; it left an interrupted run that refused the corrected command, while a resume refused another
  endpoint. A resume of a run with parts keeps its journal.
- Go port: SIGHUP (a closed terminal or SSH session) stops a run as SIGTERM does: its outcome is recorded (class
  `terminated`, `stop_source` `sighup`), and the process dies by the signal (129); it killed the run with no
  outcome, report or `error.json`. Under `nohup` it stays ignored. The exit codes page says that exit 2 with no
  outcome is a crash (a panic, SIGQUIT), resumed as a kill.
- Go port: `--report` naming a device, a descriptor or a named pipe (`/dev/stderr`, `/dev/fd/3`, a FIFO) is
  written there; it failed after the run (exit 1), and a FIFO was replaced by a regular file.
- Go port: `swath resume DIR` writes a report only when given `--report`; it no longer replays the run's
  recorded `--report`, which replaced the earlier attempt's report with its own.
- Go port: a full quota (EDQUOT) exits 74 (`disk_full`, cause `no_space`) as a full disk does, and spill
  waits on it as on a full disk; it was exit 1 and ended the run (#518). An exit-1 run's `resumable` says
  what `swath resume` does: true for a journaled run stopped by `auth`, `access_denied` or a disk error, once
  the cause is fixed (it said false while resume continued it); false for `not_found`, `region_redirect` and
  another process's output (#517).
- Go port: `complete` means nothing is missing: the listing reached the end of its scope and read every entry.
  The manifest's `coverage.complete`, the report's `outcome.complete`, the `outcome` log event, `swath show` and
  `swath du --from-dataset` say false for a dataset with unreadable entries (before: true, beside
  `unreadable` 1); filters do not make a dataset partial. The outcome class `completed` is `ok` (a `| head` run
  is `ok` with `complete` false). `stop_reason` is a coverage reason only (`end_of_scope`, renamed from
  `completed`; `max_keys`; `downstream_closed`), absent for a run that wrote no output (before: its class), and `outcome.unreadable` is absent before the source reported. A partial dataset
  (exit 3) gets no `error.json`: its manifest's coverage says what is missing. The `outcome` event adds
  `status`, `complete`, `stop_reason` and `unreadable` and drops `message` (it repeated `msg`). A person reads
  one verdict everywhere: `complete`, `partial: 1 unreadable entry`, `partial: --max-keys reached`, `stopped:
  --max-duration reached`, `interrupted`, in the closing block (before: `INCOMPLETE (class)`), `swath show`'s
  coverage line (before: `complete=true stop=completed unreadable=1 ...`) and `swath du --from-dataset`'s
  dataset line; the warnings that explain it now print before the closing block.
- Go port: `swath du -h DIR` and `swath list SOURCE -h` are refused (exit 2) instead of printing help with exit 0:
  `-h is help; sizes are already human-readable in the table`. `-h` alone, and `--help` anywhere, still print
  help; `swath` alone prints its short help.
- Go port: an `-o` name with a `.tsv` or `.jsonl` component must end there or in `.gz` or `.zst`, in any case;
  `rows.jsonl.zstd` (did you mean `.zst`), `rows.tsv.lz4` and the like are refused instead of becoming dataset
  directories. An existing file or a device (`-o /dev/null`) as a dataset directory is refused with exit 2
  and what to use instead, not exit 1. `--part-size`, `--part-interval` and `--part-max-rows` are refused with
  text rows.
- Go port: `--part-rotation-max-rows` is `--part-max-rows`, beside `--part-size` and `--part-interval`; the old
  spelling is suggested.
- Go port: `--include-regex` and `--exclude-regex` match the key below SOURCE on every source, as the globs
  do. Before, on an object store they matched the whole key, SOURCE's prefix included, while a local tree's
  key is below its root.
- Go port: `--modified-since` and `--modified-until` read a time without an offset as local time (`TZ`), as the
  tables print it and `find -newermt` reads it (it was UTC), and also take the tables' form
  `2026-11-15 07:45[:00]`; the run records the UTC time it names, as for a duration. `Z` or an offset is
  honoured as given.
- Go port: `swath du` takes `--max-depth N` (GNU du's spelling of `--depth N`) and `-x` (also on `swath list`)
  for `--one-file-system`; GNU du's `-s` hint appears on `swath du` only. The 0.3
  spellings (`--force-path-style`, `--idle-timeout`, `--force`, `--parquet-part-size`, `--compression`, ...)
  name what replaced them, and `--format json` suggests `jsonl`, `--format parquet` a dataset (`-o DIR`).
- Go port: `s3:/bkt` (one slash) is refused naming `s3://bkt` rather than taken as a local path; an unsupported
  scheme names the forms SOURCE takes, and an S3 or GCS https URL (`https://bkt.s3.amazonaws.com/p`,
  `https://storage.googleapis.com/bkt/p`), `s3a://`, `gcs://` or `file://` the source meant.
- Go port: a live listing of an object-store prefix with no key warns `no key starts with "PREFIX"` (exit 0).
- Go port: every value error reads `--flag "VALUE": want ...` (sizes name their units), and every usage error
  of `swath list` and `swath du` ends with the help to read.
- Go port: `swath du` on a terminal shows the live progress line while it lists and erases it before the table;
  `swath list --progress` with rows on the same terminal is refused.
- Go port: the `SWATH_TEST_*` hooks exist only in a `-tags swathtest` build; a stop by SIGTERM says
  `stopped by SIGTERM`.
- Go port: one name per concept in every dataset schema. Times: `created_at` (GCS `time_created`, Azure
  `creation_time`, local `birth_time`), `deleted_at` (GCS `time_deleted`, Azure `deleted_time`), `accessed_at`
  (Azure `last_access_time`, local `atime`); MD5: `md5` (Azure `content_md5`); Azure's access tier is
  `storage_class`, with `storage_class_inferred` and `storage_class_changed_at`. `swath/gcs-generations/v1`
  gains a required `is_latest`. A local `kind` is lower case (`file`, `dir`, `symlink`, ...). Every part has
  every column of its schema id: no column may be absent.
- Go port: one provider token per provider on every machine surface: `s3`, `gcs`, `azure`, `local` (manifest
  `source.kind`, report `source.name`, the journal's provider); the Azure schema ids are
  `swath/azure-objects/v1` and `swath/azure-versions/v1`, and the reader's types `AzureObject` and `AzureVersion`.
- Go port: the manifest's `coverage.filters` is a list of `{"filter": NAME, "value": VALUE}` named for the
  options (`modified_since`, `one_file_system`, ...; `null` for a switch), its times with six fraction digits;
  `coverage.snapshot` is gone. A new `collected` records the metadata tier and the optional columns the run
  asked for, and `committed_object_bytes` is absent when an object's size was not read (`--metadata names`)
  instead of 0. `swath du --from-dataset`'s `dataset` member and event carry the filters in the same objects.
- Go port: a JSONL row of a local tree always has `key` and `key_base64` (and `link_target`,
  `link_target_base64`): the text, null when not UTF-8, and the base64 bytes, null when UTF-8.
- Go port: a run stopped while its parts were still publishing no longer lets a later part add its page ends to
  the cut file's cuts beside an earlier part still adding its own (a data race, seen under the race detector).
- Go port: `swath du`'s rows carry both byte figures under the dataset's names, `size` (the lengths) and
  `allocated_bytes` (a local file's blocks), whatever `--allocated` says; `allocated_size` is gone. `unsized`
  is replaced by `size_missing` and `allocated_bytes_missing`: a figure no counted object had is null, one
  that some had is the sum of theirs beside the count of the others, and an empty prefix's is 0. A bucket's
  `allocated_bytes` is null. The newest time under a prefix is `max_last_modified`; `list` rows and the dataset
  keep `last_modified`. Every row adds `row_type` (`total` or `prefix`), `primary`, `sort` and `links_once`.
  `--allocated` only puts the allocated bytes first and makes `--sort size` order by them; `--apparent-size`
  selects the default, and the two together are refused. The run report's `output.totals` takes the same
  names (`size`, `allocated_bytes`, the missing counts, `max_last_modified`, `primary`) in place of `bytes`,
  `bytes_column` and `unsized`. `links_once` says whether the listing reads links (a local tree at stat or
  statx, without `-l`), and the manifest records a local listing's metadata tier (`collected.metadata`), so a
  dataset listed with `--metadata names` totals as the live run does, an empty one included. A name of a
  hard-linked file counted under another name adds no time to `max_last_modified`.
- Go port: the `swath du` table shows one figure, `size`, or `on disk` with `--allocated`, the counts with
  grouped digits, each prefix's newest time (`max_last_modified`) in local time to the second under a heading
  naming the zone (`last_modified (CET)`), and each path indented two blanks a level; an unknown figure is `-`, a partial one
  `>= ` its lower bound. For a local tree a line on stderr under the table gives the totals of both figures
  (`100 MiB in 6 files · 36.4 MiB on disk`); `-q` leaves it out. The `swath list` table heads its local times
  with the zone's abbreviation (`last_modified (CET)`) in place of `(local)`, and prints `-` for a missing value.
- Go port: `swath du SOURCE` on a terminal, without `-d`, `--sort` or `--format`, draws the largest prefixes and
  objects at whatever depth they are: a tree, largest first, each with its share of the total and, where the
  width allows, a bar and the newest modification time in local time (the bar is left out first, then the
  time; a long name is cut in the middle), color following `--color` and `NO_COLOR`. Each parent's other
  children fold into one `(N more)` line carrying their sum, so every level adds up. Every figure is exact;
  memory stays small because a complete prefix (inside one range, or a local directory whose listing ended)
  is kept only at 1 % of the bytes seen so far and among the 100 largest of its depth. A pipe, `-d N`,
  `--sort` and an explicit `--format` print the rows as before. The run report's `diagnostics.totals_view` counts
  what the view kept, dropped and merged across ranges. The idea of the tree with shares is `dust`'s and
  `pdu`'s (both Apache-2.0), implemented from their documented behavior.
- Go port: `--no-header` leaves out the TSV header line of `swath list` and `swath du`.
- Go port: a local `--metadata statx` listing on a filesystem that does not return a field (blocks on some
  network filesystems) records it as null, not 0.
- Go port: SeekParquet is pinned at its release `go/v0.2.0`, the first that writes format 1, draft revision 1
  (`format_version` 0, `draft_revision` 1). That release fixes the draft: the datasets swath writes are no
  longer subject to in-place changes of the format. No code change from the commit pinned before.
- Go port: S3 credentials that expire (an assumed role, web identity, SSO, `credential_process`, container
  credentials, an instance role) are refreshed ahead of their expiry, by the smaller of 5 minutes and a quarter
  of their lifetime, for the source and for a bucket destination. Before, they were used until the instant
  they expired, so a request signed just before could be refused with `ExpiredToken`, and a listing longer
  than its credentials' session stopped with cause `auth`. An expired-token answer to such credentials now
  renews them, once for all the requests refused for the same credentials, and the source's request is retried
  once; keys that do not refresh still stop the run with cause `auth`, saying so. The run report counts
  `credentials.refreshes` and `credentials.expired_refreshes` (#523).
- Go port: the default spill cap follows the disk. Without `--spill-budget` spill may use the spill filesystem's
  free space after the reserve: all of it for a dataset in a bucket, whose parts are uploaded from memory, and
  half of it otherwise (the parts, the text file, or for a stream other programs' temporary files share the
  filesystem); a frame the cap would refuse rereads the free space, and so does the run every 2 s while the
  cap holds it back, so a disk grown under a live run (a cloud disk resized and its filesystem grown online) is used without a
  restart. `--spill-budget` is a fixed cap. The run report adds `resources.spill.max_by`, `start_max_bytes`,
  `max_raised`, `max_rechecks` and `max_recheck_raised`; `max_bytes` is the cap in effect at the end.
- Go port: `objfile/remote` opens a location with explicit settings: `remote.OpenWith(ctx, location,
  remote.Options{...})` takes a GCS credentials file, an endpoint (an emulator's for GCS), an S3 region and
  profile, an Azure SAS and a request timeout; `remote.Open` is `OpenWith` with the zero `Options`. An Azure
  container on a custom endpoint, Azurite's or the replay server's, can now be read (#384). `Options` replaces
  `DestinationOptions`, so `OpenDestination` takes the same struct; its write fields (`GCSWrite`, `S3Write`,
  `AzureWrite`) are the destination's only. A store `Open` builds now waits at most `DefaultRequestTimeout`
  (2 minutes) for a response's headers, and Azure's token requests are bounded by it; `RequestTimeout` changes
  it, and zero or negative is the default (there is no unlimited value). GCS Application Default Credentials,
  for reads and now for destinations too, keep the auth library's own clients, so an X.509 workload identity
  configuration gets its mTLS client; only a `CredentialsFile`'s token requests are bounded by `RequestTimeout`.
- Go port: `--include` and `--exclude` refuse, with exit 2 before any listing, a glob that carries a sign of a
  regular expression (a leading `^`, a final `$`, `\.`, `\d`, `(a|b)`), naming `--include-regex` or
  `--exclude-regex`; a 0.3 command line no longer lists nothing and exits 0 (#526). A directory an interrupted
  0.3 run left (`.swath-state.json` or `.swath/checkpoint.sqlite`) is replaced by
  `--overwrite`, and `swath resume` refuses it as written by 0.3.
- Go port: a seeded run with many workers and a small spill cap no longer crawls on spill the output has passed.
  The frontier window held admission while live spill was past half the cap, and with durable spill most of that
  spill could be certificates already drained, waiting for the part holding their rows to publish, which no
  drain lowers: the drainer then admitted one tile at a time (145 million keys at 2,048 workers, 57 MB cap: 219 s
  against 78 s uncapped). The window now leaves those certificates out of its spill gate and evicts them, oldest
  first, while live spill is past half the cap, as a refused frame does (a crash relists their keys); it holds
  fewer tiles while the spill ahead of the output is past half the cap. Without durable spill (no journal, or a
  cap under 16 MiB) the window is unchanged. The run report adds `resources.window.spill_rows` (the mean rows held
  at the spill gate's refusals, forced admissions' included), `peak_behind_bytes`, `evicted` and `wakes` (the
  window's wake paths that engage only with durable spill) (#535).
- Go port: a name from the source that reaches stderr (a local path in an unreadable-entry warning, a key or
  provider error in a message, a dataset's recorded source) can no longer forge a `swath:` line or send
  terminal control sequences: text lines write control characters, line separators and invalid UTF-8 bytes as
  visible escapes, and JSON log lines write them as `\u` escapes (#521).
- Go port: every committed sorted dataset carries `cuts.txt` beside its manifest: its key distribution in the cut
  file format (a cut at key page ends, at most 262,144 lines and 16 MiB), collected as the parts are published,
  the segment tail's included, and read back from the retained parts' page index on a resume. It is outside the
  SeekParquet format and written last, after the run report; a run that cannot write it (in a bucket, within a
  time bound of its own) warns and stays committed. `--seed-from DATASET` makes its
  tiles from it instead of the parts' page index when it matches the manifest; a bucket dataset's can be copied
  and passed as a cut file. The run report adds `invocation.prior.cuts_file` and `cuts_refused`.
- Go port: a run seeded from a counted prior (`--seed-from` a dataset, or a cut file with counts) lists front to
  back. The prior's tiles are made as the run goes (a dataset's page index read part by part) and admitted in key
  order through a frontier window (8 million rows, smaller when `--spill-budget` and `--sort-buffer` hold
  less), so rows listed ahead of the output stay near the window instead of growing with the bucket, and such a
  relist needs a fraction of a cold run's spill. The window admits tiles; it does not bound storage, so a region
  that grew since the prior can still fill a small cap. A dry run, uncounted cut files and unseeded runs are
  unchanged. The run report adds `resources.window`; the range engine counts `seed.from.merged` and
  `seed.from.plan_errors`; `swath diag list --seed-from-tile-pages` sets a tile's pages (32) and
  `--seed-from-window-rows` the window's rows (#535).
- Go port: a resume that reuses durable-spill certificates writes its tail with the parallel segments. Before,
  such a resume never finished listing: its replay ranges were counted as ranges still to list, so the segments
  never engaged (`drain.segments.reason` `listing_active`), the encoders reserved for after listing were never
  admitted, and every replayed row went through the one drainer. A resume took 2.11 to 2.18 times a fresh run's
  wall on 28 production key lists, and takes 0.67 to 0.82 times it now (10 of those lists). Listing now ends with the gaps around the
  certificates, and the certificates the drainer has not reached by then are replayed by the segments, read by
  offset after one scan of their files. The parts are the uninterrupted run's. The run report adds
  `drain.segments.certificates_read`, `certified_rows` and `certificate_scan_ns`, and
  `spill.durable.resume.replayed_certificates`; `progress.json` counts the bytes the scans read
  (`scanned_bytes`, in `advance`), so a long scan is not stopped as stuck (#288).
- Go port: the durable spill's certificate limits scale with the spill cap instead of stopping at 10,000
  live certificates and a 40,000-record ledger: one certificate slot per 64 KiB of the spill cap (of the free
  space without one), up to a ceiling of 262,144 certificates and 512 MiB of their metadata, keys counted
  (`metadata_exhausted`), which a 16 GiB cap reaches (`limited_by` `metadata`); a ledger history of four records
  per slot, at least 40,000. A resume reads the ledger within the largest limits, whatever free space it finds.
  A billion-key listing that spilled 73,081 ranges sealed 20,000 under the old limits, and in a kill-and-resume
  sweep of 28 lists of 160 to 593 million keys at 2,048 workers, 16 ran out of slots even at one per MiB and
  their resumes replayed as little as 35 % of the keys from certificates, against 93 to 97 % for the rest; those
  lists' ranges now all have a slot, so a resume relists little more than the ranges unfinished at the stop. With the default
  spill cap a resume counts the certified files it kept as free space, so on the same disk it no longer rejects
  those that fit the stopped run's cap. The run report says what the limits derive from:
  `resources.spill.durable.limit_bytes` and `limited_by` (#513).
- Go port: a sorted run whose spill cap is far too small, so that listing waits on the output, says so: once
  spill has stood refused with workers waiting on it (publishing or at admission) for 2 minutes, the error event
  `spill_pressure` names the wait, spill and its cap, the output's rate and the remedy, and repeats every 30
  minutes while it lasts (`order.spill_pressure_since` in `progress.json`). `--spill-wait-timeout DURATION`
  (default 0: wait) stops such a run instead: exit 74, `cause` `spill_budget`, `stop_source` `spill_wait`,
  resumable with more disk or a larger `--spill-budget` (#535).
- Go port: the run report says where a sorted run's workers waited and for how long:
  `resources.publish_wait_ns`, `admission_wait_ns` and `idle_wait_ns` with their peaks (live in `progress.json`
  as `order.publish_waiters`, `admission_waiters`, `idle_workers`), `resources.spill.refused_ns` and
  `refused_spells`, and `resources.drain.writer_before_listing_done`; the range engine's `awaited.*` counters
  say why the range the output waited for was or was not split (#535).
- Go port: a sorted run under a spill cap no longer stops after listing with `spill: corrupt frame` when a range
  had every frame refused by the cap (its spill file then holds only its trailer, which the parallel tail's
  check misread). The failure was loud and published nothing from the bad check.

- Changes from 0.3: 0.4 is a different tool. The Go implementation replaces the Java 0.3 releases with
  another command line (`swath list`, `du`, `show`, `resume`; renamed and removed options), more sources
  (local and mounted filesystems, Google Cloud Storage, Azure Blob Storage beside S3), and another dataset
  format: SeekParquet with the swath dataset profile ([docs/dataset.md](docs/dataset.md)), which 0.3 readers do
  not open. 0.4 does not read 0.3 datasets or resume 0.3 runs. The 0.3 documentation is at the
  [v0.3.2 tag](https://github.com/varveio/swath/tree/v0.3.2/docs).

- Go port: a run in which nothing has answered, every connection refused or its host not found, stops as
  stuck after 30 s (at most half `--stall-timeout`), exit 75, cause `unreachable`, naming the endpoint, and
  resumable as its journal allows (it waited out the whole `--stall-timeout`); once anything has answered,
  failures are retried as an outage, as before. The
  persistent-failure warning says how long retrying lasts. A stuck run without an output directory writes its
  goroutine dump to a file in the temporary directory and names it (the stacks reach stderr only with `-v`),
  and the tmpfs spill warning is printed at a run's first spill, not at every start.

- Go port: `swath du --from-dataset DIR [SOURCE]` totals a committed local dataset (`swath list -o DIR`)
  instead of listing the source again: the same rows, depths, orders, formats and filters as `swath du` of the
  source, read in one parallel pass over the key and the columns the totals need. SOURCE, in the form a live
  `swath du` takes it, is the dataset's source or a path or URL below it, whose keys the read seeks to and
  whose rows are those `swath du SOURCE` prints (a local directory's relative to it); any other SOURCE, such
  as a key prefix, is refused (exit 2) naming the dataset's source. Stderr names the dataset's source, when it was listed and its coverage, and JSONL rows carry it as a
  `dataset` member; a dataset whose listing did not complete, or left out entries it could not read, prints its totals and exits 3
  (`incomplete`).
  Options that shape a listing, a dataset in a bucket and an interrupted run are refused. Plain `swath du DIR`
  still totals DIR's files (#392).

- The Java implementation, its Gradle build, Java images and Java CI jobs are removed from the
  repository; the last Java release is v0.3.2, and the final tree is preserved at the tag
  `java-final` (#73).

- Go port: `reader` keeps swath metadata, `CheckSchema`, the schema registry and concrete typed
  decoders. Scanning, aggregation and browse pages now use SeekParquet's `Plan.ScanParallel`,
  `Dataset.Aggregate` and `Dataset.Page` through `Dataset.SeekParquet()`.
  The unreleased reader wrappers `Stream`, `StreamLocal`, `Scan`, `ScanBatches`, `Totals`, `Aggregates`,
  `List` and `Page`, their request/result types (including `ScanRequest`, `PathTotals` and `StreamStats`),
  and SeekParquet type/error aliases (including `Options`, `Limits` and `ReadStats`) are deleted.
  Callers use SeekParquet types and errors directly; see the [reader guide](reader/README.md).

- Go port: `reader.Open` and `OpenDir` take `seekparquet.OpenOptions`; negative `Limits` fields fail
  with `seekparquet.ErrInvalidRequest`, and limit failures are `*seekparquet.LimitError` wrapping
  `seekparquet.ErrLimitExceeded`. SeekParquet owns read statistics, listing read-ahead and callback stop
  behavior. `CheckSchema` prints an unannotated column's logical type as `NONE`.

- Go port, Azure Blob: `--all-versions` lists every entry of each name: blob versions, snapshots and
  soft-deleted blobs beside the current blob (`include=versions,snapshots,deleted`), under the new schema
  `swath/azblob-versions/v1`: the `swath/azblob-objects/v1` columns, then `version_id`, `is_latest` (the row a
  current listing has), `snapshot`, `is_deleted`, `deleted_time` and `remaining_retention_days`. A name's rows
  are consecutive, in Azure's order (snapshots and versions oldest first, the current blob last), never split
  across parts or ranges; a resume restarts after the last complete name. It needs no permission beyond listing
  and uses the same call and format. It was refused before. Versions and snapshots are tested against
  recorded and synthetic listings only; soft-deleted blobs also against a public container.

- Go port, Azure Blob: rows carry the properties List Blobs returns for every blob that swath dropped:
  `access_tier_inferred`, `access_tier_change_time`, `archive_status` and `rehydrate_priority` (a pending
  rehydration from Archive), `last_access_time`, `content_encoding`, `server_encrypted`, `encryption_scope`,
  `customer_provided_key_sha256`, `lease_state` and `tag_count`. They come in the same responses, so they cost
  no request. `--azure-fetch-metadata`, `--azure-fetch-tags` and `--azure-fetch-immutability` fill `metadata`
  and `tags` (JSON objects) and the immutability policy and legal hold columns, null otherwise; tags need the
  SAS `t` permission or an Entra ID role that reads tags, and a listing refused for it stops naming the
  permission. The columns are added to `swath/azblob-objects/v1` as optional columns, as the dataset evolution
  policy allows: parts written before lack them, and readers read them as null there (`reader`'s
  `AzblobObject` decodes them). A dataset of the same container therefore gains these columns.

- Fixed: Go port, Azure Blob: admission refused most public open-data containers (60 of the 64 Planetary Computer
  containers; NOAA's GOES, HRRR, GFS and NWM; NAIP) unless `--azure-assume-flat` was given (#457). Asked anonymously or
  with a container SAS, Get Account Information does not answer, and those accounts' Data Lake host answers 409
  `EndpointUnsupportedAccountFeatures`, which Azure documents as the answer for a flat account with blob soft delete
  on (a hierarchical one keeps its Data Lake endpoint). That answer now establishes a flat namespace; the report's
  `namespace` reads `filesystem endpoint blocked`.

- Go port: parallel scans use SeekParquet's `Plan.ScanParallel` over borrowed batches; totals use
  `Dataset.Aggregate` with named reductions and caller-selected workers and value budgets. A complete
  parallel scan checks key order across shards, including repeated unique keys at row-group boundaries;
  aggregation requests this validation with `AggregateOptions.CheckKeys`. See
  [reading through SeekParquet](reader/README.md#reading-through-seekparquet).

- Go port, Azure Blob: listings ask for the Arrow format and read XML wherever the service answers XML
  (accounts without Arrow listing, Premium Page Blob, Azurite, swath-replay) (#440). On the 468,557-blob
  `Crime/` prefix of a public container, on a 4-vCPU VM, a listing took 3.4 s and 3.9 CPU-seconds against
  29.5 s and 58 for XML, with byte-identical output ([measurements](docs/internals/measurements.md#arrow-listing-format-2026-11-10)).
  An Arrow page is checked before it is decoded: a stream missing declared rows or failing to parse, a
  missing `NextMarker` or `NumberOfRecords`, or a written column in a type the decoder would read as nulls is
  refused, never listed as a shorter or final page. The run report counts pages per format (`list_format.*`).
  Requests name service version `2026-06-06`, the oldest with the Arrow format, which Azurite 3.37 also
  answers; it refused the earlier `2026-10-06` (#451).

- Go port, Azure Blob: the default page is 1,000 entries, not the service's 5,000 (#448). A range reads to its
  first non-member, and at 5,000 most of each range's last page was discarded; at 1,000 the 1.84M-blob
  container listed in about half the time with half the memory, for three times the requests
  ([measurements](docs/internals/measurements.md#azure-page-size-2026-11-10)). Follow-ups: #449.

- Go port: a credential failure that will not pass (no credentials found, or configured ones that do not load
  or are refused) is reported on one line, the source and the first line of its cause, with `--anonymous`
  suggested only when swath was discovering credentials itself; `-v` and the run report keep the whole
  cause, and transient failures print as before (#447). A SAS naming a request parameter (`comp`, `restype`,
  `prefix`, `marker`, `endBefore` and the like) is refused before any request, naming the parameter, never
  its value.

- Go port: Go 1.27.4; azblob 1.8.3, whose pagers now send `startFrom` themselves (#434, #438). The S3, GCS and
  Azure clients each check the requests they send against a wire snapshot, so an SDK update that changes them
  fails a test (#434, #446). azblob 1.8 links `apache/arrow-go` into the binary (about +4.8 MB;
  Azure/azure-sdk-for-go#27998).

- Release tooling: DuckDB 2.0.0 and Syft 1.57.1 in `release.yml`, OSV-Scanner 2.7.1 in the nightly scan (#442),
  and Buildx v0.38.1 with BuildKit v0.34.2 pinned by digest in `release.yml` (#444); the next release
  candidate is the first `release.yml` run with these pins.

- Go port: ordered streaming uses SeekParquet's `Dataset.Scan` with a zero limit for one pass to the end
  of the span. Callers select columns and optionally decode owning rows with `reader`'s concrete
  decoders. SeekParquet reports per-call `ReadStats`; `Close` waits for callbacks, which must not call it.

- Release: the Go archives and both images carry `THIRD_PARTY_NOTICES`, generated by
  `notices.sh`, which fails the image build and the release on a linked module whose license
  is not recognized or not allowed. The nightly OSV scan covers the Go images and their linked
  modules instead of the Java distributions and images.

- Go port: the command line is reworked (#394). Options are parsed GNU-style: short options bundle (`-vq`)
  and take attached values (`-d1`), a long option takes two dashes, options and operands interleave, and a
  default-on switch is turned off by `--no-NAME`. An option one provider has carries its prefix
  (`--s3-region`, `--s3-profile`, `--s3-requester-pays`, `--gcs-credentials-file`, `--gcs-billing-project`,
  `--azure-sas-token`, `--azure-assume-flat`); the options every object store has are bare (`--endpoint-url`,
  `--anonymous`, `--ca-bundle`, `--no-verify-ssl`). `--include` and `--exclude` take globs, and regular
  expressions move to `--include-regex` and `--exclude-regex`. Text rows use the dataset's column names for
  every source, chosen with `--columns`; `swath du` sorts with `--sort`. `-h` prints a command's short help,
  `--help` all of it; `swath completion bash|zsh|fish` prints a completion script; `--log-format json` writes
  stderr as JSON events. An unknown option names the nearest one, older spellings included (`--region`: did
  you mean `--s3-region`?). `swath list -o DIR` and `-o FILE` print nothing on stdout, except the report
  with `--report -`. The removed names are unknown flags, without aliases; a run recorded by an earlier
  build is finished with it or started over.

- Go port: refuse credential-bearing endpoint URLs before recording a run, redact query and
  fragment endpoint arguments, and redact credentials in location errors and older journal
  displays while preserving clean diagnostic values.

- Go port: S3, GCS and Azure Blob share `--endpoint-url`, `--request-timeout` (default 30 s)
  and `--anonymous`. The provider-prefixed endpoint and timeout flags are removed; resume
  refuses a recorded removed flag by name. Help derives shared groups and provider flags from
  registration, placing shared groups first and providers by scheme within each section (#338).
- Go port: contributors add a provider's runtime integration in its directory plus one blank
  command import, composing shared flags, range-engine interfaces and client activity. Reader
  schemas, verifier support and conformance tests still need explicit integration; the
  [provider checklist](docs/internals/providers.md#adding-a-provider) lists the steps (#338).
- Go port: the reader exposes version-column roles as `VersionSpec` on `SchemaSpec.Versions`.
  Text output, version counts and prefix totals use those declarations; typed decoders still
  read named columns. Schema ids, columns, dataset metadata and report fields are unchanged (#338).
- Go port: the provider audit consolidates retry accounting, token waits, response capture,
  transport pools and payload delta encoding, and gives restart state to the range engine.
  Azure binds its producer schema to the reader specification. Shared conformance tests check
  client and provider integration. Listing behavior, diagnostics and persisted bytes are unchanged;
  provider-specific credentials, endpoint rules and retry signals remain explicit.
- Go port: `reader.DecodeLocalEntry` preserves every filesystem column and its nulls. SeekParquet's
  `List` and `Page` expose a common prefix's exact row for optional typed decoding. Local `CountRows` and
  `Sum(size)` reductions include directories and their own `st_size`; sums skip null/absent sizes, so request
  `CountNulls` and `CountAbsent` to detect unknown bytes under `--metadata names`. Azure rows decode
  `access_tier` with `DecodeAzblobObject`. See the [reader guide](reader/README.md).
- Go port: `--no-dirs` is recorded in a local dataset's `coverage.filters` (`no_dirs`).
- Go port: `objfile` pins objects across opens and restarts (`Pin`, `Restore`, `Pinned.Snapshot`: versioned
  JSON of each opened name's size and identity, restored sealed to those names and refused for another scope),
  parses dataset roots (`ParseLocation`: `s3://`, `gs://`, `az://ACCOUNT/CONTAINER`) and opens one with each
  provider's default credentials (`objfile/remote.Open`). Every ranged read now checks the offset the store served,
  and the GCS client `remote.Open` builds also checks the requests the SDK's reader sends on its own: a read returns
  exactly the requested bytes or fails.
- Go port: `reader` opens and reads; it does not judge a dataset. `Open` exposes the manifest's source, scope
  and coverage through `Metadata()`, and the application decides what to do with them; part digests are not
  checked on read. `IsRefusal` tells the data's refusals from other errors. Browse through SeekParquet's
  `Dataset.Page`, which returns a listing page with its next cursor and requested prefix reductions.
- Go port: `swath list az://ACCOUNT/CONTAINER[/PREFIX]` lists an Azure Blob container on flat-namespace
  accounts (`--azure-*` flags, schema `swath/azblob-objects/v1`), in byte order: names at U+E000 and above, which Azure
  lists in UTF-16 order, are reordered by an adapter whose cost the report's `order_*` counters show.
- Go port: datasets in swath-dataset format 2 (`"format": "swath-dataset"`, `format_version` 2), which builds
  from `main` wrote between 2026-11-05 and 2026-11-08, are no longer read: `swath show`, `--seed-from` and
  `swath-replay` refuse them, and `--overwrite` does not take their directory for this tool's output. Re-list
  them as SeekParquet datasets. Java-era fixtures are still served by `swath-replay`.
- Go port: the local filesystem schema id is `swath/local-entries/v1` (was `swath-local/v1`); schema ids follow
  `swath/<row-contract>/vN`. The manifest's `extensions.swath` carries `"version": 1` as its first member, and
  `swath show`, `--seed-from` and `reader` refuse a manifest whose `extensions.swath` lacks it or holds
  another value ("unsupported swath metadata version"); generic SeekParquet reads ignore the extension. Re-list
  datasets written by earlier development builds. `swath show` names each layer: the SeekParquet format and
  draft, the swath schema id and the swath metadata version.

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
