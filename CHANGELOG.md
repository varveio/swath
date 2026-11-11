# Changelog

Notable changes per release. The full human summary for the current release is in
[release notes](docs/ops/dev/RELEASE_NOTES.md).

## Unreleased

- Go port, Azure Blob: rows carry the properties List Blobs returns for every blob that swath dropped:
  `access_tier_inferred`, `access_tier_change_time`, `archive_status` and `rehydrate_priority` (a pending
  rehydration from Archive), `last_access_time`, `content_encoding`, `server_encrypted`, `encryption_scope`,
  `customer_provided_key_sha256`, `lease_state` and `tag_count`. They come in the same responses, so they cost
  no request. `--azure-fetch-metadata`, `--azure-fetch-tags` and `--azure-fetch-immutability` fill `metadata`
  and `tags` (JSON objects) and the immutability policy and legal hold columns, null otherwise; tags need the
  SAS `t` permission or an Entra ID role that reads tags, and a listing refused for it stops naming the
  permission. The columns are added to `swath/azblob-objects/v1` as optional columns, as the dataset evolution
  policy allows: parts written before lack them, and readers read them as null there (`go/reader`'s
  `AzblobObject` decodes them). A dataset of the same container therefore gains these columns.

- Fixed: Go port, Azure Blob: admission refused most public open-data containers (60 of the 64 Planetary Computer
  containers; NOAA's GOES, HRRR, GFS and NWM; NAIP) unless `--azure-assume-flat` was given (#457). Asked anonymously or
  with a container SAS, Get Account Information does not answer, and those accounts' Data Lake host answers 409
  `EndpointUnsupportedAccountFeatures`, which Azure documents as the answer for a flat account with blob soft delete
  on (a hierarchical one keeps its Data Lake endpoint). That answer now establishes a flat namespace; the report's
  `namespace` reads `filesystem endpoint blocked`.

- Go port: `go/reader`'s `Dataset.Totals` scans in parallel shards over borrowed batches, as `Scan` does, with
  `runtime.GOMAXPROCS(0)` goroutines, instead of one serial pass of owning rows; it plans only the paths not
  inside another (the same rows) and takes each `Count` from the rows its scan reads instead of a count plan
  per path. Totals, checks and API are unchanged; with several rows that fail, which one's error is returned is
  now unspecified, as with `Scan`. `Aggregates` sums in one shard, as before, but no longer plans a prefix
  inside another, so it can decode fewer key pages.

- Fixed: `go/reader`'s sharded reads (`Scan`, `ScanBatches` since they shard, and `Totals`) did not check the
  key order across shards: each shard's scan checks only its own keys, so a key below the one before it, or
  repeating a unique key, where a row group begins (and past the pages a `CheckSchema` decodes) was read as a
  success, with wrong totals. Each shard's first key is now checked against the last of the shard before it,
  failing with SeekParquet's `key_order` refusal naming the key, as one pass refuses it; `Scan` and
  `ScanBatches` return it after the shards' objects were handed on.

- Go port, Azure Blob: listings ask for the Arrow format and read XML wherever the service answers XML
  (accounts without Arrow listing, Premium Page Blob, Azurite, swath-replay) (#440). On the 468,557-blob
  `Crime/` prefix of a public container, on a 4-vCPU VM, a listing took 3.4 s and 3.9 CPU-seconds against
  29.5 s and 58 for XML, with byte-identical output ([measurements](go/docs/internals/measurements.md#arrow-listing-format-2026-11-10)).
  An Arrow page is checked before it is decoded: a stream missing declared rows or failing to parse, a
  missing `NextMarker` or `NumberOfRecords`, or a written column in a type the decoder would read as nulls is
  refused, never listed as a shorter or final page. The run report counts pages per format (`list_format.*`).
  Requests name service version `2026-06-06`, the oldest with the Arrow format, which Azurite 3.37 also
  answers; it refused the earlier `2026-10-06` (#451).

- Go port, Azure Blob: the default page is 1,000 entries, not the service's 5,000 (#448). A range reads to its
  first non-member, and at 5,000 most of each range's last page was discarded; at 1,000 the 1.84M-blob
  container listed in about half the time with half the memory, for three times the requests
  ([measurements](go/docs/internals/measurements.md#azure-page-size-2026-11-10)). Follow-ups: #449.

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

- Go port: `go/reader`'s `Dataset.Stream` and `StreamLocal` read the dataset in one ordered pass
  (SeekParquet `Dataset.RangeBatches`) instead of ⌈N/1,024⌉+1 `Range` requests that each re-seeked after the
  last. Objects, order, values and row errors are unchanged. `StreamStats.Requests` counts `RangeBatches`
  calls, one per stream (more only past `math.MaxInt` rows on 32-bit platforms); a failing part or page now
  ends the stream after the objects of the pages before it (a paged request read 1,024 keys before handing
  any on); `fn` runs inside the read, so `Close` waits for it and `fn` must not call `Close`; each stream's
  `StreamStats` are added once, when it returns.

- Release: the Go archives and both images carry `THIRD_PARTY_NOTICES`, generated by
  `go/notices.sh`, which fails the image build and the release on a linked module whose license
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
  [provider checklist](go/docs/internals/providers.md#adding-a-provider) lists the steps (#338).
- Go port: the reader exposes version-column roles as `VersionSpec` on `SchemaSpec.Versions`.
  Text output, version counts and prefix totals use those declarations; typed decoders still
  read named columns. Schema ids, columns, dataset metadata and report fields are unchanged (#338).
- Go port: the provider audit consolidates retry accounting, token waits, response capture,
  transport pools and payload delta encoding, and gives restart state to the range engine.
  Azure binds its producer schema to the reader specification. Shared conformance tests check
  client and provider integration. Listing behavior, diagnostics and persisted bytes are unchanged;
  provider-specific credentials, endpoint rules and retry signals remain explicit.
- Go port: `go/reader` reads a filesystem's entries typed: `Dataset.StreamLocal` hands on every `LocalEntry`
  (every column, nulls kept), `List` and `Page` return an entry's `Entry.Local` (a directory's prefix `d/` paired
  with its own row), and `Stream` refuses a local dataset. `Totals` and `Aggregates` total local entries of every
  kind and their summed `st_size` (entry bytes, directories' own sizes included); `PathTotals.Bytes` is now
  `*int64`, nil when an entry has no size (`--metadata names`). `Dataset.Scan` scans the current objects under a
  prefix with several goroutines (`ScanRequest{Prefix, Workers, Stats}`), in batches. `Totals` of an Azure
  dataset no longer panics on an exact blob (it read `storage_class`; the access tier is `access_tier`).
- Go port: `--no-dirs` is recorded in a local dataset's `coverage.filters` (`no_dirs`).
- Go port: `go/objfile` pins objects across opens and restarts (`Pin`, `Restore`, `Pinned.Snapshot`: versioned
  JSON of each opened name's size and identity, restored sealed to those names and refused for another scope),
  parses dataset roots (`ParseLocation`: `s3://`, `gs://`, `az://ACCOUNT/CONTAINER`) and opens one with each
  provider's default credentials (`objfile/remote.Open`). Every ranged read now checks the offset the store served,
  and the GCS client `remote.Open` builds also checks the requests the SDK's reader sends on its own: a read returns
  exactly the requested bytes or fails.
- Go port: `go/reader` opens and reads; it does not judge a dataset. `Open` exposes the manifest's source, scope
  and coverage through `Metadata()`, and the application decides what to do with them; part digests are not
  checked on read. `IsRefusal` tells the data's refusals from other errors, and `Dataset.Page` is a listing page
  with its next cursor and its prefixes' totals.
- Go port: `swath list az://ACCOUNT/CONTAINER[/PREFIX]` lists an Azure Blob container on flat-namespace
  accounts (`--azure-*` flags, schema `swath/azblob-objects/v1`), in byte order: names at U+E000 and above, which Azure
  lists in UTF-16 order, are reordered by an adapter whose cost the report's `order_*` counters show.
- Go port: datasets in swath-dataset format 2 (`"format": "swath-dataset"`, `format_version` 2), which builds
  from `main` wrote between 2026-11-05 and 2026-11-08, are no longer read: `swath show`, `--seed-from` and
  `swath-replay` refuse them, and `--overwrite` does not take their directory for this tool's output. Re-list
  them as SeekParquet datasets. Java-era fixtures are still served by `swath-replay`.
- Go port: the local filesystem schema id is `swath/local-entries/v1` (was `swath-local/v1`); schema ids follow
  `swath/<row-contract>/vN`. The manifest's `extensions.swath` carries `"version": 1` as its first member, and
  `swath show`, `--seed-from` and `go/reader` refuse a manifest whose `extensions.swath` lacks it or holds
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
