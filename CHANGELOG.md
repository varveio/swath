# Changelog

Notable changes per release. The full human summary for the current release is in
[release notes](docs/ops/dev/RELEASE_NOTES.md).

## Unreleased

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
