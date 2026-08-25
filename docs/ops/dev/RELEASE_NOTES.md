# swath 0.2.5

## User-facing changes

- **A clearer first-run path.** The README and Getting Started guide now begin with one
  historical-day prefix from NOAA's public `noaa-gestofs-pds` bucket. The 39.6-million-
  object whole-bucket run remains available as a separate full-scale demonstration rather
  than being the first command a newcomer is asked to run.
- **Compressed and partitioned text output.** TSV and JSONL can use gzip or Zstandard, and
  can publish bounded multi-writer directory datasets with manifests and `_SUCCESS`
  completion markers. Text outputs remain one-shot and non-resumable.
- **A diagnostic discard sink.** `--format discard --checkpoint none --report run.json`
  runs the normal listing pipeline without materializing listing rows, making it easier to
  separate object-store/listing cost from output cost.
- **Faster listing and text encoding.** S3 responses are streamed, canonical timestamps are
  parsed through a faster path, redundant checksum conversion was removed, and partitioned
  TSV writes UTF-8 bytes directly. The AWS XML parser was upgraded and compatibility guards
  cover streamed error and response handling.
- **Resource-aware dataset writers.** Direct Parquet and partitioned text output share a
  bounded writer-pool model with a process-wide queue budget. Expert writer counts are
  checked against available heap, and run reports expose aggregate saturation, sticky-lane
  head-of-line blocking, per-lane work, finalization, and publication evidence.
- **Safer dataset publication.** Manifests are published at completion rather than during an
  incomplete run. Part digests are computed while writing, publication ownership is
  centralized, and shutdown/failure paths have stronger liveness and cleanup coverage.
- **Optional writeback shaping.** `--writeback-size` can periodically force bytes already
  emitted to open TSV, JSONL, or direct-Parquet dataset parts and sorted Parquet final
  files. Writeback does not finalize a part, publish a manifest, or shorten the
  crash-recovery window.
- **Faster sorted Parquet finalization.** Sorted output shares the physical Parquet writer
  boundary, avoids repeated final-file work, exposes merge-parallelism controls, and uses a
  faster canonical timestamp conversion path. Final sorted parts retain the same global
  ordering and completeness contracts.
- **`swath-replay` is a published toolkit.** The replay module and container now have a
  stable name, release packaging, runtime attestation, and improved sorted-fixture serving,
  reader admission, metrics, and reproducibility guidance.
- **Newcomer and operator documentation was consolidated.** Public terminology now
  distinguishes a complete live listing from a point-in-time snapshot, defines managed
  Parquet once, labels S3 as the supported backend and GCS XML access as experimental, and
  separates the small quickstart, full-scale evidence, ordinary operation, and contributor
  internals.

## Evidence

- PR #118 added gzip/Zstandard text compression and bounded parallel TSV/JSONL directory
  datasets, with publication, interruption, compression, and CLI-contract coverage.
- PR #123 renamed and published `swath-replay`, added release/container verification, and
  retained replay conformance and runtime-attestation checks.
- PRs #124, #125, #128, #129, #130, and #131 exercised writer-pool telemetry, heap
  admission, streaming digests, shutdown liveness, publication ownership, and
  completion-only manifest publication through focused unit and integration tests.
- PR #135 measured the streaming S3 and TSV paths and recorded the diagnosis method in
  `docs/performance.md`; the discard sink provides a matched output-free comparison for a
  user's own endpoint and bucket shape.
- PR #139's local replay gate measured direct Parquet at 1.516 million rows/s with
  32 MiB writeback versus 1.440 million rows/s without it across five matched runs. The
  result is host- and filesystem-specific, so writeback remains disabled by default.
- PR #140 retained byte-identical sorted output and the existing completeness contract
  while sharing the physical writer boundary, adding final writeback control, and
  exercising merge/finalization paths with focused sorted-output tests.
- The release documentation's headline commands are parsed in
  `HeadlineDocsCommandSmokeTest`; the newcomer entry points also have local-link and
  release-wording regression coverage.

## Limits and known issues

- Swath remains pre-1.0; CLI options, schemas, and experimental controls can still change.
- Durable resume is available for managed Parquet directory datasets. Stdout, text files,
  partitioned text datasets, the discard sink, and the legacy `.parquet`-looking one-writer
  layout are non-resumable.
- A destination such as `-o inventory.parquet` still uses the pre-1.0 compatibility layout:
  a one-writer directory under a file-looking path, not one physical Parquet file. Use a
  directory path for managed output.
- `--writeback-size` is a performance control only. A crash can still lose and re-list the
  current unfinished part because the durability boundary remains final close and
  publication.
- S3 general-purpose buckets are the supported backend. GCS through the XML API is
  experimental S3-compatible access rather than a native GCS implementation. S3 directory
  buckets are not supported.
- A long live listing is not a point-in-time snapshot of a bucket that changes during the
  run. `_SUCCESS` means Swath completed and published the result it observed.
- Existing managed Parquet consumers should continue to wait for `_SUCCESS` and read all
  parts listed by the manifest or a `data/*.parquet` glob.
- Automation should parse `_swath_summary.json` rather than terminal prose. The report is
  additive within its current major schema, and 0.2.5 adds writer-pool, trajectory,
  writeback, and sorted-finalization evidence.
- Re-run performance comparisons after upgrading. Streaming response handling, text
  encoding, writer-pool admission, writeback, and sorted-finalization changes can move the
  bottleneck even when listing semantics are unchanged.
