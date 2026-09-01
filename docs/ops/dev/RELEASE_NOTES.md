# swath 0.3.0

## User-facing changes

- **Parquet `key` is now a `STRING` column.** The `key` column carried no logical type
  before this release, so query engines surfaced it as an opaque binary value. It is now
  annotated `STRING`. Physical `BINARY` storage, the byte-exact key bytes, column
  statistics, and sort order are unchanged, and existing datasets are not rewritten — only
  the type a reader reports moves. DuckDB and Spark report `VARCHAR`/`string` where they
  reported `BLOB`/`binary`; pyarrow and pandas yield `str` instead of `bytes`. Drop the
  conversions that used to be necessary: `decode(key, 'utf-8')` or `CAST(key AS VARCHAR)`
  in DuckDB, `CAST(key AS STRING)` in Spark, and `.str.decode('utf-8')` in pandas. A
  comparison against a blob literal such as `key = 'prefix/'::BLOB` becomes an ordinary
  string comparison. The schema and a short upgrade note are in
  [Using swath](../../usage.md#parquet-schema).
- **Keys that are not well-formed UTF-8 are rejected for Parquet publication.** A key whose
  bytes cannot be represented by the new `STRING` column fails the run with a typed output
  error naming a bounded hex prefix of the offending key, rather than writing a value a
  reader would misdecode. This does not affect a live listing: keys arrive from the S3
  client already decoded, and swath encodes them back to UTF-8, so a live `swath list` run
  cannot produce a key this guard rejects. What it does affect is re-publishing an existing
  capture — `swath-replay sort-fixture` and the capture-sorting path read raw key bytes from
  a Parquet capture without validating them. A capture written before this release that
  holds a non-UTF-8 key stays readable, and `swath-replay` serves legacy binary-key, current
  string-key, and mixed dataset directories; but that capture can no longer be re-published
  as Parquet or sorted into one. Table, TSV, and JSONL output are unchanged.
- **Sorted output finalizes through one reference-routed pipeline.** `--sort` no longer
  carries a second, range-parallel finalization layer. A sorted run stages compressed
  page-run segments under `_staging/` while it lists. At finalization a bounded cascade
  first reduces an over-wide catalog, header-only cursors then scan the surviving segments
  sequentially without decoding payloads, one router consumes every reference exactly once
  and emits complete part plans carrying dense zero-based ordinals, and an admitted pool of
  encoders reads those plans positionally. The plan ordinal — not worker identity or
  completion order — owns final order, so part geometry no longer depends on how many
  encoders ran: the same staged catalog, `final-file-bytes`, and admitted reference cap
  produce the same part count, boundary keys, row counts, and part bytes at one encoder or
  at eight. Admission is priced against real heap and file-descriptor budgets before any
  reader or writer opens, and a budget that cannot fit the work refuses resumably instead of
  failing part-way through. Global ordering, adjacent-part disjointness, and equal-key
  atomicity are unchanged; a raw-key group stays in one part even when that part exceeds
  `final-file-bytes`.
- **Sorted-merge parallelism means something different, and no longer decides how many parts
  you get.** The knob moved from `-Dswath.sort.merge-parallelism` to
  `--tune sort.merge-parallelism`, and it now caps final encoders instead of splitting the
  keyspace into that many contiguous ranges. Part count follows the data and
  `final-file-bytes` alone, so the earlier behavior — parallelism 8 producing eight parts,
  parallelism 4 producing four — is gone. The 256 MiB `min-parallel-staged-bytes` floor that
  kept small sorted runs serial is gone with it: every sorted run now takes the same path.
  So is the failure mode where a heavily overlapping keyspace could not be partitioned and
  collapsed to a single worker.
- **Sorted parts are numbered from zero.** The first part is `part-00000.parquet`, not
  `part-00001.parquet`. The footer's `swath.sort.file_index` stays one-based, so
  `part-00000.parquet` carries `file_index=1` and a consumer that derives an index from the
  filename sees a one-position shift. Sorted part bytes will also not match a 0.2.4 run over
  the same bucket: the staging codec default and the calibrated part sizer both move part
  boundaries, and the `key` annotation changes footer bytes. Row content and global order
  are unaffected.
- **Sorted finalization is separated from dataset publication.** Finalization now produces a
  validated, unpublished part set; a separate publisher owns the staging and output
  directories, final names, and the sweep; and a separate committer writes `manifest.json`,
  `.swath-state.json`, `symlink.txt`, and finally `_SUCCESS`. A pre-publication failure
  leaves the sealed listing resumable and removes only disposable staging, so an interrupted
  finalization can be retried without manual cleanup. Sorted output also refuses to run on a
  filesystem whose Java provider reports no file key, because the identity check swath makes
  before deleting or renaming a directory cannot be weakened safely.
- **Sorted staging written by an older version is refused rather than reused.** The page-run
  staging container is now version 4 and there is no legacy read path. A sorted run
  interrupted under 0.2.4 cannot be resumed by 0.3.0: swath refuses the staged segments and
  advises `--restart`. Published Parquet datasets are unaffected — this applies only to a
  run's in-progress `_staging/` directory.
- **The supported CLI surface is now explicit.** [Supported CLI surface](../../cli.md) names
  every option visible in the installed help for `list` and `resume`, and a coverage test
  requires that anything visible is documented. Diagnostic entry points that were never part
  of the supported surface — `--engine-toggle` and `dump-run` — are hidden. `--tune` stays
  visible as the single typed door to expert keys, and `--tune help` now declares each key
  as stable, experimental, or diagnostic. `--tune sort.keep-staging=on` is new and
  diagnostic: it retains the checkpoint-tracked staging segments after a successful sorted
  publish. No visible flag changed behavior. Four sorted-output JVM properties are gone,
  and a script still passing one will be ignored silently: `swath.sort.merge-parallelism`
  and `swath.sort.merge-boundary-policy`, both superseded by `--tune`; and
  `swath.sort.min-parallel-staged-bytes` and `swath.sort.segment-row-group-bytes`, whose
  mechanisms no longer exist. `swath.sort.final-row-group-bytes` now also prices encoder
  heap admission, so raising it admits fewer parallel encoders.
- **Failure paths that used to be quiet now fail fast and say why.** A fresh run whose seed
  request cannot reach the endpoint at all — a wrong `--endpoint-url`, an unresolvable
  host — fails on the first attempt with `seed_endpoint_unreachable` instead of spending the
  transient retry budget first. `--bearer-token-command` is bounded: its output is capped,
  its streams must close promptly, and its process tree is killed on teardown. Directory
  fsync is probed once per filesystem at startup and now degrades to a no-op only for
  filesystems on an explicit allowlist; a permission or I/O failure anywhere else is fatal
  rather than silently skipped.
- **Compressed and partitioned text output.** TSV and JSONL can use gzip or Zstandard, and
  can publish bounded multi-writer directory datasets with manifests and `_SUCCESS`
  completion markers. Text outputs remain one-shot and non-resumable.
- **A diagnostic discard sink.** `--format discard --checkpoint none --report run.json`
  runs the normal listing pipeline without materializing listing rows, making it easier to
  separate object-store/listing cost from output cost.
- **Resource-aware dataset writers.** Direct Parquet and partitioned text output share a
  bounded writer-pool model with a process-wide queue budget. `--tune parquet.writers` now
  accepts 2 through 64 rather than 2 through 4, because anything above 4 is admitted against
  available heap instead of guessed at; the pool serves direct Parquet output, not `--sort`.
  Run reports expose aggregate saturation, sticky-lane head-of-line blocking, per-lane work,
  finalization, and publication evidence.
- **Safer dataset publication.** Manifests are published at completion rather than during an
  incomplete run. Part digests are computed while writing, publication ownership is
  centralized, and shutdown and failure paths have stronger liveness and cleanup coverage.
- **Optional writeback shaping.** `--writeback-size` can periodically force bytes already
  emitted to open TSV, JSONL, or direct-Parquet dataset parts and sorted Parquet final
  files. Writeback does not finalize a part, publish a manifest, or shorten the
  crash-recovery window.
- **Faster listing and text encoding.** S3 responses are streamed, canonical timestamps are
  parsed through a faster path, redundant checksum conversion was removed, and partitioned
  TSV writes UTF-8 bytes directly. The AWS XML parser was upgraded and compatibility guards
  cover streamed error and response handling.
- **A smaller, pinned distribution and refreshed images.** The shipped dependency closure is
  pinned through version constraints, and netty — eleven jars that no swath code path used,
  shipped with a native/classes version skew — is excluded. A build check asserts that each
  application distribution ships at most one version of each module, which also removed a
  duplicate Jackson generation from the replay distribution. The `ghcr.io/varveio/swath` and
  `ghcr.io/varveio/swath-replay` images move to digest-pinned Temurin 25 bases and are
  scanned nightly, with the gate keyed to the base distribution's own advisory priority.
- **The run report's sorted-output block gained the pipeline's evidence and dropped the
  range layer's.** `_swath_summary.json` now reports which finalization arm ran, header-scan
  and router wait, plan-queue wait, encoder page reads and read wait, peak decoded page
  bytes and reference counts, staging peak bytes, handoff queue depth, and backpressure
  engagement, alongside finalization, manifest, and publication timings. Two things were
  taken away: the `sort.merge_boundaries_ms` field is gone, and `sort.buffer_sort_fallbacks`
  is kept only for compatibility and is always `0`. The OTLP meters
  `swath.sort.merge.range.latency` and `swath.sort.merge.boundaries.latency` are replaced by
  the `swath.sort.pipeline.*` family. Nothing outside the `sort` block changed, and
  `schema_version` remains `2`.
- **`swath-replay` is a published toolkit.** The replay module and container have a stable
  name, release packaging, and runtime attestation. Replay now honors S3 continuation-token
  precedence over `start-after`, renders timestamps correctly outside UTC, serves sorted
  fixtures and cold pages at a cost proportional to the answer, and reads legacy, current,
  and mixed key typings.
- **Newcomer and operator documentation was consolidated.** The README and Getting Started
  guide begin with one historical-day prefix from NOAA's public `noaa-gestofs-pds` bucket;
  the 39.6-million-object whole-bucket run remains available as a separate full-scale
  demonstration rather than the first command a newcomer is asked to run. Public terminology
  distinguishes a complete live listing from a point-in-time snapshot, defines managed
  Parquet once, labels S3 as the supported backend and GCS XML access as experimental, and
  separates the small quickstart, full-scale evidence, ordinary operation, and contributor
  internals.

## Evidence

- PR #171 annotated the `key` column and verified against parquet-column bytecode that
  physical bytes, ordering, statistics, and truncation are unchanged. Key validation was
  consolidated onto one `KeyBytes` implementation, and a DuckDB integration test plus replay
  store tests cover legacy binary, current string, and mixed dataset directories.
- PR #166 landed the reference-routed finalization pipeline behind a tuning key and gated it
  on measurement before adoption. On a retained 9,919,142-row corpus over 23 segments —
  three fresh-JVM samples per arm, alternating, on one 8-core host — the pipeline matched or
  beat the range merge at every parallelism it was compared against: 0.987× wall at eight
  workers, 0.930× at one, and 0.814× at sixteen workers with a 32 MiB part target where the
  range merge had already plateaued. On a fully overlapping corpus, which the range merge
  could not partition at all and served with a single worker, the pipeline finished in
  0.38× the time. Order-sensitive fingerprints and multiset digests were identical on every
  arm of every run. These are ratios from one host and one private corpus at three samples,
  not a throughput envelope; `docs/performance.md` states plainly that no publishable
  production scaling result exists yet for this pipeline.
- PR #169 then made the pipeline the only mechanism and deleted the range layer —
  157 files, +2,267 / −19,652 lines — replacing it with a property test over random
  multi-page segments, overlap shapes, and `final-file-bytes` targets that checks an
  independently sorted fingerprint oracle plus dense-ordinal and cross-part disjointness
  assertions.
- PRs #178, #179, and #180 split the sort package into staging, spill-format, and
  finalization owners with a separate publisher and committer, holding output fingerprints
  and per-file assertion counts unchanged and freezing the dependency direction with an
  executable seam guard.
- PR #183 verified byte-identical part geometry across one, four, and eight encoders and
  across repeated runs; made an oversized overlap component spill its reference coordinates
  instead of exhausting merge memory; scaled writer heap admission with the configured row
  group; and made a finalization capacity refusal resumable end to end through a
  two-invocation checkpoint test.
- PRs #152, #157, and #175 hardened sorted output ahead of the pipeline: staged strings are
  validated as strict UTF-8, staging pages carry verified frame checksums, overlapping or
  regressing key ranges are rejected as typed corruption before publication rather than
  published, and a `_SUCCESS` written before cleanup finished is recovered without re-listing
  or re-merging. Their range-indexing and seek-plan machinery did not survive PR #169 and is
  not part of this release.
- PR #167 measured one real sorted listing of 9,919,142 rows, three interleaved fresh-JVM
  runs per arm: `ZSTD1` staging held 633.6 MB against `LZ4`'s 893.3 MB — 29.1% less staging
  disk — for about 9.7% more pack CPU, with no wall-clock regression detectable at that
  sample size. Staging disk is the constrained resource in a large sorted run, so `ZSTD1` is
  now the default. That listing was network-bound, so a pack-bound run is unvalidated;
  re-measure before sizing a volume from these numbers.
- PR #158 keeps full column-index bounds for keys up to S3's 1,024-byte limit in served
  sorted files, costing 0.075% in file size and buying exact single-page pruning on long
  shared-prefix keys. PR #136 moved sorted parts to zero-based filenames, and PR #149
  guaranteed that an equal-key group lands in one part.
- PR #170 gave the supported surface its own page, pinned the help goldens, and added the
  coverage test asserting that every visible option on `list` and `resume` is named there.
- PR #165 covered the endpoint-unreachable seed path, the bounded
  `--bearer-token-command` process contract, and the directory-fsync probe with its explicit
  unsupported-filesystem allowlist.
- PR #118 added gzip/Zstandard text compression and bounded parallel TSV/JSONL directory
  datasets, with publication, interruption, compression, and CLI-contract coverage.
- PRs #124, #125, #128, #129, #130, and #131 exercised writer-pool telemetry, heap
  admission, streaming digests, shutdown liveness, publication ownership, and
  completion-only manifest publication through focused unit and integration tests.
- PR #135 measured the streaming S3 and TSV paths and recorded the diagnosis method in
  `docs/performance.md`; the discard sink provides a matched output-free comparison for a
  user's own endpoint and bucket shape.
- PR #139's local replay gate measured direct Parquet at 1.516 million rows/s with
  32 MiB writeback versus 1.440 million rows/s without it across five matched runs. The
  result is host- and filesystem-specific, so writeback remains disabled by default.
- PR #154 scanned the previously shipped closure and found 62 OSV advisories across
  16 packages — 1 critical, 26 high, and 40 of them from netty alone. After pinning and the
  netty removal the CLI distribution is 84 MB in 98 jars (from 87 MB in 111) and the replay
  distribution 171 MB in 69 jars (from 175 MB in 83), with 9 advisories remaining, none
  critical. Those nine share one root cause — Parquet's own relocated Jackson copy, which
  no dependency constraint can reach — and are individually excepted with expiry dates
  rather than muted; the scan fails on anything not listed, and PR #174 runs it nightly over
  both distributions and both runtime images. PRs #153 and #173 made CI a real gate, stopped
  the deep tier from being silently skipped, and stabilized the fixtures that gate depends
  on.
- PRs #123, #113, #120, #133, #127, and #147 published `swath-replay` with release and
  container verification and corrected its continuation-token precedence, non-UTC timestamp
  rendering, and cold-page and sorted-fixture serving cost.
- The release documentation's headline commands are parsed in
  `HeadlineDocsCommandSmokeTest`; the newcomer entry points also have local-link and
  release-wording regression coverage.

## Limits and known issues

- swath remains pre-1.0; CLI options, schemas, and experimental controls can still change.
- Re-publishing a capture that holds a key which is not well-formed UTF-8 fails at final
  encoding, not at read time. Nothing validates keys as a capture is staged, so on a large
  capture the failure arrives near the end of the work, and it recurs on retry — the input
  has to be corrected. A live listing is not exposed to this.
- Sorted staging is not portable across swath versions. A sorted run interrupted under an
  earlier release must be restarted rather than resumed.
- Sorted finalization has no free-space preflight of its own. swath checks free space before
  and during the listing, so retain enough capacity for checkpoint-owned staging, cascade
  intermediates when fan-in is constrained, and temporary plus published parts together.
- `swath-replay`'s `--inject-latency` is now a deadline rather than an addition to the
  server's own cost, so injected-latency numbers recorded before this release are not
  comparable to numbers recorded after it.
- Durable resume is available for managed Parquet directory datasets. Stdout, text files,
  partitioned text datasets, the discard sink, and the legacy `.parquet`-looking one-writer
  layout are non-resumable.
- A destination such as `-o inventory.parquet` still uses the pre-1.0 compatibility layout:
  a one-writer directory under a file-looking path, not one physical Parquet file. Use a
  directory path for managed output.
- `--writeback-size` is a performance control only. A crash can still lose and re-list the
  current unfinished part because the durability boundary remains final close and
  publication.
- Sorted output requires a local filesystem whose Java provider reports file keys.
  Object-store and other alternative `FileSystemProvider` implementations are refused during
  preflight rather than sweeping a directory swath can only identify by pathname.
- A hard kill during sorted finalization can leave disposable staging that the free-space
  precheck counts before the sweep that would remove it (refs #184).
- S3 general-purpose buckets are the supported backend. GCS through the XML API is
  experimental S3-compatible access rather than a native GCS implementation. S3 directory
  buckets are not supported.
- A long live listing is not a point-in-time snapshot of a bucket that changes during the
  run. `_SUCCESS` means swath completed and published the result it observed.
- Existing managed Parquet consumers should continue to wait for `_SUCCESS` and read all
  parts listed by the manifest or a `data/*.parquet` glob.
- Automation should parse `_swath_summary.json` rather than terminal prose. The report is
  otherwise additive within its current major schema, but 0.3.0 is an exception inside the
  `sort` block: it drops `merge_boundaries_ms`, pins `buffer_sort_fallbacks` to `0`, and
  retires two `swath.sort.merge.*` OTLP meters while `schema_version` stays `2`. A consumer
  that reads those specific sort fields needs updating; every other block is additive.
- Re-run performance comparisons after upgrading. Streaming response handling, text
  encoding, writer-pool admission, writeback, and the rewritten sorted finalization can move
  the bottleneck even when listing semantics are unchanged.
