# Testing

```bash
cd go
go vet ./...
go test -race ./...
```

CI runs the same in the `go` job of `.github/workflows/ci.yml`, plus `CGO_ENABLED=0` builds for
linux/amd64, linux/arm64, darwin/arm64 and windows/amd64. A commit touching `go/` needs that job green.

`SWATH_PERF=1` is the performance tier: a test whose law holds at a small scale runs that scale by default
and the full one (seconds, more memory) only then. CI does not set it.

## Invariant tests

| Invariant ([internals/contracts.md](internals/contracts.md)) | Test |
|---|---|
| hostile keyspaces | `source/keyrange` `TestHostileFixturesMatchSequential`: the cardinality bomb (6,000 keys after a 200-byte shared run, plus neighbours of the prefix) and the encoding minefield (percent-encoding traps, `+`, spaces, `?#&=`, control characters, NBSP, BOM, U+FFFD, non-BMP, combining marks, long runs of one byte or scalar) list row-identical to the sequential oracle, parallel and seeded, and their ranges tile the keyspace |
| metamorphic laws | `source/keyrange` `TestMetamorphicLaws`: one table over every fixture and hostile keyspace; the listing is invariant to the page size (pages capped at 3, 7, 13, random short pages), the range decomposition (seed on and off, the seed's pagination and descent at pages of 3, 1, 4, 8, 16, 64 workers, forced random splits, a seeded resume), the memory budget (20 rows to 1 M rows) |
| the seed | `source/keyrange` `TestSeedDescent`: the waves, requests, levels, folders per level and stop reason on the single-root date layout (5 waves, 16 requests, 4 levels to 336 days), a 2,300-folder top level (3 pages), 95 % of the keys under the last folder (one level; no tile over 3/(4·workers) of the keys, an empty open frontier), one folder per level 100 deep (the level budget), top-level objects (the round-trip budget) and the page cap; `TestSeedListsEveryKey`: every shape, fixture and hostile keyspace row-identical, and the seed's tiles contiguous with cuts strictly ascending, safe, real folders, inside the scope; `TestSeedResume`: a resumed scope tiled with every cut above K, row-identical under random splits, on the S3-shaped and the inclusive-start providers; `TestSeedResumeAfterCrash`: seeded runs killed at random splits and range ends (`faults.At`), resumed with the seed, row-identical to the keyspace; `TestSeedChargesEmptyPages` (empty continuing pages before the first folder are seed requests and round trips: 10 on the top level stop at 6 round trips, 40 folders behind one each stop at 64 requests; `seed.requests` equals the delimiter requests sent), `TestSeedFailurePublishesCounters` (a failing or canceled second wave still reports its requests, round trips, pages and `seed.stop.failed`) |
| dataset format contract | `listing` `TestDatasetContract`: multi-part S3 datasets (sequential, parallel, seeded) and a local one with a non-UTF-8 name, read from the manifest alone: parts are exactly `data/` in name order, bytes and MD5 match, footer rows and exact base64 bounds match, keys rise strictly within and across parts, footers carry exactly the four `swath.*` stamps and `sorting_columns` key ascending, and the three S3 runs hold the same keys |
| no gap, no overlap; boundary to the left | `source/keyrange` T1 (random splits cover every key once, traced intervals tile the keyspace) |
| safe pivots | `source/keyrange` T2 (pivot properties over adversarial bounds) |
| guards | `source/keyrange` T3 (every clause) |
| paging, trimming, truncation errors | `source/keyrange` T4; `source/s3` `TestOutOfOrderListingFails` |
| the S3 adapter's requests | `source/s3` `TestRequests`: the scope prefix, a resume's start-after, continuations without it, 1,000-key pages, the seed's delimiter listing in its first wave (beside the first data page), a cut at a folder whose name holds `%` sent as start-after (`seed-percent`); the engine's split decisions are covered by its laws (T1, resume, sorted output), not pinned by a hash |
| split balance (synthetic) | `source/keyrange` T6 (`p/%07d`, 200k keys, 20 ms pages, 16 workers) |
| pivot policy (synthetic) | `source/keyrange` `TestPivotPolicyEngages`: date-partitioned, nbm-like, flat and hashed keys (about 500 pages of 100 keys, 1 ms, 64 workers) keep `split.max_depth` within 4·log2(pages)+8, with the pivot-source and engagement counters naming the paths; `TestPrefixedFrontierSplits` (a scope prefix: the frontier still splits); `TestReflectionCeiling`, `TestGallopStride`, `TestGenerationAndAnchor`, `TestSliverRefused` (refused, not unsplittable: it decides again after each page); `TestInvalidScopeCeiling` (scope `ÿ`, byte ceiling `c3 c0`: a one-key first page is refused, not made unsplittable, and the next page splits inside the scope), `TestInvalidAnchor`; `FuzzPropose` (any proposed pivot is strictly inside its bounds, valid, Safe, at most 1,024 bytes; seeded from the fixture and hostile keyspaces); `TestStructureEngages`: with structure probes on their evidence (the command's default), date and nbm-like keep `split.max_depth` within 3·log2(pages)+8 (about 19 and 13), pivoting on folder boundaries; flat keys send at most 8 probes and none on stall evidence, hashed keys none; a split never waits for a probe, so the timed runs require the structural path or, when every probe was late (`structure.late`), the anchored reflection, and an inline arm (`probeInline`: each probe lands before the next decision) requires the structural path; `TestStructureSuppressionInherited` (children inherit the count of probes that found no folder; asserted with the probes inline, since whether a probe lands before its range splits is a race the engine never waits on), `TestStructureLevels`, `TestStructureFlag`. The metamorphic laws gain `w64/structure-on-frontier` and `w8/structure-auto/forced-splits/page=7`, T1 and `TestResumeWithSplitKills` run a share of their seeds with structure on its evidence |
| split admission | `source/keyrange` `TestGaugeEstimates` (the gauge reads a run in its own alphabet, a pivot past the page's digits as their end, and +∞ for a bound in another folder); `TestAdmissionEngages` (each gate refuses on the synthetic shapes at 64 workers, the listing is the keyspace and the ranges tile it); `TestAdmissionRedecides` (a refusal declines one page's split, not the range's); `TestAdmissionFlat` (a flat list of 1,000 pages ends with fewer than 360 ranges, within the depth bound and about 64 wide: 100,000 keys in pages of 100 by default, the million-key list in pages of 1,000 with `SWATH_PERF=1`) |
| count-only and retained bounds | `order` `TestCountCountsEveryRow`, `TestCountGuardsEachRange` (a range's own keys repeating or descending, or at or below the resume key, fail the count; overlapping ranges are not detected, by design), `TestCountStops`, `TestCountStopsOnEmptyBatches` (after a stop an empty `Rows` batch returns the cause); `listing` `TestCountOnlyHoldsNothing` (no file under `TMPDIR`, no spill, the order-stage peaks zero, the heap at the pages in flight) and `TestRetainedBoundsDoNotPinPages` (real XML through the production parser behind a held-up first range: the peak live heap stays within the budget and the pages in flight, and fails with the cursor left a view of its page); `source/keyrange` `TestDetach` |
| GCS connections | `gcs/client` `TestTransportPoolSpreadsConnections`: an HTTP/2 TLS server sees exactly `PoolSize(workers)` connections, with equal shares, at 1, 16, 64 and 128 workers |
| order and memory bound | `order` `TestOrderedOutput`, `TestSinkFailureStopsPromptly`, `TestCancellation`, `TestFailedRangeStopsTheStream` (a failed range stops the drainer before it is seen as done: rows of later ranges already listed never reach the sink past the gap its dropped children leave; every run holds the drainer until the workers have expanded both siblings and holds the context's stop callback until the run returns, barriers rather than sleeps, so it fails on every run without the latch), `TestInterruptedRunReportsStall` (a run cancelled, or failed by its provider, after 200 ms with nothing handed on reports that stall as `FrontierStuck`), `TestFinalFlushReportsStall` (one row batched until the final flush, a sink blocked until the deadline: the wait is the stall) |
| deterministic output | `parquetout` `TestRouterIsDeterministicAcrossEncoders`, `TestColumnParallelEncodingIsByteIdentical` |
| publication order and gate | `dataset` `TestCheck`, `TestPublishFailureLeavesNoSuccess`; `listing` cancellation and encoder-failure tests |
| crash and resume (I1, I5, I6) | `listing` `TestCrashMatrix`, for S3 and GCS (a fake JSON API, `gcs/gcstest`): an injected crash (no cleanup, as a kill) at every point in `faults.Points` (split, range done, part closed, record written, rename, every part published, manifest installed), at its 1st, 4th and 12th occurrence (GCS: 1st, 3rd, 7th; spill on and off for S3; a first occurrence never reached fails the test, except the points not on a provider's path, later ones skip), and the first part's journal append (`journal-append#2`: the calibrating part claimed, unrecorded), then resume (the durable spill's points, `faults.SpillPoints` and `faults.RecoveryPoints`, have their own matrices on a fixture that reaches them, failing if one is not reached: `TestDurableKillMatrix` with torn writes and lost unsynced state, `TestDurableRepeatedRecovery`, `TestSIGKILLDurableSpill`): the dataset is byte-identical to an uninterrupted run (row-identical in the arms with the part interval on, whose time cuts depend on timing). `cmd/swath` `TestSIGKILLAtEveryPoint`, for S3 and GCS (the real process SIGKILLs itself at 13 points, including between a rename and its directory sync, before the manifest and after it, and at `journal-append:2`; every first occurrence must be reached except `split` in the seeded arms, whose ranges never split (S3 runs seeded and unseeded, the unseeded arm crossing it); byte-identical with `--part-interval=0`, since a 30 s time cut under load moves part boundaries, and row-identical in a 1 ms interval arm), `TestGCSOutageThenResume` (503 to every request from three quarters through a run: exit 75, resumable, then `swath resume` byte-identical), `TestSIGKILLResume`: a real process killed with SIGKILL after 4 parts, resumed with `swath resume` (a frozen flag on resume is a usage error, exit 2). `TestCalibratingClaimRecoversRepeatedly`: the calibrating part claimed and unrecorded, the resume killed again (before and at the reproduction's claim, after the next record, and inside recovery while syncing the suspect directory it was moved to), a second resume byte-identical; `parquetout` `TestPinnedCalibratingPartCalibrates`. `TestResumeChecksIdentity`: typed mismatch errors, overwrite needed for a fresh run. `TestGCSResumeRefusesAnotherRoute`: refusal of a GCS resume through another route or billed project. `TestCredentialRotationResumes`: a signed run resumes with rotated keys (credentials are not identity), rotated keys with another region are refused; `TestGCSCredentialRotationResumes` the same on GCS (bearer tokens from a rotated authorized-user credential resume; another billed project is refused). `cmd/swath` `TestGCSCappedRunIsIncomplete`: a GCS run capped by `--max-keys` commits with incomplete coverage, exits 1 (`incomplete`, not resumable), keeps no journal, and `swath resume` refuses it unchanged. `listing` `TestDiskFaultMatrix`: a part file, a journal record and the manifest each fail at a write (half its bytes, then `ENOSPC`) and at its fsync (`EIO`): the run fails without a manifest, published parts stay, and a resume is byte-identical. `TestWatchdogWriteInFlightAtRelease`: a watchdog write under way at the end of a run keeps the lock held and the run waiting; a later write is refused. `TestClaimedPartKeepsItsColumns`: a GCS run by a build without `time_created` stopped with part 1 claimed (at `journal-append`, and as a torn record) is resumed by a build with it: part 1 is reproduced byte for byte with the claimed part's columns, the later parts carry the column (`parquetout` `TestCutReproducesClaimedFooter`: the claimed part's columns and `created_by` are each needed) |
| compositions | `listing` `TestResumeChangesPriorWorkersEncodersAndSpillFormat`: a run seeded from a cut file, spilling delta-coded zstd frames with 8 workers and 2 encoders, killed (after its third record, or with the calibrating part claimed), resumed with the prior removed or stale, 3 workers, 1 encoder and plain uncompressed frames: byte-identical to the unseeded clean run. `TestPriorSlowSplitPercentThrottleCompose`: a cut-file prior over folders named with `%`, slow-split on, one slow folder and a 503 SlowDown every seventh request: every key once, in order, with tiles from the prior, `%` bounds sent and throttles met (the slow-range reason itself does not fire on that shape). `TestTextFormattersShareTheSpillingBudget`: four formatters on the 128 KiB budget the spilling order stage shares: the file equals the one-formatter run's; a write failure is the run's error; a cancellation with spill live is the cancellation; the destination untouched and nothing left beside it |
| resume after K (I4) | `source/keyrange` `TestResumeAfterKey`: random keyspaces, split coins and K; the resume lists exactly the keys after K and its ranges tile (K, ⊤]. `TestResumeWithSplitKills`: K inside a date folder, also a `%` and a control-character key (listed from K's safe prefix), killed at random split points (`faults` "split") and resumed after the last key that reached the sink until done: exactly the keys after K, once each |
| spill (step 3) | `order` `TestSpillKeepsWorkersBusy` (slow dense frontier, 56 fast siblings, 220-byte keys, 8 workers, 500-row budget; the frontier is held until every sibling has finished emitting: identical output, buffered rows within budget + one batch per worker + the frontier path, every sibling completes behind the held frontier (spill keeps them progressing; a liveness property, not a speed figure), no files left; plain and zstd), `TestSpillCleanedOnFailure`; `TestSpillDiskBudget` (a cap at a quarter of the uncapped peak holds with identical output; a reserve larger than the disk spills nothing); frontier and budget waits are nonzero without spill; frame formats: `order` `TestDeltaFramesRoundTrip` (every producer's random sorted rows through delta, delta+zstd, plain and plain+zstd frames: arbitrary byte, empty and mutually prefixing keys, null fields, uppercase, quoted, short and multipart ETags, times across zero and int64 overflow; the reader's buffers are overwritten after each frame, so a row still viewing them fails), `TestDeltaFrameMalformed` (prefix expansion past the declared storage, storage above any written, prefix past the previous key, suffix past the frame, trailing bytes, foreign payload coding or frame type, a zstd bomb: refused before allocating, decode-cap refusals counted), `s3` `TestDeltaCodecForms`, `listing` `TestSpillFormatsIdentical` (spill forced in every format: Parquet byte-identical to no spill), `BenchmarkSpillFormats` (bytes, CPU and allocations per row) |
| summary semantics | `listing` `TestSummaryDistinguishesOffFromZero`: a disabled policy's counters are absent and its feature reads false; an enabled one reports zeros |
| attempt latency percentiles | `source/objhttp` `TestLatencyBuckets` (the finite buckets tile 0 to 2^40−1 ns, each at most a quarter of its lower edge wide), `TestLatencyQuantiles`, `TestLatencySaturation` (2^40−1 ns reports the last finite edge and no overflow; 2^40 ns and a 20-minute attempt count as `inflight.latency_overflow`, and a percentile among them reads open, 9223372036854775807) |
| layering | `source` `TestImportBoundary`: the pipeline packages import nothing under `internal/source/` (transitively), `internal/source` imports no provider, `cmd/swath` imports providers only with `_` |
| journal | `journal` `TestJournalRoundTripAndTornTail`, `TestCheck` (identity table: pipeline fields, and source fields added, dropped, changed or respelled) |
| overwrite safety | `dataset` `TestPrepareNeverDeletesTheTree`, `TestReplaceRemovesOnlyOwnEntries` |
| command surface and report schema | `cmd/swath` `TestCLIDocFlags` (docs/cli.md's flag block is the generated `--help` of `swath list`, `swath count` and `swath diag list`), `TestHelpGroupsCoverFlags` (every flag in exactly one help group; the Diagnostics group is exactly the flags `swath list` refuses), `TestMovedFlagsNameDiag` (each flag moved to `swath diag` is refused on `swath list` and `swath count` with exit 2 naming `swath diag`, accepted by `swath diag list`), `TestCountRefusesOutputFlags`, `TestCount`, `TestOldNamesRejected` (the names of the builds of `main` before 0.4 and Java's, `--output`, `--force`, `--endpoint-url`, `--quiet`, `--spill-max`, `--count-only`, and `--concurrency`, are unknown flags, exit 2), `TestEveryHelpExitsZero`; `listing` `TestReportSchemaGolden` (`testdata/report-schema.golden`: every JSON path of the run report), `TestReportSchemaDoc` (docs/cli.md's "Run report" names every field), `TestHistoryBounded` |

## Oracles

- **Filesystem:** a sorted `filepath.WalkDir` (`TestListingEqualsSortedOracle`), exact sequence, both
  engines, every tier, with and without DIR rows, reorder budgets from 1 to unbounded, tricky names.
- **S3 parallel vs sequential:** the parallel core's dataset is byte-identical to the sequential
  continuation-token listing (`listing` `TestS3ParallelEqualsSequential`, on gofakes3).
- **S3 client vs the AWS SDK:** `internal/source/s3/sdkoracle` puts the SDK behind the provider's `API`.
  `TestClientPagesMatchSDK` sends the same requests through both and compares every page field;
  `TestRowsMatchSDK` compares rows, sequential and parallel; `listing` `TestDatasetsMatchSDK` requires
  byte-identical datasets (SDK sequential vs client sequential, parallel and seeded, and SDK parallel) on a
  bucket with spaces, `+`, `%`, XML metacharacters, non-ASCII and 900-byte keys. Credentials and signing
  are the SDK's own modules; `TestCredentialChain` checks the chain's use (environment, profile,
  anonymous, a missing profile); `TestCredentialsFailClosed` that a configured source that is slow, mounted
  late, refused or down never becomes anonymous.
- **Page parser vs encoding/xml:** `FuzzParseList` (arbitrary documents) and `FuzzParseFields` (valid
  documents from fuzzed values) require that whenever encoding/xml accepts a document, the scanner returns the
  same page. `TestEveryTruncationFails` holds that no proper prefix of a page parses, so a cut body is never
  taken for a shorter page; `TestParseFailuresAreRetried` that such a body is retried and counted. The
  scanner reads the whole body before parsing, so network chunk boundaries never fall inside a token.
- **GCS client vs Google's JSON API client:** `TestClientPagesMatchGoogleClient` sends the same requests
  through the hand-rolled `objects.list` call and `google.golang.org/api/storage/v1` (a test dependency;
  the binary does not link it) to a fake JSON server and compares every field the schema keeps (names,
  prefixes, tokens, size, generation, metageneration, etag, update time, creation time, storage class, MD5,
  CRC32C, content type) across prefixes, delimiters, both offsets and keys that need escaping or hold control characters. Our
  client runs twice: on the fake's endpoint, and addressed to Google's endpoint (redirected to the fake), where
  it sends the production `fields=` selector, which the fake applies while the Google client reads full
  responses; `TestRowsCarryEveryField` checks the adapter's row mapping. The fake's page tokens are opaque
  keyed hashes bound on the server to their chain's parameters; it can serve empty continuing pages, short
  nonempty pages and a lowered page cap (`TestTokenChainsAndEmptyPages`). `TestEndOffsetOmitted` forces
  bounded ranges ending at 1,024-byte names and requires the omitted `endOffset`, the counter and the trim to
  engage; `TestDelimiterBoundaries` lists a folder marker, its descendants and its byte-order neighbours at
  page size one under every delimiter-sending policy. Credentials are tested against a local token endpoint
  (`internal/source/gcs/client`, `creds_test.go`).
- **Java swath on replay** (the differential oracle): see "Replay oracle" below.

## Replay oracle

The Java tool and its replay server are the reference producer. The Go tool is held to them on recorded
listings: Java-produced recordings are the differential input, and the Go dataset must hold the same rows
(`bench/diffds.py`, all 13 format-1 columns, the format-2 fields projected with the constants of
[upgrade.md](upgrade.md)). That establishes logical value parity, not format compatibility: Java's reader
does not read format 2 (its sorted reader requires the format-1 sort stamps, which format 2 replaced), so a
Go dataset is accepted by the downstream reader and by an independent Parquet reader, DuckDB through
`verify.py` (below; [upgrade.md](upgrade.md#gates)). In a fresh container (JDK 25 is required; the system JDK
will not run the launchers):

```bash
# 1. JDK 25 (Temurin) into its own directory; check the SHA-256 against the Adoptium API.
mkdir -p ~/jdk25 && cd ~/jdk25
curl -LO https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.5%2B8/OpenJDK25U-jdk_x64_linux_hotspot_25.0.5_8.tar.gz
sha256sum OpenJDK25U-jdk_x64_linux_hotspot_25.0.5_8.tar.gz   # 68982529869257d0ee6b824ec5aa270ff8b118bb438670da0b97ebdab178d7fb
tar xzf OpenJDK25U-jdk_x64_linux_hotspot_25.0.5_8.tar.gz && export JAVA_HOME=~/jdk25/jdk-25.0.5+8 PATH=~/jdk25/jdk-25.0.5+8/bin:$PATH

# 2. Build the Java CLI and replay server from this repository (about 75 s with a warm cache).
cd "$(git rev-parse --show-toplevel)"
./gradlew --max-workers=2 -Porg.gradle.java.installations.paths=$JAVA_HOME \
  -Porg.gradle.java.installations.auto-download=false :swath-replay:installDist :swath-cli:installDist
export PATH=$PWD/swath-replay/build/install/swath-replay/bin:$PWD/swath-cli/build/install/swath/bin:$PATH

# 3. Record a bucket with Java (sorted Parquet; anonymous for public buckets).
swath list s3://noaa-normals-pds --region us-east-1 --no-sign-request --concurrency 64 \
  --format parquet --sort -o /data/rec/noaa-normals-pds --report /data/rec/noaa-normals-pds/_swath_summary.json

# 4. Serve the recording (path-style S3, no auth, plain HTTP), optionally with recorded latency.
swath-replay serve --fixture /data/rec/noaa-normals-pds/data --bucket noaa-normals-pds --host 127.0.0.1 \
  --port 19090 --serving-mode sorted --metrics-port 19192 --inject-latency worker_page=95ms &
until curl -sf http://127.0.0.1:19192/healthz >/dev/null; do sleep 1; done

# 5. List it with the Go tool and compare.
cd go && go build -o /tmp/swath ./cmd/swath
/tmp/swath list s3://noaa-normals-pds -endpoint http://127.0.0.1:19090 -path-style -anonymous -o /tmp/go-ds
python3 bench/diffds.py /data/rec/noaa-normals-pds /tmp/go-ds       # identical: true
python3 verify.py /tmp/go-ds                                         # the format-2 dataset, read by DuckDB
```

- `bench/diffds.py A B` compares two datasets with DuckDB. It checks:
  - the row sets on the 13 canonical columns, in both directions;
  - that both datasets are strictly ascending by key bytes;
  - the dataset's minimum and maximum key.

  It also prints part counts and sizes side by side. Replay's S3 route renders `LastModified` in
  milliseconds; datasets listed through replay therefore agree at that precision, and `--ts-ms` compares a
  replayed listing with a direct one.
- **Java's reader on Go datasets:** not a gate since format 2. Replay's sorted serving mode validates the
  format-1 sort stamps and refuses a format-2 dataset; the results of that check in
  [measurements.md](measurements.md#output-identity-with-the-java-tool) predate format 2 and are historical.
  Format-2 acceptance is the downstream reader's and `verify.py`'s.
- **Synthetic fixtures** are generated with DuckDB and sorted with `swath-replay sort-fixture --capture DIR
  --output DIR`. A result from such a fixture is synthetic and names its generator. The 1M-key fixture of
  [measurements.md](measurements.md#output-identity-with-the-java-tool) is generated by `bench/gen_fixture_1m.py`.
- **Replay store fixtures:** the replay store's unit tests build every fixture in the test. Its Java-written
  evidence tier (`go/internal/replay/store` `TestJavaWrittenFixtures`, `TestSortsTheAzureEvidenceCorpus`,
  `TestStartupCost`, and `TestRecordings` when `recordings/BUCKET/dataset/data` exist) runs with
  `SWATH_REPLAY_FIXTURES=ROOT` and skips without it. `bench/replay/make-fixtures.sh ROOT/fixtures` writes
  that tree from this checkout with the Java replay distribution on `PATH` and `JAVA_HOME` set: DuckDB
  captures (synthetic 10k and 1M rows, the GCS and Azure evidence corpora), sorted by the Java
  `sort-fixture`. The script fixes every file's modification time, so the fixture identity the test
  compares with the Java server's is reproducible. CI runs this tier, except `TestRecordings`, in the
  `go-fixtures` job ([replay.md](replay.md#continuous-integration)).
  The same variable runs the GCS evidence replay (`go/internal/replay/evidence`, see
  [replay.md](replay.md#the-gcs-evidence-replay)); it needs only the committed archive and uses
  `ROOT/fixtures/evidence-gcs/sorted` when present. The Azure evidence replay (`go/internal/replay/azure`
  `TestAzureEvidenceReplay`) runs on the same variable and sorts `ROOT/fixtures/evidence-azure/capture`
  itself; CI's `go-fixtures` job runs both evidence replays.
- **Policy toggles:** Java's policy layer can be switched off per mechanism with the hidden
  `--engine-toggle NAME=off`. The names are `owner_split`, `density_ewma`, `radix_bands`,
  `structure_probes`, `far_ahead`, `alphabet_pivots`, `reflect`, `confetti_feedback`, `reflect_lift`,
  `fanout_tiling`, `mass_aware_seed` and `rate_anchored_sensing`, plus `readahead=on` and
  `tail_floor=MODE`. The seed is set with `--tune seed.mode=none|shallow`.
- **Run summaries:** `bench/summaries.py PATH...` tabulates Java's `_swath_summary.json` and the Go tool's `run.json` into the same columns (the Java-to-Go
  name mapping lives there).
  Each run is one TSV row, keyed by bucket, version and configuration, with every engagement counter as a
  column.
- **Policy arms on a recording:** `bench/policy_replay.sh ARMS BUCKET LATENCY_MS PORT REPS OUT` serves one
  recording with a fixed page latency and runs the arms listed in `ARMS` (Java flags, or a Go binary and its
  `swath diag list` flags) interleaved, `REPS` times; `bench/policy_table.py OUT` prints wall, requests,
  attempts (the replay server's HTTP count), splits, empty children, overfetched keys, probes and mean active
  ranges per arm.

- **Replay load tests:** `SWATH_REPLAY_LOAD=1` runs the replay server's load tests
  (`go/internal/replay/server` `TestLoad*`: the request ceiling under the fault proxy, the reader-slot bound under
  256 walks, no overrun at 64 walks), a few seconds each; they serve `ROOT/fixtures/synthetic-1m/sorted-rolled`
  when `SWATH_REPLAY_FIXTURES=ROOT` is set, else a generated fixture. `bench/replayload` is the walker the
  [replay measurements](replay.md#measurements) use.

## Replay server oracle

The Go `swath-replay` ([replay.md](replay.md)) is held to the Java one by `bench/replaydiff`: both servers on one
fixture, the same scripted matrix, token walks and seeded random requests to each, and every answer's status,
compared headers and body bytes required equal ([internals/replay.md](internals/replay.md#the-differential-driver);
[internals/replay-java.md](internals/replay-java.md) lists the documented divergences and residuals):

```bash
go build -o /tmp/replaydiff ./bench/replaydiff && ./build.sh -o swath-replay ./cmd/swath-replay
/tmp/replaydiff -java "$(command -v swath-replay)" -go ./swath-replay -fixture /data/sorted -bucket bkt \
  -report /tmp/diff.json
```

Against the providers themselves the check is a recording: the live matrix (provider cases, walks, altered and
invented tokens, the scripted matrix) is sent once through `bench/faultproxy -record` with `replaydiff
-live-record`, and `replaydiff -capture` replays the recording offline as often as needed, judging each answer by
the live comparison and its documented differences ([replay.md](replay.md#the-recorded-live-matrix)). A live run
is only how a recording is made. Recordings of real buckets stay local and are never committed; the tests CI
runs record fake providers in-process (`bench/replaydiff` `TestLiveMatrixRecordedReplaysOffline`,
`TestPinnedDifferencesAreDocumentedInBoth`; `bench/faultproxy` `TestRecordAnnotations`), and the provider
evidence they replay is the owned-bucket archive committed under `docs/`.

## The fake bucket

`bench/s3fake` serves a gofakes3 bucket loaded from a key list:

```bash
go run ./bench/s3fake -keys keys.tsv -bucket bench -addr 127.0.0.1:9000 &   # TSV: key, mtime (µs)
./swath list s3://bench -endpoint http://127.0.0.1:9000 -path-style -o /tmp/ds
```

Fake-bucket numbers are synthetic:
they measure client CPU and coordination, not S3 latency.

**Test fixtures.** Tests build their buckets from two shared packages: `internal/source/s3/s3test` (a
gofakes3 bucket `bkt` whose bodies are the keys: `Backend` populates, `Handler` serves, `Delayed` adds
latency, `Serve` owns the server's lifecycle, `NoCredentials` hides the host's AWS credentials) and
`internal/source/gcs/gcstest` (`Strict`, the objects.list fake with opaque page tokens bound to their query,
empty and short pages and a fields selector, which the GCS provider's protocol tests run against; `Bucket`
and `Handler` serve it for run-level tests). A test that injects faults wraps the handler; protocol oracles
and malformed-response handlers stay in their tests. `keyrangetest` (engine laws), `scalefake` (lazy large
keyspaces) and `faulttest` (transport disturbance) keep their own purposes.

## Fault injection

`internal/source/objhttp/faulttest` is a fault-injecting HTTP proxy for tests and benchmarks (never linked
into the binary). In front of swath-replay or a fake bucket it answers a share of the requests in a time
window, or a range of request numbers (what a test that must see its faults on a loaded host uses), with the throttle answer (503 `SlowDown` for S3, 429 `rateLimitExceeded` for GCS), delays them, or
resets their connection; a rate cap throttles requests beyond N per trailing second. Request n of a run gets
the same faults for the same seed and plan. It records a timeline (100 ms buckets: arrivals, faults, most
requests in flight). A request counts as in flight from its arrival until the proxy releases its answer, so a
client that sends its next request on the previous answer is never seen with two; answers are forwarded
whole, not streamed, and `Plan.MaxBody` bounds what is buffered: a longer upstream body is answered `502` with
`X-Faulttest-Body-Limit` (`TestMaxBodyBoundsTheBuffer`). `bench/faultproxy` serves it:

```bash
go run ./bench/faultproxy -target http://127.0.0.1:19090 -listen 127.0.0.1:19091 \
  -window throttle:10s-25s:0.5 -window latency:0s-:0.2:300ms -timeline storm.tsv &
./swath list s3://BUCKET -endpoint http://127.0.0.1:19091 -path-style -anonymous -o /tmp/ds
kill %1   # writes storm.tsv and prints the totals
```

## verify.py

`python3 verify.py OUT` checks a format-2 dataset with DuckDB, independently of the Go code: the manifest's
fields and coverage, no `_SUCCESS` or `symlink.txt`, `data/` holds exactly the manifest's parts, row counts,
strictly ascending raw key bytes within and across parts, exact manifest bounds (`min_key_base64`/`max_key_base64`, decoded), the four footer
stamps and no others, and for S3 datasets every column of the current-object schema.

## Fuzzing

```bash
go test -run XXX -fuzz FuzzParseDirents ./internal/source/localfs         # getdents record parser
go test -run XXX -fuzz FuzzParseList -fuzztime 2m ./internal/source/s3/client
go test -run XXX -fuzz FuzzParseFields -fuzztime 1m ./internal/source/s3/client
go test -run XXX -fuzz FuzzPropose -fuzztime 1m ./internal/source/keyrange   # pivot policy
```

Failing inputs land in `testdata/fuzz/` and run as regular tests from then on.

### Replay server targets

Each target seeds from the goldens and fixtures of its package, runs its seeds in a plain `go test`, and
states a property that holds for every input. `-fuzz` takes one target per run (a regular expression that
matches one name, from inside `go/`):

```bash
nice go test ./internal/replay/s3 -run XXX -fuzz '^FuzzToken$' -fuzztime 2m
```

| Target | Property |
|---|---|
| `s3` `FuzzToken` | `DecodeToken` never panics and answers a non-token with the one 400 `InvalidArgument`; an accepted string is the only spelling of its boundary; a boundary round-trips; a token with a character replaced, a decoded byte changed or padding added is refused (CRC-32 catches every error burst of 32 bits or fewer); one with one or four characters dropped or one appended is refused or is the only spelling of another boundary (the seed `four-dropped-is-a-token` is such a pair) |
| `s3` `FuzzParseRequest` | `ParseRequest` never panics; every refusal is a 400 `InvalidArgument`; an accepted request has a page of 0 to 1,000, UTF-8 bounds with no NUL where one is refused, and a token that decodes |
| `s3` `FuzzListingText` | any key, prefix and delimiter render to a listing `encoding/xml` parses; with `encoding-type=url` each echoed value decodes back to its bytes by S3's table (`+` a space); without it each reads back as the key's text with malformed UTF-8 replaced, wherever that text is representable in XML 1.0 (no CR, which the parser folds, and no control character, which S3 writes as a character reference) |
| `s3` `FuzzErrorText` | an error document carrying any value parses and reads back as the value, under the same representability |
| `s3` `FuzzRowFastPathMatchesUnits` | `TestRowFastPathMatchesUnits` for any key and text values, chunk size and cap: the same bytes, refusal, chunks and percent-encoding counts |
| `output` `FuzzEscaped`, `FuzzPercentEncoded` | `XMLEntity`'s escaping and the percent encoder write the same bytes at every chunk edge as the byte-by-byte definition; the percent encoding decodes back to the input |
| `gcs` `FuzzToken`, `FuzzTokenScope` | `decodeToken` never panics and refuses with `errToken`; an accepted token re-encodes to one that decodes alike; a round trip keeps the progress byte; another binding, or any altered character of the binding, is refused; a token is accepted exactly under the scope and fixture it was issued for |
| `gcs` `FuzzParse` | never panics; every refusal is a `*Failure`; an accepted request has a page of 1 to 1,000 and UTF-8 key parameters of at most 1,024 bytes |
| `azure` `FuzzMarker`, `FuzzMarkerScope` | the same properties for `az1.` markers, with the scope's fields (prefix, delimiter, `startFrom`, fixture identity, service version) each binding |
| `azure` `FuzzParse` | never panics; every refusal is an `*Error` with a status and code; accepted values are within the bounds |
| `azure` `FuzzCompareIsUTF16Order` | `Compare` is the comparison of UTF-16 code units for any two valid UTF-8 names, and a total order on any bytes |
| `azure` `FuzzListingOrder` | a walk over any valid names (flat or delimited, any prefix, `startFrom` and page size, native or row rollup) equals the brute-force UTF-16 listing |
| `azure` `FuzzListingXML` | a listing of any name and echoed values parses with `encoding/xml` and reads back as the input, a name sent `Encoded="true"` after percent-decoding; a name the route refuses is a `FixtureIncompatible` error |
## Durable spill campaign (opt-in)

The durable spill's crash tests above run on a 30,000-key fake bucket. The campaign repeats their schedule on
a real S3-compatible backend and on production key lists, through the real binary (`cmd/swath`
`durable_campaign_test.go`). For each fixture a reference run is listed first, and then a cold run with a
slow frontier: a page below the key at 10 % of the list waits 200 ms more, so the ranges ahead of it spill
and seal certificates. Each arm then kills a run with `SWATH_TEST_KILL_AT` at one of the following,
or with SIGKILL once a given share of the keys has been served:

- a durable file's capture frames, its seal, the ledger append, the ledger sync, and the first unlink;
- after the second part record;
- mid recovery replay (killed at the third ledger sync, then the resume killed at its second replayed
  frame);
- unsynced state lost, either unrecorded durable files cut short after a kill at a seal, or the ledger's
  last append torn after a kill at its sync;
- 40 % and 70 % of the keys served.

The arm then resumes. Each schedule runs with `--part-interval=0` and again with a 1 s interval. A kill counts
only as the fault that was asked for: a hook kill must leave its marker on stderr, and a key-count kill must
be the harness's. A run that the 20-minute watchdog ends fails (`TestCampaignRunnerFaults`). Every arm asserts
the following:

- The dataset is byte-identical to the reference (row-identical, every column, with the interval on).
- Rows reconcile: retained plus listed rows equal the dataset, and the objects emitted, less the filtered
  rows, plus the replayed rows, equal the rows listed.
- No certificate is rejected.
- No data request of any resume starts in `[max(cs, K), ce)` of a certificate that is live in the ledger's
  durable prefix when that resume starts. Every resume is checked: the killed first resume of the mid-replay
  arm, and each try and the final resume of the suspect-composition arm. A resume that made requests must
  have logged some.
- The reuse arms are the unlink, mid-replay, 40 % and 70 % kills, all of which fall after seals. Each needs
  certificates above K, and the resume must accept and replay some. The kills before the first seal are not
  asked to reuse.

One more arm, `suspect-composition`, runs at a 3 s interval. This is §6's "time cuts on" row: the composition is
forced, and the reproduction is checked byte for byte.

1. A run with serialized seals is killed at its sixth ledger sync, so five certificates are durable. Its parts
   are 64 MiB, so the interval closes them.
2. Copies of that output are resumed, without the slow frontier, and killed at the n-th journal append,
   after the part's claim and before its record. At 1 s, or behind the frontier, MinIO's time-closed parts
   held one certificate or one gap, never both. This repeats until the claimed part holds rows from two
   certificates' intervals and rows listed live between them.
3. The next resume must take those rows from replay. It rejects no certificate, accepts every certificate live
   above K (the part's among them), and replays at least the part's certificate rows. With reuse off, the arm
   fails.
4. That resume must also reproduce the claimed file at its ordinal byte for byte. This is the suspect-part
   invariant, claimed-file reproduction. Identity with the cold run's parts is the interval-0 arms' assertion,
   whose parts are cut alike.

The reference must list exactly the key file the fixture serves (`SWATH_CAMPAIGN_KEYS`, required), which is an
oracle independent of swath. Both tiers skip without their endpoint, so CI never runs them. The retained
results ([measurements.md](measurements.md#durable-spill)) reproduce with:

```bash
# MinIO in Docker, 250,000 keys (up, the test, then down, also on failure)
head -n 250000 noaa-nbm-grib2-pds.keys > nbm250k.keys
RESULTS=durable-minio.tsv bench/minio.sh campaign nbm250k.keys

# three production lists, each list's first 2,000,000 keys behind the private key-list fake (keyfake) at 5 ms
KEYFAKE=/path/to/keyfake MAX_KEYS=2000000 SLICE_DIR=/dev/shm RESULTS=durable-corpus.tsv \
  bench/durable_corpus.sh real-changesets.keys noaa-nbm-grib2-pds.keys argoverse-6M.keys
```

**What covers the contract's test matrix.** The durable spill contract's §6 rows, and the tier that runs each
one. "Campaign" is this tier; the in-process tests run on the 30,000-key fake and in `order` unit harnesses.

| §6 row | Covered by |
|---|---|
| kill matrix (§3.4-5, §3.8, after a part record, mid recovery replay) | campaign (all four fixtures); `listing` `TestDurableKillMatrix` (1st and 4th occurrences); `cmd/swath` `TestSIGKILLDurableSpill` (its recovery arms start from an unpublished prefix, K at ⊥, so they do not exercise recovery with K advanced; the campaign's recovery arms do) |
| ledger damage (flipped byte, foreign header) | `order` `TestDurableLedgerPrefix` |
| Sol 1 (reordered persistence, crash again in recovery) | `order` `TestDurableLedgerPrefix` (reordered); `listing` `TestDurableRepeatedRecovery` |
| Sol 2 (shutdown order) | `listing` `TestDurableShutdownOrder` |
| Sol 3 (ledger failure, eviction, credit) | `order` `TestDurableLedgerFailureEvicts`; `listing` `TestDurableLedgerTornWrite` |
| replaced output | `order` `TestDurableReplacedOutput`, `TestDurableReplacedMidCycle` |
| credit path | `order` `TestDurableCreditWithoutOrderLock` |
| new ancestors lost | `listing` `TestDurableNewAncestorsLost` |
| lock order | `order` `ledgerOrder` hook, in `TestDurableCertifiesExactRows`, `TestDurableCreditWithoutOrderLock`, `TestDurableLedgerFailureEvicts` |
| seal errors | `order` `TestDurableSealErrorStopsRun` |
| slots full | `order` `TestDurableSlotsFull` |
| history race | `order` `TestDurableHistoryRace` |
| retire defer | `order` `TestDurableRetireDefer` |
| repeated recovery | `listing` `TestDurableRepeatedRecovery` (each recovery step, on its own copy); not at every cleanup step twice |
| history across attempts | `order` `TestDurableHistoryAcrossAttempts`, `TestDurableRecoveryHistory`, `TestDurableReducedCap` |
| reuse (kill at 40 %, skewed fixture) | campaign: kills at 40 % and 70 %, slow frontier, request log; `keyrange` `TestReuseTilesAroundSpans` |
| pre-capture | `order` `TestDurableZeroRowCertificate`, `TestDurableReplayAboveK` |
| abandon | `order` `TestDurableAbandonThenSpill` |
| fence vs chunks | `order` `TestDurableCertifiesExactRows` (pages of 1, 999, 1,025, 3,000) |
| filtered | `order` `TestDurableZeroRowCertificate`; a long wholly filtered range: not covered |
| unsafe K, then crash again | `keyrange` `TestFencesHoldTheContract` (unsafe resume keys, no crash); crash again after an unsafe K: not covered |
| corruption | `order` `TestDurableCorruption`, `TestDurableFrameClaims` |
| foreign | `order` `TestDurableCorruption` (another run's file renamed into a recorded ID) |
| compression | `listing` `TestDurableCompressionFlip` |
| versions | `listing` `TestDurableVersions` |
| exclusion | `listing` `TestDurableOverwrite`; a second process: the output lock's tests, not durable-specific |
| changed source | `listing` `TestDurableChangedSource` |
| time cuts on (suspect part over two replay files and a gap) | campaign `suspect-composition` (3 s interval) |
| reduced cap, ENOSPC | `order` `TestDurableReducedCap`; `listing` `TestDurableSealENOSPC`; ENOSPC on a captured frame: not covered |
| eviction | `order` `TestDurableEviction`, `TestDurableLedgerFailureEvicts` |

On the campaign's fixtures, unsynced loss is emulated in two ways only: unrecorded files are cut short after a
kill at a seal, and the ledger's last append is torn after a kill at its sync.

`SWATH_CAMPAIGN_RESULTS` (which both wrappers set) collects one line per arm. The retained results are
in [measurements.md](measurements.md#durable-spill).

## Measurements

The measurements, their configurations and the commands that reproduce them are in
[measurements.md](measurements.md); scripts are in `../bench/`.
