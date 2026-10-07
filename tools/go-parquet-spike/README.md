# Go Parquet writer feasibility spike

This standalone experiment tests unresolved write-side questions in a possible
Go implementation of swath. It is not a production writer or a replacement
listing engine. Go is pinned to 1.26.5 and parquet-go to 0.32.0, matching the
Varve reader that motivated the experiment.

Scope was discussed with Claude Opus before implementation. The chosen boundary
is one completed Parquet part: real schema, page/index geometry, cross-reader
compatibility, encoding costs, and failure propagation through close and sync.
No production swath code changes are required.

## Questions and gates, declared before measurement

| Gate | Required evidence | Interpretation of failure |
| --- | --- | --- |
| Layout | All 13 canonical columns and null semantics; microseconds/UTC annotation; sorted footer stamps; page rows and row-group cuts; valid offset/index bounds | A file that merely opens is insufficient. Record public-API control gaps; do not patch library internals. Schema comparison treats absent and explicit full natural bit widths as equivalent, reporting Java object-equality separately. |
| Interoperability | Identical ordered rows through Java, Go, and DuckDB; exercise actual Java indexed key cursor in addition to a full scan | Record serving rejection separately from file corruption. Do not silently relax Java's guard. |
| Encoding | Identical preloaded rows, one encoder, output sizes by column, separate write and close clocks, first and repeated iterations | Measurements characterize this corpus. No arbitrary throughput ratio is a production acceptance criterion. |
| Part durability | Footer completion before file sync/close and parent sync; no successful metadata on injected failures | Does not establish dataset publication, SQLite durability, recovery, or power-loss safety. |

Known questions to exercise include single-page `UNORDERED` indexes, equal
truncated bounds, UTF-8/code-point truncation differences, dictionary fallback,
nullable zero/false/empty values, and a final short row group. The page-row cap
uses public column flush APIs. Go's explicit row-group row limit is not claimed
equivalent to Java's byte target. Compression uses klauspost `SpeedDefault`,
approximately libzstd level 3, not identical compression behavior.

No wrapper rewrites footer bytes or changes the reader to make a test pass.
Encoding alternatives are declared (`dict` and `delta` keys); results retain both,
including regressions. No language-wide performance conclusion follows from this
writer-only measurement.

After the baseline measurements, a separate exploratory follow-up was added:
keep delta keys, change INT64 columns from dictionary to delta encoding, then
independently disable duplicate data-page-header statistics if inspection confirms
they explain the long-key overhead. ColumnIndexes and footer statistics must
remain present. These are explicitly post-baseline policies, not replacements
for the original measurements. Their corpus hashes must match the baseline.

## Corpus and measurement plan

Generate one canonical JSONL corpus and give the same bytes to both writers.
Small correctness cases cover empty, one-row, multi-page, long/Unicode keys,
optional fields, and version/delete-marker schema rows. Mixed/version fixtures
check the file contract; they do not claim supported OBJECT-only replay serving.

Two diagnostic throughput corpora each contain 200,000 deterministic OBJECT rows:
short numeric keys with repeated metadata, and a `wide` sensitivity case with
long keys and unique ETags. Both are synthetic; neither represents the full range
of production buckets. This second corpus was added before benchmark measurement
to avoid judging codecs only on low-cardinality data. Read and
materialize the input before each writer's timing loop. Run six iterations in one
process: report iteration zero separately and the median of iterations 1–5.
Use one writer and codec concurrency one, with `GOMAXPROCS=2` and Java
`-XX:ActiveProcessorCount=2`; this is a CPU-availability control, not proof of
identical internal concurrency. Record host/runtime versions. Compare per-column
compressed bytes and actual row-group/page geometry alongside latency.

File writes and syncs use ordinary local storage. Close/sync timings are reported
as implemented; no tmpfs result is presented as durable-output speed. Process RSS
includes preloaded rows and runtime state, and must not be called per-writer
buffer memory. Repeated files may benefit from OS caching. Build/Java source
compilation is outside timed benchmark execution.

## Running

Use a JDK 25 executable explicitly if the shell default differs. Build the actual
Java reference first:

```bash
./gradlew :swath-cli:shadowJar -PnoIntegration
```

The Go module and Java helper live here so the probe can be rerun against future
library revisions. `SwathParity.java` uses the current swath writer and reader;
the DuckDB verifier is independent of both implementations.

Generated corpora, Parquet files, binaries/classes, and receipts belong outside
the repository. The original evidence root is
`/workspaces/swath-data/go-parquet-spike-20261007`, registered in Varve as a
benchmark. Temporary build and review artifacts belong under `/tmp/`.

From the repository root, run the baseline matrix or benchmark into a new
directory (the runner refuses to reuse a directory):

```bash
python3 tools/go-parquet-spike/run_probe.py \
  --java-home /path/to/jdk25 --output-dir /absolute/evidence/correctness
python3 tools/go-parquet-spike/run_probe.py --bench --profile wide \
  --java-home /path/to/jdk25 --output-dir /absolute/evidence/bench-wide
```

Use `--profile objects` for the short-key corpus. The exploratory policy is
reproducible with `--integer-encoding delta --page-statistics false
--key-encoding delta`; `--go-only` omits a new Java benchmark while retaining
Java verification. Preserve the separately recorded Java reference and compare
input hashes when using it. The runner captures linked versions, source hashes,
commands, full verification reports and process timings.

Module checks are separate from Gradle:

```bash
cd tools/go-parquet-spike
go test ./... -count=1
go vet ./...
CGO_ENABLED=1 go test -race ./... -count=1
```

## Observed outcome (2026-10-07)

All 27 baseline files (nine corpora, Java plus two Go variants) passed full-row,
null, schema and index checks in Java, Go and DuckDB. Six Go files retained a
known Java indexed-serving incompatibility: a one-page row group marked
`UNORDERED`. Eighteen files in the adjusted-policy matrix also passed file checks,
with the same three Go fixture shapes rejected for serving. No production reader
guard was relaxed. Tests exercise errors through footer writing and local sync;
they do not establish crash recovery.

Delta integer encoding and omission of redundant header statistics reduced most
of the baseline file-size penalty using public APIs. On the two synthetic
200,000-row corpora, the adjusted policy produced files about 3.5%/5.3% larger
than Java and warm median part-write totals about 31%/13% slower. First writer
invocations were cheaper in Go. These figures include durability, exclude input
loading and process startup, and compare different row-group policies. They are
observations, not runtime or isolated-codec rankings. Original arms remain in
`baseline-summary.json`; follow-ups are in `followup-summary.json` under the
evidence root. Java's timings were still improving across repeated iterations.

## Explicitly outside this spike

Work stealing, weighted pipeline backpressure, source SDK behavior, SQLite
checkpoint migration, external spill/merge, remote durable output, and native
GCS listing remain unproved. A synthetic ledger would not validate swath's real
split/commit/publication protocol, so this experiment does not build one.
Similarly, a toy in-memory sort would add little evidence about the existing
bounded external finalizer. Those require a later real vertical slice.
