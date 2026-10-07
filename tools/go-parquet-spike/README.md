# Go Parquet and checkpoint feasibility probes

These standalone experiments assess replacing swath's producer in Go. They use
parquet-go 0.32.0 and Go 1.26.5, the real Java writer/readers, independent DuckDB
checks, and Java-created SQLite checkpoints. They are research tools, not a
replacement listing engine.

The initial one-part experiment has been extended to close its concrete gaps:
compatible single-page indexes, byte-targeted row groups, measured writer
performance, and crash/recovery interoperability. The only production change is a
Java reader fix accepting one-page boundary-order conventions when both indexes
agree there is one page. Decoded-key checks and multi-page rejection remain.

## Current findings

- The canonical 13 fields, nulls, logical annotations, footer stamps and indexes
  round-trip through Java, Go and DuckDB. The public schema adapter preserves bare
  INT64 for `size`; full-width optional type-length metadata is treated as
  semantically equivalent and reported explicitly.
- With delta keys, `-single-page-order ascending` uses a public indexer factory to
  produce files accepted by the unmodified Java reader. This is not footer
  rewriting. Dictionary keys plus this option are refused: their library-private
  type handling prevents the same clean adapter. The Java fix also accepts stock
  Go single-page files without this writer option.
- A public `Size()`-based hybrid buffer target works at page boundaries, with a
  separate row ceiling. One million real rows produced 103 groups at 256 KiB and
  four at 8 MiB, with full cross-reader equality. This is a payload estimate,
  neither exact Java boundaries nor a heap/RSS cap.
- A schema mismatch made `GenericWriter[Row]` use its reflective conversion path.
  Public row-value and column APIs avoid it. All three APIs produce byte-identical
  files under the same settings; the column path preserves the library's 64-row
  write quantum and shares one field mapping with the row-value path.
- The tuned pure-Go policy is broadly competitive with Java in the measured writer
  workloads. An optional native codec is also available, with explicit cgo and
  platform costs. These are preloaded-part writer observations, not whole-listing
  throughput promises or byte-identical Java files.
- Seven real SIGKILL cases interoperate with the production Java checkpoint and
  recovery classes. The publication-only case retains every finalized part and
  performs zero source reads. This covers one root range and a core invocation,
  not CLI restoration or a concurrent Go work-stealing engine.

## Writing and verifying a part

Use an explicit JDK 25 path: the shell's default Java may be older. Build the Java
reference and test fixture support from the repository root:

```bash
./gradlew :swath-cli:shadowJar :swath-core:testFixturesJar -PnoIntegration
```

Run a cross-reader matrix into a **new absolute directory outside this repo**:

```bash
python3 tools/go-parquet-spike/run_probe.py \
  --java-home /path/to/jdk25 --output-dir /absolute/evidence/compatibility \
  --row-api columns --key-encoding delta --integer-encoding delta \
  --etag-encoding delta --page-statistics false \
  --single-page-order ascending --codec go-entropy
```

`--java-jar` selects an older artifact for backward-compatibility checks. Mixed
and version fixtures check the file contract and intentionally skip OBJECT-only
indexed serving. The runner reports those distinctions and serving failures.

The Go executable also accepts `write` and `verify`. Its baseline defaults are
retained to reproduce the initial investigation. Select the measured policy
explicitly, rather than assuming the defaults describe the optimized candidate:

```bash
cd tools/go-parquet-spike
CGO_ENABLED=0 go run . write -input /absolute/input.jsonl \
  -output /absolute/output.parquet -row-api columns -sort-mode objects \
  -key-encoding delta -integer-encoding delta -etag-encoding delta \
  -page-statistics=false -single-page-order ascending -codec go-entropy \
  -page-rows 1024 -group-rows 1000000 -group-bytes 8388608
```

`-group-bytes 0` retains fixed-row grouping. `-sort-mode none` omits sort stamps
for direct parts; `-layout direct` changes page/dictionary/bound settings, not the
run's sort semantics. Explicit `objects`/`versions` describes the run even if a
part happens to contain only one kind of row. `auto` is fixture convenience.

The codec choices are:

| Codec | Implementation | Cost/limit |
| --- | --- | --- |
| `go` | parquet-go's SpeedDefault wrapper | Original baseline; skips entropy coding on some all-literal blocks |
| `go-entropy` | Same pinned klauspost implementation with public all-literal entropy enabled | Pure Go, owned contexts; useful with delta-encoded ETags |
| `native` | gozstd 1.26.0 / ZSTD 1.5.7, reusable level-3 streaming frames | Requires `CGO_ENABLED=1` and `-tags nativezstd`; explicitly releases C resources |

For standalone native execution, use `go run -tags nativezstd . ... -codec
native`. The Python runners add the tag when selected. No codec algorithm,
unsafe application code, library fork, or post-write footer patch is introduced.
Native streaming frames differ from the one-shot JNI codec diagnostic.

## Performance and byte-target measurements

`compare_writers.py` pins both processes to the same two CPUs and compares one
row group over the same input. Default panels have five process pairs with
alternating order, ten warmup writes and ten measured writes per process. It
records actual encodings/geometry, full writer-call wall/CPU, file hashes, every
iteration, and an exploratory process-pair bootstrap interval. Java writer heap
is fixed at 1 GiB; the untimed full-row verifier has a separate larger heap.

```bash
python3 tools/go-parquet-spike/compare_writers.py \
  --input /absolute/corpus.jsonl --output-dir /absolute/evidence/paired \
  --java-home /path/to/jdk25 --jar /absolute/swath.jar \
  --row-api columns --etag-encoding delta --codec go-entropy
```

`--profile` produces separate Go CPU/JFR diagnostics, not timing evidence. Input
loading and file inspection are outside the profiled writer loop; Go validation,
tracking, encoding and durability are inside it. JFR environment and system
property events are disabled. `--go-binary` supports a frozen executable and
records its hash and linked versions.

The initial unpinned, six-iteration results remain as historical evidence. Their
13–31% gap was not a language limit: the schema override selected a reflective
path, Java was still warming, and row-group counts differed. Matching those
controls preceded the API and codec experiments. Post-baseline policies are
identified in the results; no favorable run replaced an unfavorable one.

Byte-group reports identify the trigger, baseline, previous batch, observed
payload and overshoot, then check the actual reopened group rows. “At most the
last batch's growth” is an observed accounting relationship, not a static heap
bound. RSS receipts include preloaded rows and later inspection.

## Checkpoint and codec diagnostics

`CheckpointInterop.java` creates the database with real swath classes and recovers
through `runToParquetWorkStealing`. `run_resume_probe.py` supplies a Java fixture
to the Go checkpoint tests, refuses skipped checks and Python `-O`, waits for an
explicit barrier, sends real SIGKILL, and checks counters, rows, part hashes and
publication. Active datasets live in `/tmp` because recovery deletes orphans;
completed evidence is copied to the requested output directory. Its `--help`
lists the executable/classpath arguments. The classpath needs the CLI fat JAR,
compiled helper, and current swath-core test-fixtures JAR.

The six interrupted-tail/transaction cases use one worker. The seventh has no
pending rows: the actual empty-worklist entrypoint publishes through
`ParquetWriterPool.close`, with zero workers, source reads or relisted rows.
A no-op producer emitting the same readiness message is rejected by independent
generation/page-counter checks. `identity_spec=null` is explicit core-test scope,
not a claim that the CLI would restore arbitrary Go-created run identities.

`codec-probe`, `CodecInterop.java`, and `run_codec_probe.py` compare codecs on
identical encoded page payloads, excluding V2 repetition/definition prefixes.
Contexts and output buffers are reused; decompression and cross-decoding happen
outside the clocks. That isolates codec cost but does not predict whole-writer
speed or native streaming behavior.

## Checks and evidence

```bash
cd tools/go-parquet-spike
CGO_ENABLED=0 go test ./... -count=1
go vet ./...
CGO_ENABLED=1 go test -race ./... -count=1
CGO_ENABLED=1 go test -tags nativezstd ./... -count=1
```

The three checkpoint tests require `SWATH_CHECKPOINT_FIXTURE` from the Java
helper; the resume driver sets it and checks they actually ran. Run the full Java
integration gate with `./gradlew build` when changing the production reader.

Durable evidence is registered in Varve under
`/workspaces/swath-data/go-parquet-spike-20261007`, with continuation results in
`continuation/`. These are local files, not GitHub downloads. They include source
and input hashes, original/final outputs, raw command receipts, profiles, paired
summaries, million-row checks and the seven-case crash matrix. Never put generated
fixtures, binaries, recordings or review output in this repository.

The remaining full-port work includes scanning policy, split/steal concurrency,
weighted backpressure, external sort scheduling, multi-part Go publication and
CLI/operational compatibility. These probes retire specific library and protocol
risks; they do not implement or certify that complete product.
