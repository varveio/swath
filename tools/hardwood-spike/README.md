# Hardwood feasibility harness

This isolated experiment pins released `dev.hardwood` **1.1.0.Beta2**. It does not
change swath's dependencies, module settings, or production code. The standalone
Gradle build uses composite substitution to compile the Apache oracle and baseline
writer against this checkout's actual `swath-core` implementation and runtime.

The candidate source sets have no Hadoop or Apache Parquet dependency. The oracle
is a separate JVM with Apache Parquet 1.15.1 and swath's production dependency
constraints. Its compile-only Hadoop API dependency does not introduce a second,
untrimmed Hadoop runtime.

## Reproduce

Use JDK 25. On the investigation machine, the available installation is
`/home/vscode/.jdks/jdk-25.0.3+9`. Run from the repository root, and choose a fresh
evidence directory outside the checkout:

```bash
export JAVA_HOME=/path/to/jdk-25
export EVIDENCE=/absolute/path/to/kept-evidence
./gradlew -p tools/hardwood-spike writeClasspaths auditDependencies \
  -PevidenceDir="$EVIDENCE/harness"
./gradlew --init-script tools/hardwood-spike/production-audit.init.gradle \
  :swath-core:spikeRuntimeAudit :swath-cli:spikeRuntimeAudit \
  -PevidenceDir="$EVIDENCE/harness"
python3 tools/hardwood-spike/audit.py "$EVIDENCE/harness"
python3 tools/hardwood-spike/src/core/run-core.py \
  --evidence-root "$EVIDENCE" --run-name core-final
```

Each source set also has a `JavaExec` task. For example:

```bash
./gradlew -p tools/hardwood-spike coreRun \
  -PmainClass=io.varve.swath.spike.core.HardwoodCore \
  "-PappArgs=write $EVIDENCE/candidate.parquet canonical"
./gradlew -p tools/hardwood-spike oracleRun \
  -PmainClass=io.varve.swath.spike.core.ApacheOracle \
  "-PappArgs=read $EVIDENCE/candidate.parquet canonical"
./gradlew -p tools/hardwood-spike remoteRun \
  -PmainClass=dev.swath.spike.RemoteProbe \
  "-PappArgs=$EVIDENCE/remote/run-reviewed" \
  '-PjvmArgs=--enable-native-access=ALL-UNNAMED -Xmx256m -XX:MaxDirectMemorySize=256m'
```

The `appArgs` and `jvmArgs` convenience properties split on spaces. For paths
containing spaces, run the emitted classpath with a direct argument-array process
launcher, as the Python runners do.

After the native build below, run the same core runner with a fresh run name and
`--native` to include independent decoding of the native-produced files.

## Dependencies

`auditDependencies` records coordinates, bytes, SHA-256 and resolved cache paths,
and retains the three published source JARs and POMs. `audit.py` inspects every
resolved class for Hadoop package references, lists Hadoop class definitions,
and records bundled native libraries and GraalVM metadata. This is a static
linkage audit, not a general vulnerability or license review.

The plain core has no mandatory third-party runtime JAR. ZSTD, Snappy, LZ4 and
Brotli are optional codec libraries. GZIP and uncompressed data use the JDK.
The all-codecs audit includes Brotli's separate Linux aarch64 native companion;
other platforms require their corresponding companion. These optional codecs
were audited as dependencies; the functional/native probes cover ZSTD and
uncompressed data, not every optional codec.

The `hardwood-s3` closure is core plus its small HTTP/SigV4 adapter and has no AWS
SDK dependency. The optional `hardwood-aws-auth` closure brings AWS SDK auth,
profiles, regions and related modules at **2.42.17**. swath currently pins a
different SDK version, so adopting that adapter needs deliberate version
alignment. That closure is distinct from core+S3 and does not include ZSTD unless
the application adds it.

**ZSTD is not compatible with swath's existing 1.5.6-9 runtime:** writing succeeds,
but Hardwood reading raises `NoSuchMethodError` for
`Zstd.decompress(byte[], ByteBuffer)`. The spike uses the published optional
version **1.5.7-9**, and forces it in both performance arms. Production remains
unchanged. The failed old-codec smoke is retained as evidence.

The production audit records actual `swath-core` and CLI runtime closures. Those
whole-runtime sizes include SQLite, observability and other unrelated features;
they are context, not a claim that replacing Parquet removes every byte of the
difference. The isolated oracle closure is also not the shipped CLI.

Measured resolved runtime dependency JARs (not shaded distribution sizes):

| Closure | JARs | Bytes |
| --- | ---: | ---: |
| Hardwood core | 1 | 1,317,656 |
| Core + ZSTD | 2 | 8,887,435 |
| Core + S3 + ZSTD | 3 | 8,915,748 |
| Core + all optional codecs, Linux aarch64 | 7 | 12,720,806 |
| Core + S3 + optional AWS auth, without codecs | 15 | 4,665,892 |
| Current swath-core runtime dependencies | 60 | 64,658,930 |
| Current CLI runtime dependencies | 96 | 76,217,354 |

Every audited candidate closure has zero Hadoop class definitions and zero
Hadoop class-name references. SHA-256 values for each artifact are in the TSVs.

## Encoding, performance and memory

`EncodingProfiles` uses the released writer API. The `auto` profile leaves every
column at AUTO; in Beta2 that policy chooses plain or dictionary encoding. The
`delta` profile explicitly selects `DELTA_BYTE_ARRAY` for key, etag and version_id,
and `DELTA_BINARY_PACKED` for size and last_modified. Other columns remain AUTO.
This is configuration, with no library patch or production migration.

Two independent corpus manifests define the expected thirteen-field row stream:

- `legacy` preserves the original synthetic corpus, including its unsorted hex
  keys and linear size/timestamp values. It deliberately retains the case that
  made the original AUTO file-size difference look especially large.
- `irregular` is a deterministic, realistic-shaped synthetic stress corpus. It
  has nonlinear sizes, shuffled timestamps, high-entropy etags, nullable metadata,
  shared-prefix keys, embedded controls, supplementary Unicode and long keys. Its
  completed keys are strictly sorted by unsigned UTF-8 bytes. It is not a sampled
  bucket and establishes no claim about production workload distributions.

`CorpusManifest` establishes the expected values, typed length-delimited SHA-256,
key length, null counts and key ordering without reading any output file. Each
writer hashes its input. Each independent reader checks every decoded field and
row count, hashes the decoded stream, and checks actual key order for `irregular`.
The driver requires every hash to equal the separate manifest. No corpus-sized
row list is retained.

To reproduce the matched encoding comparison in a fresh directory:

```bash
./gradlew -p tools/hardwood-spike writeClasspaths \
  -PevidenceDir="$EVIDENCE/iteration-2/encoding"
for corpus in legacy irregular; do
  if [ "$corpus" = legacy ]; then
    apache_target=2097152
    hardwood_rows=14000
  else
    apache_target=2359296
    hardwood_rows=16300
  fi
  for profile in delta auto; do
    python3 tools/hardwood-spike/bench.py --phase performance \
      --corpus "$corpus" --encoding-profile "$profile" \
      --apache-target "$apache_target" --hardwood-row-group-rows "$hardwood_rows" \
      --evidence "$EVIDENCE/iteration-2/bench/$corpus/$profile" \
      --classpath-dir "$EVIDENCE/iteration-2/encoding"
  done
done
python3 tools/hardwood-spike/encoding-report.py "$EVIDENCE/iteration-2"
```

Each corpus/profile runs 100,000 and 1,000,000 rows, three fresh JVMs per writer at
each size, and both readers on every output: 144 successful processes in total.
The process limit is three minutes. `run-provenance.json` captures exact commands,
source hashes, runtime class/JAR hashes and the starting commit. The driver
refuses to append to an existing phase. `comparison-summary.json` checks all
144 receipts, hashes, logical row counts and unchanged candidate row-group
boundaries between AUTO and delta, then records timings, file hashes and each
column's compressed/uncompressed bytes, encodings and pages.

The measured swath module archives carry production commit `69cb809`; the spike
commit changes their manifest stamp. A scoped rebuild reproduced the original
measured JAR hashes exactly and proved that only `META-INF/MANIFEST.MF` differs
from the current archives: every class/resource entry is identical. The receipt
is `iteration-2/encoding/artifact-provenance-verification.json`.

These are **cold fresh-JVM runs without an in-process warmup**, with
`-Xms32m -Xmx256m`. Engine order alternates within each three-fork set. Operation
time includes setup, class loading, JIT activity, generation or complete read
validation, logical SHA-256 and footer close. Hashing was added for iteration 2;
**timings cannot be compared directly to iteration 1**. `/usr/bin/time` records
process wall time and maximum RSS. A 10 ms heap sampler can miss short peaks.
Reads immediately follow writes, so OS cache is warm. The shared host is not an
isolated performance laboratory.

Both writers use ZSTD level 3 with zstd-jni 1.5.7-9, 1 MiB page targets,
20,000-row page targets, 1,024-byte statistics truncation and streaming MD5.
**Neither measured path fsyncs the file or directory.** Apache uses the actual
canonical `ListEntryParquetWriters.buildTracked` and `ListEntryWriteSupport`.
Its primary reader uses `LocalInputFile`, row-group pages and
`GroupRecordConverter`, the same physical path used by
[ParquetEntryReader](../../swath-core/src/main/java/io/varve/swath/output/parquet/fixture/ParquetEntryReader.java)
and the sorted reader. It preserves raw physical nulls; it does not include
production model construction. Apache writer mapping constructs `ObjectEntry`;
Hardwood uses its row builder. Apache's served dictionary budget is 8 KiB;
Hardwood exposes no identical cap. Apache receipts correctly label their policy
`production`; the requested profile only changes Hardwood.

Byte thresholds do not denote the same memory/encoding accounting. Geometry was
calibrated before timing, using row counts rather than a favorable timing result:
legacy uses Apache 2 MiB / Hardwood 14,000 rows, while irregular uses Apache
2.25 MiB / Hardwood 16,300 rows. Hardwood retains an 8 MiB buffer target in both.
AUTO and delta use identical settings within a corpus. At one million rows,
legacy has Apache 71 groups / 923 column pages versus Hardwood 72 / 936;
irregular has 62 / 867 in both arms. Candidate AUTO and delta boundaries match
exactly. These are approximate engine-to-engine geometry controls.

Confirmed file sizes at one million rows:

| Corpus | Hardwood AUTO | Hardwood delta | Apache production |
| --- | ---: | ---: | ---: |
| Legacy | 12,525,793 | 4,357,074 | 4,771,155 |
| Irregular | 49,882,386 | 45,416,941 | 46,344,197 |

The public encoding profile removes the initial file-size objection: delta cuts
legacy AUTO by 65.22% and irregular AUTO by 8.95%. The effect is corpus dependent.
On the linear legacy values, compressed size/last_modified columns drop from
3,036,653 / 2,051,405 bytes to 4,947 / 5,036. On irregular sizes, delta instead
**increases** that one column from 3,744,743 to 4,040,457 bytes (+7.9%), while other
columns make the whole file smaller. No adaptive encoding heuristic is inferred
from these two corpora.

Observed median operation seconds over three fresh JVMs, including hashing and
value validation:

| Corpus | Hardwood AUTO write | Hardwood delta write | Apache write in delta comparison |
| --- | ---: | ---: | ---: |
| Legacy | 1.103 | 0.977 | 1.408 |
| Irregular | 1.696 | 1.489 | 2.101 |

| Read the same Apache-produced file | Hardwood delta comparison | Apache physical decoder |
| --- | ---: | ---: |
| Legacy | 1.010 | 1.352 |
| Irregular | 1.257 | 1.629 |

These are bounded observations, not a general throughput claim. Default Hardwood
reader contexts used all 18 available processors. On irregular delta's own file,
median maximum RSS was about 368 MiB for Hardwood versus 212 MiB for Apache on
its own output, despite the 256 MiB Java heap cap. Heap and process RSS differ.

A secondary comparator invokes the actual `ParquetEntryReader`, including typed
model construction, on the valid object corpus:

```bash
python3 tools/hardwood-spike/bench.py --phase production-reader \
  --corpus irregular --encoding-profile delta \
  --evidence "$EVIDENCE/iteration-2/bench/irregular/delta/production-reader" \
  --classpath-dir "$EVIDENCE/iteration-2/encoding"
```

All six secondary processes pass complete field/hash checks. Medians were
1.719 seconds on Hardwood delta files and 1.741 seconds on Apache files. The
primary physical comparator remains separate so its costs and null semantics
stay explicit.

For memory diagnosis, `--phase stress-fixture --corpus CORPUS --encoding-profile
delta` produces a one-million-row, one-group candidate file with a 512 MiB writer
heap and 1 GiB group target, then independently decodes it with Apache. This is
fixture generation, not a recommended writer configuration. Small-group and
one-group files are retained for `memory.py`. The original 64 MiB/default-reader
failures remain evidence; default concurrency is not an admission policy, and
passing a matrix is not a proof for arbitrary schema widths, malformed input,
concurrent readers or keys. See the separate memory probe's recorded settings
and results for the row API versus explicitly sized column batches.

For example, generate an independently validated stress file, then bracket row
and explicit column-batch memory on immutable files:

```bash
python3 tools/hardwood-spike/bench.py --phase stress-fixture \
  --corpus legacy --encoding-profile delta \
  --evidence "$EVIDENCE/iteration-2/encoding/stress-legacy-delta" \
  --classpath-dir "$EVIDENCE/iteration-2/encoding"
python3 tools/hardwood-spike/memory.py --phase bracket --corpus legacy \
  --small "$EVIDENCE/iteration-2/bench/legacy/delta/hardwood-1000000-0.parquet" \
  --stress "$EVIDENCE/iteration-2/encoding/stress-legacy-delta/hardwood-onegroup.parquet" \
  --classpath-file "$EVIDENCE/iteration-2/encoding/hardwoodBench.classpath" \
  --evidence "$EVIDENCE/iteration-2/memory/legacy-bracket"
python3 tools/hardwood-spike/memory.py --phase warm --warm-heap 64 --warm-api column \
  --corpus legacy \
  --small "$EVIDENCE/iteration-2/bench/legacy/delta/hardwood-1000000-0.parquet" \
  --stress "$EVIDENCE/iteration-2/encoding/stress-legacy-delta/hardwood-onegroup.parquet" \
  --classpath-file "$EVIDENCE/iteration-2/encoding/hardwoodBench.classpath" \
  --evidence "$EVIDENCE/iteration-2/memory/legacy-selected-warm"
```

The selected public configuration is `HardwoodContext.create(1)` plus
`ColumnReadersBuilder.batchSize(1024)`, materializing and validating all thirteen
fields. At a 64 MiB heap it passed both corpora with both small groups and a
single million-row group, including one warmup and two measured passes per file.
The row API at that heap failed on legacy's one-group file and both irregular
files. Explicit column batch sizing improves the tested heap admission envelope;
it does not make process RSS uniformly lower, and it is not a proof for arbitrary
inputs or concurrent readers. The memory report retains the failures and limits.

## Native feasibility

`NativeProbe` writes and reads the complete thirteen-field, 1,045-row canonical
fixture, including null patterns, raw UTF-8, signed microsecond timestamps and
multiple row groups. Run with a GraalVM 25 installation on Linux aarch64:

```bash
python3 tools/hardwood-spike/native.py --graal-home /path/to/graalvm-25 \
  --classpath-dir "$EVIDENCE/iteration-2/encoding" \
  --evidence "$EVIDENCE/iteration-2/encoding/native" --profiles auto delta
python3 tools/hardwood-spike/src/core/run-core.py \
  --evidence-root "$EVIDENCE/iteration-2" --run-name delta-with-native --profile delta \
  --classpath-root "$EVIDENCE/iteration-2/encoding" \
  --native-root "$EVIDENCE/iteration-2/encoding/native" --native
```

The bounded compilation is `--no-fallback`, a 4 GiB compiler heap, two compiler
threads, and a 180-second deadline. It explicitly includes
`-H:IncludeResources=linux/aarch64/libzstd-jni.*`. The first plain build proved an
uncompressed roundtrip but failed to load ZSTD's native library; the resource
include made both roundtrips pass. Oracle GraalVM 25.0.4+7.1 completed that build
in 39.5 seconds with 2.04 GB reported peak RSS in iteration 2. The same native
binary passes AUTO and delta with both codecs: four complete native roundtrips
and four independent Apache validations of values, actual encodings and codecs.

`native.py` also runs the independent oracle, emits `native-geometry.jsonl`
through the Apache geometry reader, and records compiler version and binary
SHA-256. `bench.py` automatically writes `geometry.jsonl` for performance,
calibration and smoke runs. The standalone equivalent is:

```bash
"$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED \
  -cp "$(cat "$EVIDENCE/iteration-2/encoding/apacheBench.classpath")" \
  io.varve.swath.spike.bench.Geometry \
  "$EVIDENCE/iteration-2/encoding/native/native-resource-delta-UNCOMPRESSED.parquet" \
  "$EVIDENCE/iteration-2/encoding/native/native-resource-delta-ZSTD.parquet"
```

This demonstrates a native core+ZSTD artifact on this architecture. It does not
validate swath's complete CLI, S3 authentication, SQLite, signal handling,
cross-platform packaging, or power-loss durability. Native commands, compiler
logs, roundtrip logs, binary SHA-256 and the compiled executable are retained in
the investigation evidence directory.

Recorded investigation outputs live under
`/workspaces/data/swath/hardwood-spike-20261015/{harness,bench,core,remote}`.
Iteration 1 remains under those original roots. Current matched encoding evidence
is under `iteration-2/{encoding,bench}`; its exact corpus, policy, file geometry
and instrumentation must accompany any quoted numbers.
