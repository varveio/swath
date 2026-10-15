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

## Performance and memory

The shared corpus streams one object row at a time. It exercises all thirteen
canonical fields, optional values, UTF-8 keys, repeated metadata, distinct etags,
and occasional keys near S3's maximum length. It retains no corpus-sized list.
Both readers validate every field and row count against independently generated
expected values, including when reading the other writer's output.

```bash
python3 tools/hardwood-spike/bench.py --phase smoke \
  --evidence "$EVIDENCE/bench/smoke" --classpath-dir "$EVIDENCE/harness"
python3 tools/hardwood-spike/bench.py --phase performance \
  --apache-target 2097152 --hardwood-row-group-rows 14000 \
  --evidence "$EVIDENCE/bench/controlled" --classpath-dir "$EVIDENCE/harness"
python3 tools/hardwood-spike/summarize.py "$EVIDENCE/bench/controlled"
python3 tools/hardwood-spike/bench.py --phase memory \
  --evidence "$EVIDENCE/bench" --classpath-dir "$EVIDENCE/harness"
```

Performance runs use 100,000 and 1,000,000 rows, three fresh JVMs per writer at
each size, both readers for every output, `-Xms32m -Xmx256m`, and alternate engine
order. These are **cold fresh-JVM runs without an in-process warmup**. Operation
time includes setup, class loading, JIT activity, corpus generation or complete
read validation, and footer close. `/usr/bin/time` additionally records process
wall time and maximum RSS. Heap sampling every 10 ms is approximate and may miss
short peaks. Files are read immediately after creation; this measures warm OS
cache behavior and does not establish cold-storage throughput. The shared host
is not an isolated performance laboratory.

Both writers use ZSTD's default level 3, the same zstd-jni version, 1 MiB page
targets, 20,000-row page targets, 1,024-byte statistics truncation, and streaming
MD5 over the emitted file. Both finalize a footer. **Neither measured path
performs a file or directory fsync.** The Apache arm calls the current canonical
`ListEntryParquetWriters.buildTracked` with `ListEntryWriteSupport`; the reader
uses the production-style `LocalInputFile` / row-group / `GroupRecordConverter`
path. The candidate uses its row API. Expected end-to-end mapping costs differ:
Apache constructs `ObjectEntry`, while Hardwood accepts a typed row builder.

Equal byte targets do not give equal row groups. An exploratory 8 MiB/8 MiB run
produced one Apache group versus sixteen Hardwood groups at one million rows.
The controlled follow-up instead uses the calibrated Apache 2 MiB target and
Hardwood's 14,000-row limit with an 8 MiB buffer target, producing about seventy
groups in both arms. This aligns geometry approximately, not byte for byte.
Encoding choices still differ: Apache uses delta encodings on keys/integers;
Hardwood AUTO selects plain/dictionary encodings for this corpus. Apache's served
dictionary budget is 8 KiB; Hardwood exposes no identical dictionary cap.
`Geometry` records actual row counts, page totals, uncompressed chunk sizes,
encodings and codecs. File sizes and reader memory must accompany timings.

The bounded-memory phase writes one million rows at 8 and 32 MiB group targets,
with 64 and 128 MiB heaps. Each process has a three-minute deadline. Passing
these cases is empirical evidence only, not a memory proof for arbitrary widths,
schema sizes, parallelism, concurrent files, malformed inputs or larger keys.
The initial default-reader case at an 8 MiB target failed at 64 MiB heap and
passed at 128 MiB; the 32 MiB writer target failed at 64 MiB. Those failures are
retained. Default reader concurrency follows available processors and is not an
application memory admission policy.

The controlled one-million-row comparison produced these medians over three
fresh JVMs; read times include complete value validation:

| Operation | Hardwood | Apache |
| --- | ---: | ---: |
| Write including MD5 and footer, seconds | 0.862 | 1.216 |
| Read Apache-produced file, seconds | 0.711 | 1.120 |
| Read Hardwood-produced file, seconds | 0.729 | 1.056 |
| Produced file, bytes | 12,525,793 | 4,771,155 |
| Row groups / total column pages | 72 / 936 | 71 / 923 |
| Writer maximum RSS, MiB | 154 | 204 |
| Reader RSS on Apache-produced file, MiB | 313 | 198 |
| Reader RSS on Hardwood-produced file, MiB | 325 | 190 |

These observations support further evaluation, while exposing a substantial
file-size and read-memory tradeoff for this corpus. They do not establish a
general speedup. The first exploratory smoke failures are explicitly retained
under `bench/failed-trial/`; they are superseded by `aligned-smoke/` and the
controlled comparison.

A follow-up `HardwoodContext.create(1)` read passed at a 64 MiB heap on the
sixteen-group candidate file, but still exhausted that heap on the one-group
Apache exploratory file. Reducing concurrency helps; decoded row-group size
still matters. This is an adapter sizing requirement, not a proven upper bound.

## Native feasibility

`NativeProbe` writes and reads the complete thirteen-field, 1,045-row canonical
fixture, including null patterns, raw UTF-8, signed microsecond timestamps and
multiple row groups. Run with a GraalVM 25 installation on Linux aarch64:

```bash
python3 tools/hardwood-spike/native.py --graal-home /path/to/graalvm-25 \
  --classpath-dir "$EVIDENCE/harness" --evidence "$EVIDENCE/harness"
python3 tools/hardwood-spike/src/core/run-core.py \
  --evidence-root "$EVIDENCE" --run-name final-reviewed --native
```

The bounded compilation is `--no-fallback`, a 4 GiB compiler heap, two compiler
threads, and a 180-second deadline. It explicitly includes
`-H:IncludeResources=linux/aarch64/libzstd-jni.*`. The first plain build proved an
uncompressed roundtrip but failed to load ZSTD's native library; the resource
include made both roundtrips pass. Oracle GraalVM 25.0.4+7.1 completed that build
in 46.9 seconds with 2.04 GB reported peak RSS on this machine. Apache independently
validated both native-produced files.

`native.py` also runs the independent oracle, emits `native-geometry.jsonl`
through the Apache geometry reader, and records compiler version and binary
SHA-256. `bench.py` automatically writes `geometry.jsonl` for performance,
calibration and smoke runs. The standalone equivalent is:

```bash
"$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED \
  -cp "$(cat "$EVIDENCE/harness/apacheBench.classpath")" \
  io.varve.swath.spike.bench.Geometry \
  "$EVIDENCE/harness/native-resource-UNCOMPRESSED.parquet" \
  "$EVIDENCE/harness/native-resource-ZSTD.parquet"
```

This demonstrates a native core+ZSTD artifact on this architecture. It does not
validate swath's complete CLI, S3 authentication, SQLite, signal handling,
cross-platform packaging, or power-loss durability. Native commands, compiler
logs, roundtrip logs, binary SHA-256 and the compiled executable are retained in
the investigation evidence directory.

Recorded investigation outputs live under
`/workspaces/data/swath/hardwood-spike-20261015/{harness,bench,core,remote}`.
The first performance/memory results at `bench/` are exploratory; the digest-
and geometry-controlled performance results are under `bench/controlled/`.
