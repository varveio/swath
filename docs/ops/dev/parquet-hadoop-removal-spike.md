# Hadoop-free Parquet feasibility spike

This is the 2026-08-31 implementation spike that follows the PR 7 linkability laboratory. It
changes a branch-only candidate, measures it, and records a production decision. It is not a
recommendation to merge the candidate's package-private bridge.

## Verdict

There are three distinct goals:

| Goal | Spike result | Decision |
| --- | --- | --- |
| Ship no Apache Hadoop runtime jars | Works on the JVM for swath's ZSTD writer and low-level reader paths | **GO**, after replacing the spike bridge with a supported upstream API |
| Have no Hadoop dependency in source or at compile time | Stock parquet-java 1.18.0 still exposes Hadoop types from classes swath must compile against | **NO-GO with the stock artifact** |
| Have no Hadoop-linked classes in the Parquet artifact, including for closed-world/native-image analysis | `parquet-hadoop` remains and has 311 class-level missing references to Hadoop | **NO-GO without a new slim upstream artifact or a maintained fork** |

The honest short answer is therefore: swath can stop **shipping Hadoop's jars**, but it cannot yet
say that Hadoop has been removed entirely while it consumes stock `parquet-hadoop` 1.18.0.

The production recommendation is to pursue a small upstream change and a slim artifact boundary.
Do not merge the spike's `org.apache.parquet.SwathReadOptions` split-package bridge. If upstream is
not willing to provide a public plain-configuration read-options path and ultimately a slim module,
retain the current Hadoop dependency rather than adopt that bridge permanently.

## What the candidate proves

The branch candidate uses the published parquet-java 1.18.0 artifacts, not a locally patched jar.
It does the following:

- supplies `PlainParquetConfiguration` to the generic writer path;
- supplies a swath-owned `CompressionCodecFactory` backed directly by zstd-jni for ZSTD level 3 and
  uncompressed pages;
- routes every production low-level reader through one `ParquetFiles.open` boundary;
- uses a version-coupled class in `org.apache.parquet` to reach the package-private
  `ParquetReadOptions` constructor without constructing Hadoop codecs or filters; and
- removes all `org.apache.hadoop` and `org.apache.hadoop.*` coordinates from the runtime graph.

On a classpath containing no Hadoop artifact or `org/apache/hadoop/` class entry, the candidate
successfully exercised:

- the direct canonical writer;
- the sorted final writer and footer stamps;
- footer, index, bounded-range, and row-group readers;
- merge-input `SegmentReader`;
- swath-authored ZSTD data; and
- DuckDB-authored ZSTD data.

The only deliberately blocked probe loads parquet-java's unused Hadoop `ZstandardCodec`, which
fails on `org.apache.hadoop.conf.Configurable`. The Docker-free project build also passes.

## Why the candidate is not the destination

The JVM succeeds because it links classes lazily. The artifact boundary is still Hadoop-shaped:

- `parquet-hadoop` remains a runtime module because it contains `ParquetFileReader` and
  `ParquetWriter`;
- `javac` still needs a non-transitive `compileOnly` `hadoop-common` jar to resolve Hadoop types in
  public class descriptors;
- the writer and write-support base classes still require deprecated Hadoop-typed overrides even
  though the generic overrides do the real work; and
- JDK 25 `jdeps -recursive -verbose:class` reports 311 Hadoop missing-reference lines in
  `parquet-hadoop` 1.18.0.

The read-options workaround is worse than an ordinary internal call. It creates a split package,
depends on a positional package-private constructor, and couples swath to every Parquet default.
The characterization test already caught two initially incorrect defaults: vectored I/O and the
maximum allocation size. The Java module path, a future Parquet release, or native-image analysis
can reject or expose this arrangement even though the classpath JVM accepts it.

The spike codec is deliberately narrow. It reads and writes `ZSTD` and `UNCOMPRESSED`; it rejects
Snappy, Gzip, LZ4, Brotli, and LZO. That covers current swath output and the tested DuckDB fixture,
but it is not a claim that every external or historical Parquet fixture remains readable. A
production migration must either state that narrower contract or add and test the codecs it wants
to support.

Parquet 1.18.0 also requires JTS during ordinary schema/statistics initialization, even when swath
does not use geospatial columns. Excluding JTS caused all ordinary writer and reader probes to fail
on `org.locationtech.jts.io.ParseException`; the final candidate retains the 1,239,978-byte JTS
jar.

## Measured gain and change

The baseline is the PR 7 build at `fad4bd6`; the candidate is this branch on top of `c76f002`, using
Temurin 25.0.4 on Linux x86-64.

| Package | PR 7 bytes | Candidate bytes | Reduction |
| --- | ---: | ---: | ---: |
| CLI fat jar | 75,449,028 | 55,713,872 | 19,735,156 (26.2%) |
| CLI install distribution | 76,454,120 | 57,042,906 | 19,411,214 (25.4%) |
| replay install distribution | 152,627,426 | 131,315,741 | 21,311,685 (14.0%) |

The candidate core runtime graph is 29 artifacts and 43,331,005 bytes, down from 59 artifacts and
64,634,573 bytes. No candidate runtime graph contains an Apache Hadoop coordinate, and the CLI fat
jar contains no Hadoop class entry.

The representative 2,048-row file remains semantically interoperable, but it is not byte-identical:
the baseline writer produced 24,050 bytes with SHA-256
`bd9a7c1981fd653cf6aff04b0cd122a4de99c7c8c11a00e492a7c71ea2efd779`, while the candidate produces
22,381 bytes with SHA-256
`202ec62de55812dd8db834a4d09c40484fc59592251bd697d27902348ad609fe`. The 6.9% size change is not
counted as a performance win. It reflects a new Parquet release and a one-shot zstd-jni codec path,
and requires the normal format, compression, memory, and throughput gates before production use.

## Smallest defensible production path

1. Upgrade to parquet-java 1.18 independently and retain Hadoop while format and replay fidelity
   settle.
2. Upstream a public `ParquetReadOptions` builder path that accepts a `ParquetConfiguration`, an
   explicit codec factory, and an explicit no-op/filter without eagerly invoking Hadoop helpers.
3. Upstream generic writer/write-support hooks that do not force subclasses to implement deprecated
   Hadoop overloads.
4. Remove Hadoop runtime jars only after the zero-Hadoop isolated laboratory, full build, packaging,
   durability, replay, compression, and resource gates pass.
5. If "Hadoop entirely" includes compilation or native image, pursue a slim Parquet file module that
   physically excludes Hadoop-specific readers, codecs, configuration, encryption helpers, and
   descriptors. The small API changes in steps 2 and 3 do not accomplish this artifact split.

A temporary, narrowly rebased parquet-java fork can unblock step 4 if schedule matters, but it does
not satisfy step 5 unless it also owns the module split. Iceberg does not remove this boundary: its
Parquet adapter is internal implementation plumbing, and its reader still ultimately uses
parquet-java. Java NIO object-store providers add filesystem semantics without fixing the
Parquet/Hadoop packaging problem.

## Direct S3 and GCS output is a second step

The object-store research is relevant, but it is orthogonal to removing Hadoop from local Parquet.
Parquet's public `OutputFile` seam is sequential, so direct cloud output does not need the proposed
random-access read abstraction. Implement provider-specific `OutputFile`/`PositionOutputStream`
adapters over the official AWS SDK v2 multipart-upload and Google Cloud Storage resumable-upload
APIs.

That work needs an explicit durability/publication contract before code:

- an object is finalized only after multipart/resumable completion succeeds;
- failed or cancelled uploads are aborted and never entered in a manifest or checkpoint;
- completed object identity records size plus ETag/version ID on S3 or generation on GCS;
- manifest/checkpoint publication remains the dataset authority, because object stores do not offer
  the local temp-file, fsync, and atomic-rename sequence; and
- sorted output may still use local scratch while sending only finalized Parquet files to the
  destination.

For future direct cloud reads, the proposed positioned, version-pinned `RandomAccessObject` is the
right internal shape. Back it first with the official AWS and GCS SDKs, then adapt it to Parquet
`InputFile`/`SeekableInputStream`. Benchmark [AWS Analytics Accelerator Library for S3](https://github.com/awslabs/analytics-accelerator-s3)
and [GCS Analytics Core](https://github.com/GoogleCloudPlatform/gcs-analytics-core) later as optional
JVM accelerators; neither should be required by the first implementation. Do not add Iceberg merely
to obtain a seekable cloud stream.

## Independent second opinion

Claude Code 2.1.251 with Claude Opus reviewed the candidate and evidence read-only. Its central
recommendation matched the final decision: distinguish no Hadoop runtime jars from no Hadoop build
dependency and from a closed-world-clean artifact; do not ship the split-package bridge; prefer an
upstream API and slim artifact over Iceberg, NIO, or a long-lived broad fork; and keep direct cloud
I/O as a separate product change.

The review also found two actionable defects in the first candidate. Native ZSTD contexts were
being accumulated rather than reused, and the read-options bridge had copied two defaults
incorrectly. The candidate now caches one compressor per level and one decompressor per codec,
releases them through Parquet's writer/reader close lifecycle, and characterizes every observable
1.18.0 read-options default against the stock builder. Those fixes make the spike evidence valid;
they do not make its unsupported bridge production-safe.

## Reproduce

```bash
./gradlew :swath-core:parquetLinkability1180
./gradlew :swath-core:parquetOperationBaseline
./gradlew parquetDependencyBaseline
./gradlew build -PnoIntegration
```

The compact checked-in evidence is under
[`evidence/2026-08-31-hadoop-free-parquet-spike/`](evidence/2026-08-31-hadoop-free-parquet-spike/).
Generated per-probe reports remain under `swath-core/build/reports/parquet-linkability/`.
