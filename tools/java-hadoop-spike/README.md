# Java Parquet Hadoop boundary probe

This isolated experiment refreshes the September Hadoop-free spike against the latest
Maven Central release checked on 2026-10-07: Apache parquet-java **1.18.1**. It changes no
production source or dependency. Runtime classpaths are audited for both Hadoop coordinates
and `org/apache/hadoop/` class entries before execution.

The stock public `ParquetReadOptions.builder(PlainParquetConfiguration)` still constructs
Hadoop codecs and parses Hadoop input-format filters before a caller can supply either
replacement. On the audited classpath it fails at `HadoopCodecs.newFactory` with
`NoClassDefFoundError: org/apache/hadoop/io/compress/zlib/ZlibCompressor$CompressionLevel`.
Calling `.withCodecFactory(...).withRecordFilter(...)` cannot repair a constructor that
already failed.

The proposed [patch](read-options-1.18.1.patch) adds a public builder constructor taking
plain configuration, an explicit codec factory and an explicit record filter. It reuses
upstream configuration/default initialization and preserves the existing constructor's
order. This is a small **upstream API proposal**, not a released supported solution.
The experiment compiles the modified upstream class and places that class first on the
classpath. There is no application class in an `org.apache.parquet` split package reaching
a private constructor; the patched upstream class still requires a maintained patched
artifact until upstream provides this API. We do not run the complete upstream Maven test
suite or demonstrate a production-ready fork.

With that patch and the adapted September explicit ZSTD/UNCOMPRESSED codec factory:

- A stock public writer writes 2,048 rows with the exact 13-field current Swath schema,
  uppercase discriminator values, mixed optional values/nulls, and ZSTD pages.
- A fresh Hadoop-free JVM reads the footer, 26 column chunks and 403 indexed pages and
  checks every field/null through full decoding.
- The actual low-level methods `getColumnIndexStore`, `setRequestedSchema`,
  `readFilteredRowGroup` and `readRowGroup` execute: a key-only projection prunes to
  64 page rows containing exactly the 20 requested keys, then restores the complete schema.
- Two separate JVMs containing Hadoop dump non-codec options: one uses the untouched
  released jar, the other the proposed API. The dumps match for defaults and explicitly
  changed configuration, including property values; allocator classes match. Codec
  factories are deliberately different and are released.

This is canonical-shape library evidence. It does not establish the complete Swath writer
or sorted-stamp/durability path, cloud integration, throughput, all external codecs, or
native-image feasibility. Those are independent gates. Compile-time Hadoop remains,
`parquet-hadoop` remains, and Hadoop descriptors remain in its classes. An API overload
removes the runtime constructor obstruction, not the artifact/module boundary.

## Reproduce

Use a fresh absolute output directory outside the repository and a JDK 25 installation:

```bash
python3 tools/java-hadoop-spike/run_probe.py \
  --output /workspaces/swath-data/java-storage-spike-20261007/hadoop-api-final-run \
  --jdk /home/vscode/.jdks/jdk-25.0.3+9
```

The driver uses the existing Gradle wrapper, Maven Central and the pinned upstream
[`apache-parquet-1.18.1` source](https://github.com/apache/parquet-java/blob/apache-parquet-1.18.1/parquet-hadoop/src/main/java/org/apache/parquet/ParquetReadOptions.java).
It verifies source SHA-256 before applying the patch. Build/cache scratch is under `/tmp`.
Receipts record commands, exit codes, exact runtime jars/hashes, source hash and results. It also generates the artifact-level `jdeps` audit (311 Hadoop
missing-reference lines on this release), checks implementation hashes before/after the
run, and freezes the final probe source alongside the evidence.
The resolved codec dependency is zstd-jni **1.5.7-15**. Compile-only Hadoop common/mapreduce
are needed to compile descriptors in the stock upstream class; neither enters the runtime
probe classpath.

The useful decision is precise: stock 1.18.1 still blocks supported Hadoop-free read-options
construction; a narrow explicit-dependencies API fixes that exercised obstruction. Prefer
upstreaming and consuming that API, or explicitly budget ownership of a small rebased
artifact patch. Delete the patch when a tested upstream release supplies the equivalent
public path. Removing every Hadoop-linked class needs a separate module/artifact change.
