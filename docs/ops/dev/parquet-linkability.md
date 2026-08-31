# Parquet without Hadoop — linkability laboratory and baseline

This is the PR 7 measurement record. It does not change production dependencies or authorize a
Parquet migration. The laboratory answers a narrower question: whether swath's current writer and
low-level reader paths link on a runtime classpath with no `org.apache.hadoop*` artifact.

## Result

Neither pinned parquet-java 1.15.1 nor 1.18.0 is a drop-in Hadoop-free runtime for swath. Both
releases have the same three blocking edges:

| Path | First missing class | Edge |
| --- | --- | --- |
| direct and sorted writers | `org.apache.hadoop.conf.Configuration` | swath's `ParquetWriter.Builder` bridge and parquet-java's legacy writer hook remain Hadoop-shaped |
| ZSTD codec | `org.apache.hadoop.conf.Configurable` | `ZstandardCodec` implements Hadoop's configuration and compression interfaces |
| default low-level readers | `org.apache.hadoop.conf.Configuration` | `ParquetFileReader.open(InputFile)` constructs `HadoopParquetConfiguration` before reading the footer |

All 14 production classes that import Apache Parquet types load successfully under both releases;
the failures begin when those paths are exercised. The laboratory derives that 14-class inventory
from `swath-core/src/main/java` and fails if its checked list drifts.

The reader blockage is shallow. A test-only bridge using `PlainParquetConfiguration`, the
package-private `ParquetReadOptions` constructor, and a small zstd-jni-backed read codec successfully
read the canonical 2,048-row swath fixture with both releases. It read footer metadata, the column
index, and actual ZSTD-compressed rows. The same bridge read a 16-row DuckDB-authored ZSTD file, and
DuckDB read the swath-authored fixture with the expected first key, last key, and row count.

That result is evidence for a usable low-level Apache Parquet path, not approval to ship the bridge.
Production still needs supported non-Hadoop writer hooks, an explicit codec factory, and public read
options at every swath reader. The test-only class under `org.apache.parquet` must not become a
production split package.

The JVM and closed-world results differ. JDK 25 `jdeps --missing-deps` found 311 Hadoop missing-reference
lines when scanning the whole `parquet-hadoop` 1.18.0 artifact with the other 1.18.0 Parquet modules
available. A native-image tool was not installed, so no native image was attempted. Lazy JVM linkage
therefore demonstrates the low-level reader mechanism, while the artifact-wide result says a slim
artifact or upstream/fork isolation is still required before making a native-image claim.

## Reproduce the laboratory

Run both isolated versions, the current-runtime operation samples, dependency closure diff, and
package-size capture:

```bash
./gradlew parquetBaseline
```

The isolated configurations force every `org.apache.parquet` module to exactly 1.15.1 or 1.18.0,
reject resolved groups equal to or beginning with `org.apache.hadoop`, and scan every runtime jar for
`org/apache/hadoop/` class entries. Each operation runs in a fresh JVM so a failed class initialization
cannot affect a later probe. A normal-classpath preparation process creates the swath and DuckDB
fixtures before the Hadoop-free JVMs open them.

Generated reports land under `build/reports/parquet-linkability/` and
`swath-core/build/reports/parquet-linkability/`. The dated raw records used below are checked in under
[`evidence/2026-08-31-parquet-linkability/`](evidence/2026-08-31-parquet-linkability/).

## Dependency and package baseline

These are observations from the x86-64 workspace at base commit
`fad4bd6c72e134bd87d4e5db73894eb621547f5e`, using Temurin 25.0.4. The closure diff removes
`org.apache.hadoop` and `org.apache.hadoop.*` groups from a copy of each runtime graph. An artifact is
Hadoop-attributable when it disappears from that graph; a dependency independently declared by the
application remains and is not counted merely because Hadoop also uses it.

| Runtime graph | Artifacts / bytes | Hadoop-attributable artifacts / bytes |
| --- | ---: | ---: |
| `swath-core` | 59 / 64,634,573 | 36 / 26,288,531 |
| `swath-cli` | 93 / 75,141,843 | 32 / 24,461,029 |
| `swath-replay` | 65 / 151,369,720 | 36 / 26,288,531 |

The replay total includes its intentional DuckDB JDBC dependency. The generated report lists every
coordinate and byte count; these totals are not estimates extrapolated between applications.

| Packaged artifact | Files | Jars | Bytes |
| --- | ---: | ---: | ---: |
| CLI fat jar | 1 | 1 | 75,449,028 |
| CLI install distribution | 102 | 97 | 76,454,120 |
| replay install distribution | 76 | 69 | 152,627,426 |
| CLI linux/amd64 container | — | — | 174,320,920 |
| replay linux/amd64 container | — | — | 251,820,235 |

The images used the exact locally built fat jar/install distribution through the repository's
promotion context and the pinned Temurin JRE base. Docker's content size is reported, not the
humanized virtual-size column from `docker images`.

## Operation and startup baseline

The operation fixture contains 2,048 sorted object rows, ZSTD level 3, dictionaries, 128-row page
limits, footer stamps, and a 24,050-byte output. Five samples ran in one JVM. The first sample includes
Parquet and native ZSTD initialization; the median below includes all five and is diagnostic, not a
portable performance threshold.

| Operation | Median wall time |
| --- | ---: |
| write, footer close, and durability barrier | 30.671 ms |
| footer/stamp read | 4.655 ms |
| actual-first-key index derivation | 3.697 ms |
| bounded 128-row range read | 6.894 ms |
| one full row-group read | 12.013 ms |
| merge-input `SegmentReader` full scan | 14.444 ms |

Every sample returned all expected rows, and all five files in that JVM had the same SHA-256. These
small-fixture timings establish a parity probe and first-operation baseline; they do not replace the
existing representative merge, replay-capacity, PERF-2 heap, or compression gates.

Fresh-process startup used the same Temurin JDK with uncontrolled filesystem cache state and five
samples per command:

| Command | Median wall time |
| --- | ---: |
| CLI `--version` | 302.840 ms |
| CLI `--help` | 308.515 ms |
| replay `--help` | 182.862 ms |

## Decision boundary

Parquet 1.18.0 does not remove Hadoop by upgrade alone. A later dependency migration must first
provide supported replacements for all three edges and must package only the slim reachable classes
needed by the writer and low-level reader. PR 7 leaves production at parquet-java 1.15.1 and Hadoop
3.4.1. The common-writer construction cleanup can proceed independently; dependency removal still
waits for a separate migration decision and the full fidelity, durability, replay, packaging, and
resource gates.
