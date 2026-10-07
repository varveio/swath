# Java Parquet alternative API smoke

This bounded check evaluates released **Hardwood 1.1.0.Beta1** with only its core jar on
JDK 25's classpath. It writes and reads 513 rows across six row groups, a required UTF8
key and optional bare INT64 size, and footer metadata. DuckDB independently checks
count, size bounds, and row-group count. Every size value is populated: this is not a
null-value compatibility check.

All downloads and output go outside the repository. Run with Python 3 plus `duckdb`
(use an external virtual environment), and a JDK 25 executable:

```sh
python3 tools/java-alternatives-spike/run_probe.py \
  --java /home/vscode/.jdks/jdk-25.0.3+9/bin/java \
  --output /tmp/hardwood-api-probe
```

The driver pins the official Maven jar checksum. It generates no benchmark or performance
claim. Native access permits Hardwood's optional foreign-function codec discovery; the
probe uses uncompressed pages. SIMD is optional and is not enabled.

This library is an independent implementation with no required Hadoop runtime jars.
Its **released writer does not emit column/offset page indexes**, so this check does not
qualify it as a replacement for Swath's indexed Parquet writer. Unreleased main has added
index emission; assess an actual release before making that migration decision. This
check also does not establish remote identity protection, cancellation, full Swath schema,
malformed-index handling, binary key ordering, or native-image viability.

Official source and license:

- [Released source](https://github.com/hardwood-hq/hardwood/tree/fe06a5d09d299c20c21823692c574edfdf780189)
- [Released writer](https://github.com/hardwood-hq/hardwood/blob/fe06a5d09d299c20c21823692c574edfdf780189/core/src/main/java/dev/hardwood/writer/ParquetFileWriter.java)
- [Dependencies](https://github.com/hardwood-hq/hardwood/blob/fe06a5d09d299c20c21823692c574edfdf780189/core/pom.xml)
- [Apache 2.0 license](https://github.com/hardwood-hq/hardwood/blob/fe06a5d09d299c20c21823692c574edfdf780189/LICENSE.txt)

Assessment evidence for Iceberg, Parquetry, Trino and wrappers is recorded separately in
`/workspaces/swath-data/java-storage-spike-20261007/alternatives/assessment.md`.
