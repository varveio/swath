# Query an inventory

A committed inventory is a directory of Parquet parts that DuckDB, pandas, Spark or any Parquet reader can
query. Below are queries to start from, how to tell whether an inventory is complete, and how to verify one;
[the swath dataset format](dataset.md) defines every file, column and field.

## Query an inventory with DuckDB

A committed inventory in a local directory holds exactly the parts its manifest lists, under `data/`, so a glob
of them reads the whole dataset. This query lists the five largest files of a local inventory in `inventory/`:

```bash
duckdb -c "
  SELECT try(decode(key)) AS key, size
  FROM read_parquet('inventory/data/*.parquet')
  WHERE kind = 'file'
  ORDER BY size DESC
  LIMIT 5;"
```

```text
┌───────────────────────┬──────────┐
│          key          │   size   │
│        varchar        │  int64   │
├───────────────────────┼──────────┤
│ archive/disk.img      │ 67108864 │
│ videos/trip.mp4       │ 31000000 │
│ photos/2025/beach.jpg │  2400000 │
│ photos/2026/snow.jpg  │  2100000 │
│ photos/2025/hills.jpg │  1900000 │
└───────────────────────┴──────────┘
```

A local tree's `key` is binary, since file names need not be UTF-8, so `try(decode(key))` reads it as text: a
name that is not UTF-8 reads as NULL there instead of failing the query (plain `decode` refuses it), and `key`
itself keeps its exact bytes. An object store's `key` is text already, so write `key`, and leave out `kind`,
which only a local tree has. Each source's columns are in the [schema registry](dataset.md#schema-registry); every part
of a dataset has every column of its schema.

The glob is not enough in two cases. There, read the part list from `manifest.json`:

- **The directory has no `manifest.json`.** Its run has not committed. An interrupted run's `data/` holds the
  parts written so far, and a glob reads them as a partial listing without an error. `swath show DIR` says
  whether a dataset is there and how complete it is ([below](#check-that-an-inventory-is-complete)).
- **The dataset is in a bucket.** Its parts are under `data/RUN/ATTEMPT/`, and a run that was abandoned can
  leave parts beside them that the manifest does not name ([a dataset in a bucket](dataset.md#a-dataset-in-a-bucket)).

```bash
duckdb -c "
  SET VARIABLE parts = (SELECT list('inventory/' || p.path)
                        FROM (SELECT unnest(parts) AS p FROM read_json('inventory/manifest.json')));
  SELECT try(decode(key)) AS key, size
  FROM read_parquet(getvariable('parts'))
  WHERE kind = 'file'
  ORDER BY size DESC
  LIMIT 5;"
```

This query reads the parts a local dataset's manifest lists and totals its rows by kind:

```bash
duckdb -c "
  SET VARIABLE parts = (SELECT list('media-inventory/' || p.path)
                        FROM (SELECT unnest(parts) AS p FROM read_json('media-inventory/manifest.json')));
  SELECT kind, count(*) AS entries, sum(size) AS bytes
  FROM read_parquet(getvariable('parts'), union_by_name = true)
  GROUP BY kind ORDER BY kind;"
```

`union_by_name = true` reads optional columns by name across parts.

## pandas and Spark

The same two forms read an inventory into pandas (with pyarrow installed) or Spark. pandas reads the directory
of parts; a local tree's `key` arrives as `bytes`, decoded here for display:

```python
import pandas as pd

df = pd.read_parquet("inventory/data/", columns=["key", "size", "kind"])
files = df[df["kind"] == "file"].nlargest(5, "size")
files["key"] = files["key"].str.decode("utf-8", errors="replace")
print(files[["key", "size"]])
```

Where the glob is not enough, read the parts the manifest lists:

```python
import json
import pandas as pd

with open("inventory/manifest.json") as f:
    parts = ["inventory/" + p["path"] for p in json.load(f)["parts"]]
df = pd.concat(pd.read_parquet(p, columns=["key", "size", "kind"]) for p in parts)
```

Spark takes the same glob, or the manifest's list as paths. An object store's `key` is a string already:

```python
df = spark.read.parquet("logs-inventory/data/*.parquet")  # or spark.read.parquet(*parts)
df.orderBy("size", ascending=False).select("key", "size").show(5)
```

## Check that an inventory is complete

`swath show DIR` prints the coverage on one line; a program reads the same from the manifest:

```bash
duckdb -c "SELECT c.complete, c.stop_reason, c.unreadable,
                  len(c.filters) AS filters, len(c.exclusions) AS exclusions
           FROM (SELECT extensions.swath.coverage AS c FROM read_json('inventory/manifest.json'))"
```

```text
┌──────────┬──────────────┬────────────┬─────────┬────────────┐
│ complete │ stop_reason  │ unreadable │ filters │ exclusions │
│ boolean  │   varchar    │   int64    │  int64  │   int64    │
├──────────┼──────────────┼────────────┼─────────┼────────────┤
│ true     │ end_of_scope │          0 │       0 │          0 │
└──────────┴──────────────┴────────────┴─────────┴────────────┘
```

`complete` is true when nothing is missing: the listing reached the end of its scope (`stop_reason`
`end_of_scope`) and read every entry (`unreadable` 0). Filters do not make it partial: an inventory is complete for
the filters it records, so check that `filters` and `exclusions` are empty, or that you accept them. Check too that the manifest's
`source` and `scope` are the ones you expect (`scope.kind` `whole` for the whole source), and treat a missing
field as unknown, not as 0 ([the swath extension](dataset.md#the-swath-extension)). Listed ETags are not portable content hashes; check the
provider's caveats in the [schema registry](dataset.md#schema-registry) before comparing inventories.

## Verify a dataset

To check that a copied or moved dataset's parts are the bytes its manifest committed, compare each part with
the SHA-256 the manifest records for it. From the dataset's directory, with `jq` and `sha256sum`:

```bash
jq -r '.parts[] | "\(.sha256)  \(.path)"' manifest.json | sha256sum -c
```

Each part prints `OK`, and the command exits 0, when every part matches; a part that differs prints `FAILED`
and the command exits 1. `swath show DIR` then says what the dataset covers.

For the format, key order and pages as well, `swath verify` is planned
([#391](https://github.com/varveio/swath/issues/391)); meanwhile, use
[SeekParquet's validator](https://github.com/varveio/seekparquet) on a local copy (see the “Validating”
section of its `go/README.md`). From the root of a SeekParquet checkout that includes `cmd/seekparquet`
and supports this dataset's format (format version 0, draft revision 1), run the following. swath pins
`github.com/varveio/seekparquet/go v0.2.0` in `go.mod`, the release that fixed draft revision 1; use that
release (tag `go/v0.2.0`) or a later one with a matching format.

```bash
go -C go run ./cmd/seekparquet validate --level digest /path/to/dataset && \
go -C go run ./cmd/seekparquet validate --level full /path/to/dataset
```

`digest` streams each part with a declared SHA-256 and compares its bytes with the manifest's digest;
swath declares one for every part. `full` separately checks the format, key order and decoded pages
(including the structural and profile levels); it does not check digests or swath's extension and
schema semantics. Neither proves that the listing covers the source you intended.

The validator exits 0 when all requested levels complete, 1 when any level fails, and 2 when validation
is incomplete, cannot run, or the arguments are invalid. With `go run`, a nonzero validator status is
printed as `exit status N`; the `go` command itself exits 1.

For a dataset produced with local output, the repository also has
[`verify.py`](../verify.py): `python3 verify.py /path/to/dataset` from the repository root, with
DuckDB installed. It checks S3, GCS, Azure Blob and local datasets independently of the Go reader: swath
metadata, schema-specific rules, row counts, key order and the manifest's part bounds, footer stamps and part
SHA-256 digests; for Azure also the `azure` source kind and every part's column types and nullability,
including UTC microsecond timestamps. It expects the producer's flat
`data/part-NNNNN.parquet` layout and `run.json`, so it is not a general validator for a bucket dataset
copied locally with its nested run/attempt part paths. See the [verifier guide](internals/testing.md#verifypy)
for its checks and optional reference comparison.
