# Dataset format

`swath list SOURCE -o DIR` saves an inventory in `DIR` as a swath dataset: sorted Parquet parts and a
`manifest.json` that commits them. This page shows what is in the directory, how to read what the listing
covered, and what each column means. Queries to start from, and how to verify a copy, are in
<a id="query-an-inventory-with-duckdb"></a><a id="check-that-an-inventory-is-complete"></a>[Query an inventory](query.md).

## What is in a dataset
<a id="what-a-swath-dataset-is"></a>

List a public prefix, then look at what the run left:

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 -o inventory
find inventory -not -path '*/.swath/*' | sort
```

```text
inventory
inventory/.swath
inventory/cuts.txt
inventory/data
inventory/data/part-00000.parquet
inventory/manifest.json
inventory/run.json
```

- **`manifest.json` is the commit.** swath writes it last, after every part is in place. It names each part with
  its row count, size, first and last key and SHA-256 digest. A directory without `manifest.json` holds no
  dataset, and one with it holds exactly the parts it lists, in key order. A part on its own is a readable
  Parquet file, but it does not show that a dataset exists, is complete or was published.
- **`data/`** holds the parts, `part-00000.parquet` upward, at most 100,000 of them. Name order is key order:
  every part has every column of the dataset's [schema](#schema-registry), its rows sorted by key, and each
  part's keys come after the previous part's.
- **`run.json`** is the [run report](reference/run-report.md): how the run went. It is written after the commit
  and is not part of the dataset.
- **`cuts.txt`** is the [cut file](#the-cut-file), which the next listing of the same bucket can start from. It
  is written last, after `run.json`; a dataset of a local tree or Azure hierarchical namespace has none.
- **`error.json`** is what a run that did not commit leaves instead of a manifest: that run's outcome
  ([exit codes](reference/exit-codes.md#the-outcome-of-record)). A committed dataset has none, a partial one
  included, except beside a dataset that an `--overwrite` run failed to replace, where it is that run's outcome.
- **`.swath/`** is the run's private workspace (its lock, journal, unpublished parts, spill and
  `progress.json`), which `swath resume DIR` continues from. Readers ignore it.

The dataset is a [SeekParquet](https://github.com/varveio/seekparquet) dataset
([format 1](https://github.com/varveio/seekparquet/blob/go/v0.2.0/spec/format-1.md)), so DuckDB, pandas, Spark or any Parquet reader reads the parts, and a SeekParquet reader can seek by key.
From Go, the [`reader`](../reader/README.md) module opens a dataset and decodes its rows, and its
[inventory view](../reader/README.md#the-inventory-view) pages, looks up, walks and sums it. The order in which a
run writes these files is in [Parquet output (internals)](internals/parquet-output.md#publication-gate-and-order).

## The swath extension

`swath show DIR` reads the manifest and says what the listing covered:

```bash
swath show inventory
```

```text
inventory: complete
  source    s3://noaa-gestofs-pds/stofs_2d_glo.20230113/
  listed    2026-11-17 11:19:09 to 11:19:10 UTC (0.8s)
  contents  5,252 objects, 383 GiB; keys "stofs_2d_glo.20230113/00/rerun/stofs_2d_glo_fcst.61.nc" .. "stofs_2d_glo.20230113/stofs_2d_glo.t18z.puertori.swl.grib2"
  coverage  nothing unreadable; no filters; no exclusions
  storage   1 part, 121 KiB of Parquet (schema swath/s3-objects/v1)
```

A program reads the same facts from `manifest.json`. Everything swath records about the listing is under
`extensions.swath`; the other members are SeekParquet's (the format, the schema id, the key column and the
parts):

```json
{
  "format": "seekparquet",
  "format_version": 0,
  "draft_revision": 1,
  "schema_id": "swath/s3-objects/v1",
  "parts": [
    {
      "path": "data/part-00000.parquet",
      "rows": 5252,
      "bytes": 124103,
      "min_key_base64": "c3RvZnNfMmRfZ2xvLjIwMjMwMTEzLzAwL3JlcnVuL3N0b2ZzXzJkX2dsb19mY3N0LjYxLm5j",
      "max_key_base64": "c3RvZnNfMmRfZ2xvLjIwMjMwMTEzL3N0b2ZzXzJkX2dsby50MTh6LnB1ZXJ0b3JpLnN3bC5ncmliMg==",
      "sha256": "6ccf6cd5720c9ba43b9d888d58edbf11fb966a2d9da202c6afe0d0360c4b1b46"
    }
  ],
  "extensions": {
    "swath": {
      "version": 1,
      "published_at": "2026-11-17T11:19:10.485248Z",
      "source": {
        "kind": "s3",
        "uri": "s3://noaa-gestofs-pds/stofs_2d_glo.20230113/",
        "container": "noaa-gestofs-pds"
      },
      "scope": {
        "kind": "prefix",
        "prefix": "stofs_2d_glo.20230113/"
      },
      "coverage": {
        "complete": true,
        "stop_reason": "end_of_scope",
        "unreadable": 0,
        "exclusions": [],
        "filters": [],
        "listing_started_at": "2026-11-17T11:19:09.662564Z",
        "listing_finished_at": "2026-11-17T11:19:10.478897Z"
      },
      "collected": {
        "columns": []
      },
      "committed_object_bytes": 410754901680
    }
  },
  "key": {
    "column": "key",
    "unique": true
  }
}
```

A filtered listing records each filter as it was given. With `--include '*.grib2' --min-size 1MiB` on the same
prefix, `swath show` reports `filtered: include=*.grib2, min_size=1MiB` and the manifest says:

```json
"filters": [
  {"filter": "include", "value": "*.grib2"},
  {"filter": "min_size", "value": "1MiB"}
]
```

A local listing also records its metadata tier and its filesystem's space (the path shortened here):

```json
"source": {"kind": "local", "uri": "…/media"},
"scope": {"kind": "whole"},
"collected": {"metadata": "stat", "columns": ["link_target"]},
"committed_object_bytes": 2015,
"filesystem": {"total_bytes": 206900281344, "free_bytes": 33451556864,
               "available_bytes": 33434779648, "measured_at": "2026-11-17T11:15:55.994952Z"}
```

| Member | Meaning |
|---|---|
| `version` | The version of this extension: `1`. A reader that does not know the version refuses the dataset, as `swath show` and `--seed-from` do |
| `published_at` | When the manifest was written (RFC 3339, UTC). It does not date the source's state |
| `source.kind` | `s3`, `gcs`, `azure` or `local` |
| `source.uri` | What was listed: `s3://bucket/prefix`, `gs://…`, `az://account/container/…`, or the absolute root path. An Azure container's name is unique only within its account, which only `source.uri` records |
| `source.container` | The bucket, or the Azure container's name; absent for a filesystem |
| `scope.kind` | `whole` (the bucket, container or tree) or `prefix` |
| `scope.prefix` | The prefix, with `prefix`; omitted when empty. `--seed-from` checks it against the new run's prefix |
| `coverage.complete` | Nothing is missing: the listing reached the end of its scope and `unreadable` is 0. Filters and exclusions are recorded beside it; they do not make it false |
| `coverage.stop_reason` | `end_of_scope` (the listing reached the end of its scope, whatever it could not read), or why it stopped early (`max_keys` only in a diagnostic sample, `unreported`, ...) |
| `coverage.unreadable` | Entries that could not be read and are missing from the dataset (a local tree's: permissions, a directory replaced while the walk ran). An entry that vanished during the walk is not counted (it is no longer in the tree), nor a symbolic link whose target could not be read (its row is kept) |
| `coverage.filters` | The filters the run applied, one object each: `filter`, the option's name in snake case (`include`, `include_regex`, `exclude`, `exclude_regex`, `min_size`, `max_size`, `modified_since`, `modified_until`, `storage_class`, and the local options that leave entries out, `one_file_system` and `no_dirs`), and `value`, as given (each pattern and storage class its own object), a time as RFC 3339 in UTC with six fraction digits (`--modified-since 7d` as the time it named), `null` for an option that takes no value |
| `coverage.exclusions` | What the listing left out by design (an output directory inside a listed tree) |
| `coverage.listing_started_at`, `listing_finished_at` | Bounds on when the entries were observed, across resumes |
| `collected` | What the listing read beyond the columns every row carries. `metadata` is a local listing's tier (`--metadata`: `names`, `stat`, `statx` or `links`, which on Windows reads `nlink` and `dev`; at `names` every stat column is null, and `swath du --from-dataset` then counts hard-linked files under every name, as the live run does); absent for an object store. `columns` names the optional columns the run asked for: `link_target` unless `--no-link-target` or the `names` tier; `owner_id` and `owner_display_name` with `--s3-fetch-owner`; Azure's `metadata`, `tags`, and `immutability_policy_until`, `immutability_policy_mode` and `legal_hold`, with `--azure-fetch-metadata`, `--azure-fetch-tags` and `--azure-fetch-immutability`. A null in a column it names means the source had no value; a null in an optional column it does not name means the run did not ask |
| `committed_object_bytes` | The dataset's object bytes: the `size` column summed over every object (a local tree's files only, not its directories; not an S3 delete marker), counted as the parts were written, so a resumed dataset states the same total as an uninterrupted one; the run report's `output.committed_object_bytes` holds the same value. Absent when unknown: a schema without a size column, or an object whose size is null (a local tree listed with `--metadata names`, which reads no sizes); `0` is a real total |
| `written_by` | The build that wrote the manifest: `tool` (`swath`), `version` (a release's `X.Y.Z` or `X.Y.Z-rc.N`; for a build from source, `devel` or a Go pseudo-version) and `revision`, the commit it was built from (`-dirty` when modified; absent when the build does not know it). Absent in manifests written by 0.4.0 and earlier |
| `filesystem` | A local source's filesystem space when the listing ended, as the operating system reports it: `total_bytes`, `free_bytes` (including space reserved for the superuser), `available_bytes` (what an unprivileged writer may use) and `measured_at`. It is what the filesystem says of itself, not the dataset's size or a disk's capacity: volumes that share a pool (an APFS container, a ZFS pool) each report the pool's free space, so do not sum it across datasets. Absent for a remote source and where it could not be measured ([which filesystem, and how](internals/providers/local.md)) |

**Reading coverage.** A listing with `coverage.complete` true and no filters covers its whole scope, minus its
`exclusions`, which are left out by design. Whether a partial, filtered or prefix-scoped dataset will do is
yours to decide from these members, as is whether it is of the source you expect. Every member but `version`
can be absent: treat an absent or `null` member as unknown (a missing `coverage.complete` is not `false`, and a
missing `filters` is not empty). Do not depend on the order of the manifest's members. swath publishes the
complete result of the live listing it performed; it is not a point-in-time snapshot of a source that changes
during the run.

## Schema registry

The manifest's `schema_id`, which `swath show` prints on its storage line, says what a row is and which columns
the parts have. A schema id is `swath/<row-contract>/vN`: the row contract it names and that contract's version.
Each id fixes the columns a reader may rely on: their names, physical and logical types and nullability, the
key's encoding and uniqueness, and the meaning of a row.

| Schema id | A row is | Key | Keys unique | Longest key |
|---|---|---|---|---|
| `swath/s3-objects/v1` | an S3 current object | UTF-8 (`STRING`) | yes | 1,024 bytes |
| `swath/s3-versions/v1` | an S3 version or delete marker (`--all-versions`) | UTF-8 | no: one row per version | 1,024 bytes |
| `swath/gcs-objects/v1` | a Cloud Storage live object | UTF-8 | yes | 1,024 bytes |
| `swath/gcs-generations/v1` | a Cloud Storage generation, live or noncurrent (`--all-versions`) | UTF-8 | no: one row per generation | 1,024 bytes |
| `swath/azure-objects/v1` | an Azure Blob current blob or hierarchical-namespace directory | UTF-8 | yes | 3,072 bytes (1,024 UTF-16 units) |
| `swath/azure-versions/v1` | an Azure Blob version, snapshot, soft-deleted blob or current blob (`--all-versions`) | UTF-8 | no: one row per entry | 3,072 bytes |
| `swath/vercel-objects/v1` | a current Vercel Blob object | UTF-8 | yes | no schema bound |
| `swath/local-entries/v1` | a filesystem entry below the root, directories included | arbitrary bytes (unannotated `BYTE_ARRAY`) | yes | none |

Every column after the key is nullable unless marked required. Types: `STRING` is `BYTE_ARRAY (STRING)`, UTF-8;
`binary` is unannotated `BYTE_ARRAY`; `int64` is `INT64` (`INT(64,true)` or unannotated); `uint32` and `uint64` are
`INT32 INT(32,false)` and `INT64 INT(64,false)`, read over their full unsigned range; `timestamp` is `INT64
TIMESTAMP(MICROS, UTC)`; `bool` is `BOOLEAN`. Every part has every column of its schema id. One concept has one
name in every schema that has it: `last_modified`, `created_at`, `deleted_at`, `accessed_at`,
`metadata_changed_at` and `storage_class_changed_at` for times, `storage_class` for the storage class (Azure's
access tier) and `md5` for the base64 MD5 a store lists.

**`swath/vercel-objects/v1`**: `key`, `size` int64 (required), `uploaded_at` timestamp (required), and `etag` STRING (null if absent or empty). Upload time is not a modification time; the ETag is opaque, not a promised content checksum. Virtual folders are not rows; real slash-suffixed blobs are.

**`swath/s3-objects/v1`**: `key`, `size` int64, `last_modified` timestamp, `etag` STRING (one pair of surrounding
quotes removed), `storage_class` STRING, `owner_id` and `owner_display_name` STRING (null unless the run used
`--s3-fetch-owner`), `checksum_algorithm` STRING (the first algorithm S3 lists), `checksum_type` STRING. `etag`,
`storage_class` and the checksums are null when S3 omits them.

**`swath/s3-versions/v1`**: the `swath/s3-objects/v1` columns in the same order, then `version_id` STRING
(verbatim; `"null"` is a real version id), `is_latest` bool (required) and `is_delete_marker` bool (required).
`size`, `etag`, `storage_class` and the checksums are null on a delete marker.

**`swath/gcs-objects/v1`**: `key` (the object name), `size` int64, `last_modified` timestamp (`updated`), `etag`
STRING (the JSON API's opaque entity tag), `generation` and `metageneration` int64, `storage_class` STRING, `md5`
and `crc32c` STRING (base64, as listed), `content_type` STRING, and last `created_at` timestamp
(`timeCreated`). `last_modified` also moves when only the object's metadata
changes, and `etag` is not a content hash. Compare content with `md5` or `crc32c`. GCS gives no `md5` for composite
objects and neither hash for objects encrypted with customer-managed or customer-supplied keys.

**`swath/gcs-generations/v1`**: the `swath/gcs-objects/v1` columns, then `is_latest` bool (required: the live
generation, exactly the row whose `deleted_at` is null) and `deleted_at` timestamp (`timeDeleted`: when the
generation became noncurrent; null for the live generation).

**`swath/azure-objects/v1`**: `key` (the blob name), `size` int64 (`Content-Length`), `last_modified` timestamp
(`Last-Modified`, at the wire's 1 s resolution), `etag` STRING (opaque, as listed), `md5` STRING (`Content-MD5`,
base64 as listed; null when absent, as for blobs uploaded in blocks without one), `content_type` STRING,
`blob_type` STRING (`BlockBlob`, `PageBlob`, `AppendBlob`), `storage_class` STRING (the access tier: `Hot`,
`Cool`, `Cold`, `Archive`, ...; null when absent; `--storage-class` filters on it), `created_at` timestamp
(`Creation-Time`). As on GCS, `last_modified` moves with metadata-only changes and `etag` is not a content hash:
compare content with `md5`, and never compare ETags across stores. Then `storage_class_inferred` bool (true when
the tier is the account's default, not set on the blob), `storage_class_changed_at` timestamp
(`Access-Tier-Change-Time`), `archive_status` STRING (`rehydrate-pending-to-hot`, `-cool`, `-cold`),
`rehydrate_priority` STRING (`High`, `Standard`), `accessed_at` timestamp (`LastAccessTime`: null unless the storage account has last access time tracking
enabled, and Azure updates it at most once per 24 hours), `content_encoding`
STRING, `server_encrypted` bool, `encryption_scope` STRING, `customer_provided_key_sha256`
STRING, `lease_state` STRING (`available`, `leased`, `expired`, `breaking`, `broken`), `tag_count` int64, and
the columns a run asks for, null unless it did (`collected.columns` names them): `metadata` and `tags` STRING (a
JSON object of names to values, keys sorted; `{}` for none; `metadata` null for a blob encrypted with a
customer-provided key, whose metadata a listing cannot read; DuckDB reads a value with `json(metadata)` or
`metadata->>'$.name'`), `immutability_policy_until` timestamp, `immutability_policy_mode` STRING
(`Unlocked`, `Locked`) and `legal_hold` bool (false for none). Each is as Azure lists it, null where it lists
nothing. A timestamp the Azure SDK decodes as `0001-01-01T00:00:00Z` (its value for a missing time) is null too.

**`swath/azure-versions/v1`**: the `swath/azure-objects/v1` columns in the same order, then `version_id` STRING
(as listed, a timestamp; null for a snapshot and on an account without versioning), `is_latest` bool (required:
the name's current blob, live, the row a current listing has), `snapshot` STRING (the snapshot's time as listed;
null unless the row is a snapshot), `is_deleted` bool (required: soft-deleted), `deleted_at` timestamp and
`remaining_retention_days` int64 (until the soft-deleted entry is purged; null unless deleted). A soft-deleted
entry keeps its size and metadata: it is stored and billed until purged. There are no delete markers.

**`swath/local-entries/v1`**: one row per entry below the root, directories included (`--no-dirs` omits them and
records `no_dirs` in `coverage.filters`):

| Column | Type | Source |
|---|---|---|
| `key` | binary, required | the path relative to the root, `/`-separated, no leading `/`; a directory's ends in `/`; the name's exact bytes |
| `size` | int64 | `st_size` (apparent bytes); a directory's row carries its own, so a byte total excludes `kind = 'dir'` (or is `swath du`'s): a plain `sum(size)` exceeds `swath du -A`'s total |
| `last_modified` | timestamp | `st_mtime` |
| `kind` | STRING, required | `file`, `dir`, `symlink`, `fifo`, `socket`, `char_device`, `block_device`, `unknown`; on Windows a junction or a volume mount point is a `symlink` |
| `mode` | uint32 | `st_mode & 0o7777` (permissions, setuid, setgid, sticky) |
| `uid`, `gid`, `nlink` | uint32 | `st_uid`, `st_gid`, `st_nlink`; on Windows `uid` and `gid` are null, and `nlink` is the link count at `--metadata links`, null otherwise |
| `inode`, `dev` | uint64 | `st_ino`, `st_dev` (statx: `makedev(major, minor)`); on Windows `inode` is the file ID where the filesystem has one (NTFS, ReFS: its low 64 bits) and `dev` is the volume's 32-bit serial number at `--metadata links`, null otherwise |
| `allocated_bytes` | int64 | `st_blocks * 512` (a sparse file has `allocated_bytes < size`); on Windows the directory listing's allocation size |
| `metadata_changed_at` | timestamp | `st_ctime` |
| `accessed_at` | timestamp | `st_atime`, which `noatime` mounts leave stale and `relatime` (the Linux default) updates at most once a day or on a write |
| `created_at` | timestamp | statx `stx_btime` (`--metadata statx` only, when the filesystem reports it); macOS `st_birthtime` (stat tier); Windows `CreationTime` |
| `link_target` | binary | the `readlink` result, symlinks only (unless `--no-link-target`); exact bytes on Linux and other Unix systems, while the portable engine on Windows writes it with `/` separators (`filepath.ToSlash`) |

Under `--metadata names` (no stat) the stat columns are null.

**Azure hierarchical directories.** A directory row uses its name with a trailing `/`; the source root
itself is omitted. It uses `swath/azure-objects/v1`, with no extra kind column. Dataset totals and `du`
count these rows as objects and sum their reported size (normally zero); a trailing slash cannot
distinguish a directory from a flat namespace's real marker blob. The tree source's diagnostic counters
separate files and directories. Optional metadata, tags and immutability fields stay null on directories.

**Totals from a local dataset.** A directory's `size` and `last_modified` are its own, never its subtree's, and
the root has no row. Summing `size` over every row gives entry bytes, directories' own `st_size` included: not
the files' content bytes, and not disk usage (`allocated_bytes`). For files only, leave out `kind = 'dir'`.
Summing `allocated_bytes` over every row, directories included, gives `swath du`'s on-disk total. Either
SQL sum counts a hard-linked file once per name; `swath du --from-dataset DIR` counts it once where the dataset
records its links (`nlink`, `dev` and `inode`: not a dataset listed on Windows without `--metadata links`, where every name counts), and
`swath du --from-dataset DIR --count-links` gives the same sum as the SQL. Under `--metadata names` the sizes are
null, so a sum is no total.

## Keys and order

This section is for reading the parts without swath: DuckDB, Python, Spark.

- **The dataset is the parts `manifest.json` lists**, in the listed order. In a committed local directory those
  are exactly the files under `data/`, so a glob reads them; elsewhere, read the manifest's list
  ([Query an inventory](query.md#query-an-inventory-with-duckdb) has both queries). Columns a schema id does not
  define may appear anywhere and are ignored.
- **Order is unsigned byte order.** Rows ascend by `key`'s bytes compared as unsigned bytes, within each part and
  across parts in manifest order. Keys are never normalized (Unicode, case, separators). Cloud keys are the
  provider's UTF-8 bytes. A local key is the name's bytes exactly as the kernel returned them, which need not be
  UTF-8. Manifest bounds are standard base64 of the key bytes: decode them and compare bytes, never the base64
  text.
- **Azure order is byte order after adaptation.** List Blobs returns names in UTF-16 code-unit order, which
  puts names with characters at U+10000 and above before those at U+E000 to U+FFFF. swath's adapter reorders
  them, so an Azure dataset is in byte order like every other one, not in the service's listing order.
- **Versions rows are consecutive per key.** In `swath/s3-versions/v1`, `swath/gcs-generations/v1` and
  `swath/azure-versions/v1` a key repeats once per version, on consecutive rows, and a key's group never spans
  two parts. On S3 the order within a group is the service's: newest first, with versions and delete markers
  interleaved. No column rebuilds it, since two entries can share `last_modified`. On GCS rows order by key and
  then by numeric `generation`, strictly ascending; the lister refuses a page that breaks this. On Azure Blob a
  group is in the service's order: a name's entries keyed by their time within the name (snapshots and versions
  oldest first; the base blob or current version, the one with `is_latest`, last). The `is_latest`,
  `is_delete_marker` and `deleted_at` values are the service's, per row. In an unfiltered, complete listing, the
  first row of an S3 group is the one with `is_latest` true, and a key whose first row is a delete marker is
  deleted. On GCS the last row is the newest generation, live (`is_latest`) only when `deleted_at` is null, and a
  name may have no live generation. A filter (`--min-size`, `--include`, ...) can remove a key's latest version,
  so in a filtered dataset none of those positional inferences hold. The current objects are
  `is_latest AND NOT is_delete_marker` (S3) or `is_latest` (GCS and Azure Blob), row by row.
- **Stored order is physical.** `ORDER BY key` alone does not recover the order among equal keys, and a generic
  SQL scan does not promise stored order. To keep a group's order, read the parts in manifest order and number
  the rows as read, then order by the part's position in the manifest and the row number. For example, list
  every version under the same S3 prefix into `versions/`, then read it in stored order:

  ```bash
  swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 --all-versions -o versions
  duckdb -c "
    SET VARIABLE parts = (SELECT list('versions/' || p.path)
                          FROM (SELECT unnest(parts) AS p FROM read_json('versions/manifest.json')));
    SELECT key, version_id, is_latest, is_delete_marker
    FROM read_parquet(getvariable('parts'), filename = true, file_row_number = true)
    ORDER BY list_position(getvariable('parts'), filename), file_row_number
    LIMIT 3;"
  ```

  ```text
  ┌────────────────────────────────────────────────────────┬────────────┬───────────┬──────────────────┐
  │                          key                           │ version_id │ is_latest │ is_delete_marker │
  │                        varchar                         │  varchar   │  boolean  │     boolean      │
  ├────────────────────────────────────────────────────────┼────────────┼───────────┼──────────────────┤
  │ stofs_2d_glo.20230113/00/rerun/stofs_2d_glo_fcst.61.nc │ null       │ true      │ false            │
  │ stofs_2d_glo.20230113/00/rerun/stofs_2d_glo_fcst.62.nc │ null       │ true      │ false            │
  │ stofs_2d_glo.20230113/00/rerun/stofs_2d_glo_fcst.63.nc │ null       │ true      │ false            │
  └────────────────────────────────────────────────────────┴────────────┴───────────┴──────────────────┘
  ```

  That bucket has no versioning, so every object has one version, whose id is the string `null`.
- **Null is unknown, not zero.** A null `size` or time is not 0 or the epoch. SQL `SUM` and `MAX` skip nulls,
  so a total over a column with nulls is a lower bound. Count the nulls beside it, as `swath du` reports
  `size_missing`.

## A dataset in a bucket

A dataset written to a bucket (`-o gs://BUCKET/PREFIX`, `s3://…` or `az://…`; how to run one is in
[Writing to a bucket](production.md#writing-to-a-bucket)) holds the same files under `PREFIX/`. For a reader it
differs in three ways:

- **Part names are unique to a run and an attempt:** `PREFIX/data/RUN/ATTEMPT/part-NNNNN.parquet`, where RUN is
  the run's UUID (kept across its resumes) and ATTEMPT the invocation's id. Ordinals stay dense and in key
  order (`part-NNNNN`), but parts of one dataset may sit under several attempt directories when the run was
  resumed. Take the parts from the manifest's `parts[].path`, never from a listing of `data/`: parts are
  uploaded as they close, before the commit, and `data/` can also hold parts no manifest names (those of a run
  abandoned before its commit, or an upload whose outcome the run could not learn).
- **The manifest is created, never replaced:** until `PREFIX/manifest.json` exists the prefix holds no
  dataset, whatever `data/` holds, and once it exists swath never replaces it.
- **`cuts.txt` and `run.json` are written only if absent**, after the commit, by the run that committed. An
  object already at either name, which no run of this dataset wrote, is left as it is, and for the cut file the
  run warns that it did not write its own: do not seed from that object.

The upload, commit and cleanup protocol is in
[Durability and resume (internals)](internals/durability.md#a-dataset-in-a-bucket).

## The cut file

`cuts.txt` records where the dataset's keys fall, so the next listing of the same bucket and scope can divide
its work from the start instead of discovering the key space again. Pass the previous dataset with
`--seed-from`, and the run reads it ([seeding a refresh](resume-and-refresh.md#seeding-a-refresh-from-the-previous-dataset)).
`--seed-from` reads a local path, so for a dataset in a bucket, copy `PREFIX/cuts.txt` and pass that file.

Each line is a key, a tab, and the number of rows after the previous line's key through that key; the counts sum
to the manifest's rows, and the last line is the dataset's last key:

```text
stofs_2d_glo.20230113/stofs_2d_glo.t00z.northpacific.f051.grib2	1024
stofs_2d_glo.20230113/stofs_2d_glo.t06z.guam.f141.grib2	1024
stofs_2d_glo.20230113/stofs_2d_glo.t12z.conus.west.f054.grib2	1024
stofs_2d_glo.20230113/stofs_2d_glo.t18z.alaska.f144.grib2	1024
stofs_2d_glo.20230113/stofs_2d_glo.t18z.puertori.f050.grib2	1024
stofs_2d_glo.20230113/stofs_2d_glo.t18z.puertori.swl.grib2	132
```

These six lines are the `inventory` dataset's: their counts sum to its 5,252 rows.

The file stays within 262,144 lines and 16 MiB whatever the dataset's size. It is advisory and outside the
SeekParquet format: the manifest does not name it, readers ignore it, and a dataset without it is whole. A
dataset has none when its source is a local tree or Azure hierarchical namespace (which `--seed-from` does not take), when it has no rows, when
its last key holds a line feed, or when the run was stopped between its commit and the cut file. A run that
cannot write it warns, and the dataset stands.

`--seed-from DIR` uses the file only when it is the dataset's own (its lines agree with the manifest). Otherwise,
or when it is absent, the run reads the parts' key page index instead, warns once when the file was there and
refused, and the run report's `invocation.prior.cuts_file` says which (`read`, `absent`, or `refused` with
`cuts_refused`). How the cuts are chosen and checked is in
[Parquet output (internals)](internals/parquet-output.md#publication-gate-and-order).

## What stays stable across versions

A dataset carries three version identifiers. Each one moves only for the changes in its row:

| Identifier | May change without a new value | Forces a new value |
|---|---|---|
| SeekParquet `format_version` / `draft_revision` | editorial clarifications | any change to which bytes are accepted or what a query returns |
| `schema_id` | writer layout (row groups, pages, encodings, codecs) | an added, renamed or removed column (every part has every column of its id), a changed type, signedness, time unit or nullability, a changed key encoding or uniqueness, a changed meaning of a row; type widening is not automatically compatible |
| `extensions.swath.version` | added informative members that readers may ignore | a change to an existing member's meaning. When an older application could misread the dataset, the schema id changes too, because older readers ignore members they do not know |

Look columns up by name: physical column order is not part of the contract, and readers ignore columns they do
not know wherever they are placed, as they ignore extension members they do not define. Each part's
`created_by` names the build that wrote it, so a resumed run can hold parts from two builds. swath's release
version moves independently of these identifiers. Who decides each change, and the identifiers private to a
run, are in [Contracts (internals)](internals/contracts.md#version-identifiers).

## Reference: the swath dataset profile

The sections above are written for someone reading a dataset. This one states the profile an independent reader
implements: what makes a SeekParquet dataset a swath dataset, and what a reader checks. The SeekParquet
specification ([`spec/format-1.md`](https://github.com/varveio/seekparquet/blob/go/v0.2.0/spec/format-1.md), at
`go/v0.2.0`, the release that fixed draft revision 1, the revision swath writes) owns the format itself: the
manifest's own members, part bounds, footer stamps, key order and commit. The SeekParquet Go module swath builds
with is a separate pin, which follows its API rather than the format ([Dependencies](internals/dependencies.md)).
The Go API over this profile is [`reader`](../reader/README.md); the guarantees swath's writer keeps, with their
tests, are in [Contracts (internals)](internals/contracts.md#dataset-guarantees-readers-rely-on-these).

A directory (or object-store prefix) is a swath dataset when all of the following are true:

| Layer | Identified by | Defined by |
|---|---|---|
| SeekParquet dataset | `manifest.json` with `format` `seekparquet`, `format_version` 0, `draft_revision` 1, and the `seekparquet.*` stamps in every part's footer | the SeekParquet specification |
| swath schema | `schema_id`: exactly one of the [registered ids](#schema-registry), compared byte for byte, never by prefix or after normalization | [Schema registry](#schema-registry) |
| swath metadata | `extensions.swath` with `version` 1 and well-formed members | [The swath extension](#the-swath-extension) |

SeekParquet allows only its own members at the top level, in `key` and in each part; a producer's metadata goes
under `extensions`, by namespace, and swath writes `extensions.swath` only. A SeekParquet dataset without a
registered schema id or without a valid `extensions.swath` is not a swath dataset; any SeekParquet reader can
still open it, and generic SeekParquet reads ignore `extensions.swath` entirely. A partial dataset (`coverage.complete`
false: it left out entries it could not read, or stopped before the end of its scope) is still a swath dataset, and
opens and reads like any other. Filters alone do not make a dataset partial: a listing that finishes with no
unreadable entries is complete for the filters `coverage.filters` records.

A reader makes two checks, and judges nothing beyond them:

| Check | What it establishes | Reads |
|---|---|---|
| Identification | The manifest is SeekParquet format version 0, draft revision 1, and the schema id is registered. When `extensions.swath` is present, its `version` is the integer 1 (absent, `null`, non-integer or any other value is refused as an unsupported version) and its members have the right JSON types (a wrong type is malformed metadata). A manifest with no `extensions.swath` opens, so identification alone does not prove that swath wrote a dataset | the manifest only |
| Validation | The manifest's key declaration and every part's footer, stamps and columns match the schema id's row contract | every footer and each row group's first key page |

Neither check reads every row's value or the part digests. Part digests are for checking a dataset after it is
copied or moved: [Verify a dataset](query.md#verify-a-dataset).

**Footer stamps.** Every part's footer carries SeekParquet's five stamps, equal to the manifest's:
`seekparquet.format_version`, `seekparquet.draft_revision`, `seekparquet.schema_id`, `seekparquet.key_column`
(`key`) and `seekparquet.key_unique`. Within-part order is the standard `sorting_columns` (`key`, ascending).
swath adds no stamp of its own. A footer identifies the row contract its part claims; it cannot show that the
part belongs to a dataset, whether parts are missing, or what the listing covered. Those are the manifest's.
