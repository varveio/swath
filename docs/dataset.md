# The swath dataset

`swath list ... -o DIR` writes a **swath dataset**: a [SeekParquet](https://github.com/varveio/seekparquet)
dataset that carries one of swath's registered schema ids and a valid `extensions.swath`. This page owns the
**swath dataset profile**: the schema ids and their row contracts, the swath extension, what each reader
operation checks, the key semantics a consumer outside `reader` must know, and how the profile evolves.
The SeekParquet specification ([`spec/format-1.md`](https://github.com/varveio/seekparquet/blob/7264a9f88420/spec/format-1.md),
at the commit `go.mod` pins) owns the format: the manifest's own members, part bounds, footer stamps, key
order and commit. [`reader`](../reader/README.md) is the Go API over this profile;
[internals/parquet-output.md](internals/parquet-output.md) explains how the writer meets it;
[upgrade.md](upgrade.md) maps the 0.3 release's format 1 onto it.

Nothing here is released. Until the first release, any of this profile may change outright, with no
compatibility window for datasets written by earlier builds.

## What a swath dataset is

A directory (or object-store prefix) is a swath dataset when all of the following are true:

| Layer | Identified by | Defined by |
|---|---|---|
| SeekParquet dataset | `manifest.json` with `format` `seekparquet`, `format_version` 0, `draft_revision` 1, and the `seekparquet.*` stamps in every part's footer | the SeekParquet specification |
| swath schema | `schema_id`: exactly one of the [registered ids](#schema-registry), compared byte for byte, never by prefix or after normalization | this page; executable as `reader.Schema` |
| swath metadata | `extensions.swath` with `version` 1 and well-formed members | [The swath extension](#the-swath-extension) |

A SeekParquet dataset without a registered schema id or without a valid `extensions.swath` is not a swath
dataset. Any SeekParquet reader can still open it, and generic SeekParquet reads ignore `extensions.swath`
entirely. A swath dataset can also be partial: a listing stopped by `--max-keys`, or one with filters, is still
a swath dataset, and opens and reads like any other.

The reader opens and reads; it does not judge a dataset. Its operations (the `reader` names are given; other
readers make the same distinctions):

| Operation | What it establishes | Reads |
|---|---|---|
| Identification (`Open`, `OpenDir`) | The manifest is SeekParquet format version 0, draft revision 1 and the schema id is registered; when `extensions.swath` is present, its `version` is 1 and its members have the right JSON types | the manifest only |
| Validation (`CheckSchema`) | The manifest's key declaration and every part's footer, stamps and columns match the schema id's row contract | every footer and each row group's first key page |

The thin [reader API](../reader/README.md) supplies metadata, the schema registry,
`CheckSchema` and concrete typed decoders. Read through `Dataset.SeekParquet()`:
SeekParquet owns `Rows`, `List`, `Plan.Scan`, `Plan.ScanParallel`, `Aggregate`,
`AggregateRows` and `Page`. Callers choose named projections and reductions, filters,
value budgets and selected-key validation; no common object projection erases
source-specific fields.

SeekParquet `Page` reuses established listing row positions for complete-prefix
reductions. Free unfiltered counts add no value reads; unknown boundaries, clipped
prefixes or unchecked contributing row counts remain unknown. Budgeted sums charge
unique compressed measure/filter pages and dictionaries, with explicit result
statuses. These value-only reads do not certify unread keys; request
`AggregateOptions.CheckKeys` when selected-key validation is needed. `Sum` skips
null and absent values; callers requiring complete sizes also request `CountNulls`
and `CountAbsent`. A browse caller can select a 4 MiB value budget without imposing
that policy on other readers. SeekParquet defines execution and exactness in its
[Go API](https://pkg.go.dev/github.com/varveio/seekparquet/go).

Concrete decoders preserve nullable values and source-specific fields. They check
projection, physical value types and required nulls; the application owns additional
acceptance rules such as nonnegative sizes, timestamp ranges, local kind selection
and Azure class mapping. `CheckSchema` supplies the full registered column check.

Whether a dataset suits a purpose is the application's decision. The manifest records how the listing ran (its
source, scope and coverage, `one_filesystem` included) and when it was published; the application reads that
metadata and decides whether a partial, filtered or prefix-scoped listing will do, whether it is of the source it
expects, and whether a dataset opened again is the one it saw before. Part digests are for checking a dataset
after it is copied or moved, not for ordinary reads. `swath verify` is planned
([#391](https://github.com/varveio/swath/issues/391)); meanwhile, use
[SeekParquet's validator](https://github.com/varveio/seekparquet) on a local copy (see the “Validating”
section of its `go/README.md`). From the root of a SeekParquet checkout that includes `cmd/seekparquet`
and supports this dataset's format (format version 0, draft revision 1), run the following. swath pins
`github.com/varveio/seekparquet/go v0.1.1-0.20261112184929-7264a9f88420` in `go.mod`; use that
revision or one with a matching format, rather than assuming a published tag includes the command.

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
DuckDB installed. It checks swath metadata, schema-specific rules, row counts, key order and bounds,
footer stamps and part SHA-256 digests independently of the Go reader. It expects the producer's flat
`data/part-NNNNN.parquet` layout and `run.json`, so it is not a general validator for a bucket dataset
copied locally with its nested run/attempt part paths. See the [verifier guide](internals/testing.md#verifypy)
for its checks and optional reference comparison.

`Open` alone does not prove a dataset is swath's. A manifest with no `extensions.swath` at all opens
(`reader.Metadata.Swath` is false, and `swath show` prints "no swath metadata"). An extension that is present
but has no `version`, or an unsupported one, is refused.

The manifest is the authority for which parts belong to the dataset and for its commit: a directory without
`manifest.json` holds no dataset, and one with it holds exactly the parts it lists, in key order. A part on its
own is inspectable Parquet, not evidence that a dataset exists, is complete, or was published.

**Layout.** `manifest.json` (the commit, installed last), `data/part-NNNNN.parquet` (the parts the manifest
lists), `run.json` (the run report: operational, written after the commit and never part of the dataset), and
`.swath/` (the run's private workspace, which readers ignore). The full tree and the publication order are in
[internals/parquet-output.md](internals/parquet-output.md#publication-gate-and-order).

## A dataset in a bucket

`swath list SOURCE -o gs://BUCKET/PREFIX --work-dir DIR` (or `-o s3://BUCKET/PREFIX`, or
`-o az://ACCOUNT/CONTAINER/PREFIX`) writes the same dataset straight to Google Cloud Storage (or S3, or Azure
Blob Storage); nothing of it is written to local disk. Readers see the same profile, with two differences in layout:

- **Part names are unique to a run and an attempt:** `PREFIX/data/RUN/ATTEMPT/part-NNNNN.parquet`, where RUN is
  the run's UUID (kept across its resumes) and ATTEMPT the invocation's id. Ordinals stay dense and in key
  order (`part-NNNNN`), but parts of one dataset may sit under several attempt directories when the run was
  resumed. The manifest's `parts[].path` names each one; a reader takes them from the manifest and never from a
  listing of `data/`, which may also hold parts no manifest names (below).
- **The manifest is created, never replaced:** `PREFIX/manifest.json` is written only if absent (GCS
  `ifGenerationMatch=0`, S3 and Azure `If-None-Match: *`), so of two runs writing one prefix exactly one commits. `PREFIX/run.json` is written by
  the run that committed, after the commit, only if absent. A prefix that already holds a manifest is refused
  before the run starts: a dataset in a bucket is not replaced (choose a new prefix per run).

Parts are uploaded as they close and are individually visible before the commit; an upload aborted before
its completion leaves no object, and one whose completion failed ambiguously may. Until the manifest exists the
prefix holds no dataset, whatever `data/` holds. The committing run deletes every part object it created that
its manifest does not name, only at the version its upload recorded (the GCS generation; the S3 version id,
or its ETag as `If-Match`; the Azure ETag as `If-Match`) (retries, attempts cut short); a run
that loses the race deletes its own. An object at one of the run's names that it never recorded creating is
left. A commit whose outcome is unknown (the creation failed ambiguously and no manifest is there yet) deletes
nothing: the manifest may still appear, and every part it names must then exist. Objects a killed run left
behind are cleaned by its resume; an abandoned run's are not cleaned by anything, and are named in its work
directory (`.swath/remote/allocated`).

`DIR`, the work directory, is local and holds what a resume needs: the lock, the journal, spill, a claim
receipt per part (`.swath/remote/claims/`), the names allocated before their uploads began, the candidate
manifest and, once committed, `commit.json` (finalized once the orphans are cleaned and `run.json` is in place). `swath resume DIR` continues the run against the recorded
destination. [internals/durability.md](internals/durability.md#a-dataset-in-a-bucket) has the protocol;
[production.md](production.md#writing-to-a-bucket) the operating guide.

## The swath extension

SeekParquet allows only its own members at the top level, in `key` and in each part. A producer's metadata
goes under `extensions`, by namespace. swath writes `extensions.swath` only:

```json
{
  "format": "seekparquet",
  "format_version": 0,
  "draft_revision": 1,
  "schema_id": "swath/s3-objects/v1",
  "key": {"column": "key", "unique": true},
  "extensions": {
    "swath": {
      "version": 1,
      "published_at": "2026-11-08T18:00:00.123456Z",
      "source": {"kind": "s3", "uri": "s3://bucket/logs/", "container": "bucket"},
      "scope": {"kind": "prefix", "prefix": "logs/"},
      "coverage": {"complete": true, "stop_reason": "completed", "skipped_entries": 0,
                   "exclusions": [], "filters": [], "snapshot": false,
                   "listing_started_at": "…", "listing_finished_at": "…"},
      "committed_object_bytes": 12345678
    }
  },
  "parts": [
    {"path": "data/part-00000.parquet", "rows": 1000, "bytes": 81234,
     "min_key_base64": "bG9ncy9h", "max_key_base64": "bG9ncy9r", "sha256": "…"}
  ]
}
```

A local listing's extension also records its filesystem's space (`filesystem`), for example:

```json
"source": {"kind": "local-fs", "uri": "/System/Volumes/Data"},
"filesystem": {"total_bytes": 994662584320, "free_bytes": 462012616704,
               "available_bytes": 430573547520, "measured_at": "2026-11-12T20:50:36.03Z"}
```

The seekparquet module (`seekparquet.MarshalManifest`) writes the manifest, and its member order is the
module's. JSON readers must not depend on that order.

| Member | Meaning | Read by |
|---|---|---|
| `version` | The version of this extension's contract: `1`. Written first | every swath read of a manifest that has the extension: identification refuses it absent, `null`, non-integer or any other value ("unsupported swath metadata version"), as do `swath show` and `--seed-from` |
| `published_at` | When the manifest was written (RFC 3339, UTC). It does not date the source's state | the application; absent is not an error |
| `source.kind` | `s3`, `gcs`, `azblob` or `local-fs` | the application; `--seed-from` |
| `source.uri` | What was listed: `s3://bucket/prefix`, `gs://…`, `az://account/container/…`, or the absolute root path | the application: an Azure container's name is unique only within its account, which only `source.uri` records |
| `source.container` | The bucket, or the Azure container's name; absent for a filesystem | the application; `--seed-from` |
| `scope.kind` | `whole` (the bucket, container or tree) or `prefix` | the application |
| `scope.prefix` | The prefix, with `prefix`; omitted when empty | the application; `--seed-from` checks it against the run's prefix |
| `coverage.complete` | The listing reached the end of its scope | the application |
| `coverage.stop_reason` | `completed`, or why the listing stopped early (`max_keys`, `unreported`, ...) | the application |
| `coverage.skipped_entries` | Entries the listing could not read | the application |
| `coverage.filters` | The filters the run applied, as given (`include=…`, …), and the local options that leave entries out: `one_filesystem` (`--one-file-system`), `no_dirs` (`--no-dirs`) | the application |
| `coverage.exclusions` | What the listing left out by design (an output directory inside a listed tree) | informative |
| `coverage.snapshot` | Always `false`: no source-wide point-in-time guarantee | informative |
| `coverage.listing_started_at`, `listing_finished_at` | Bounds on when the entries were observed, across resumes | informative |
| `committed_object_bytes` | The dataset's object bytes: the `size` column summed over every row (a local tree's files only, not its directories; a null size not counted), as the parts' writers counted them, so a resumed dataset states the same total as an uninterrupted one. Absent when unknown (a schema without a size column, or a resumed run whose earlier parts were written by a build that did not count them); `0` is a real total. Added under `version` 1 | the application; `reader` (`Metadata.CommittedObjectBytes`); the run report's `output.committed_object_bytes` |
| `filesystem` | A local source's filesystem space, as the operating system reported it when the listing ended: `total_bytes`, `free_bytes` (including space reserved for the superuser), `available_bytes` (what an unprivileged writer may use) and `measured_at`. It describes the filesystem that holds `source.uri` (for a symbolic link to a file, which lists as the link, the link's own; for a link to a directory, which is walked through, the target's); with `--one-file-system` that is the filesystem the walk stayed on, without it nested mounts are listed but not described. Measured with `statfs` on Linux (fragment units) and macOS, `GetDiskFreeSpaceEx` on Windows, where the total and available bytes are within the caller's quota and the free bytes are the volume's, so free can exceed total. Absent for a remote source, on other platforms, when the call fails, for a filesystem that reports a size of 0, and when the root no longer names what the listing started from (a link retargeted, a directory replaced). It is what the filesystem says of itself, not a disk's capacity: volumes that share a pool (an APFS container, a ZFS pool) each report the pool's free space, so it is not summed across datasets, and the dataset's size is not `total_bytes` minus `free_bytes` (scope, hard links, shared blocks, snapshots and the filesystem's overhead differ). Added under `version` 1 | the application; `reader` (`Metadata.Filesystem`) |

Every member but `version` is optional to a reader: an absent or `null` member is reported as absent (a missing
`coverage.complete` is never read as `false`, and a missing `filters` is never read as empty), and an application
that needs one decides what its absence means. A listing with `coverage.complete` true, `stop_reason`
`completed`, no skipped entries and no filters covers its whole scope minus its `exclusions`, which are left out
by design. swath publishes the complete result of the live listing it performed; it is not a point-in-time
snapshot of a source that changes during the run.

Readers ignore members they do not define and keep the raw extension (`reader.Metadata.Manifest`), so an
informative member can be added under `version` 1. A member of the wrong JSON type is malformed: `reader`
reports it as `KindMetadata`, while an unsupported version is reported as `KindSchema`.

## Schema registry

A schema id is `swath/<row-contract>/vN`: the row contract it names and that contract's version. Each id fixes the
columns a reader may rely on: their names, physical and logical types and nullability, the key's encoding and
uniqueness, and the meaning of a row. `reader.Schema(id)` returns the executable descriptor, and `CheckSchema` checks
parts against it. All producers, including Azure Blob, bind their schemas to these descriptors (`schema.Bind`).
`TestProducerSchemasGolden` pins the five S3, GCS and local schemas; Azure's `TestSchemaBinding` checks its
binding, accessor order and null handling.

| Schema id | A row is | Key | Keys unique | Longest key |
|---|---|---|---|---|
| `swath/s3-objects/v1` | an S3 current object | UTF-8 (`STRING`) | yes | 1,024 bytes |
| `swath/s3-versions/v1` | an S3 version or delete marker (`--all-versions`) | UTF-8 | no: one row per version | 1,024 bytes |
| `swath/gcs-objects/v1` | a Cloud Storage live object | UTF-8 | yes | 1,024 bytes |
| `swath/gcs-generations/v1` | a Cloud Storage generation, live or noncurrent (`--all-versions`) | UTF-8 | no: one row per generation | 1,024 bytes |
| `swath/azblob-objects/v1` | an Azure Blob current blob | UTF-8 | yes | 3,072 bytes (1,024 UTF-16 units) |
| `swath/azblob-versions/v1` | an Azure Blob version, snapshot, soft-deleted blob or current blob (`--all-versions`) | UTF-8 | no: one row per entry | 3,072 bytes |
| `swath/local-entries/v1` | a filesystem entry below the root, directories included | arbitrary bytes (unannotated `BYTE_ARRAY`) | yes | none |

Every column after the key is nullable unless marked required. Types: `STRING` is `BYTE_ARRAY (STRING)`, UTF-8;
`binary` is unannotated `BYTE_ARRAY`; `int64` is `INT64` (`INT(64,true)` or unannotated); `uint32` and `uint64` are
`INT32 INT(32,false)` and `INT64 INT(64,false)`, read over their full unsigned range; `timestamp` is `INT64
TIMESTAMP(MICROS, UTC)`; `bool` is `BOOLEAN`.

**`swath/s3-objects/v1`**: `key`, `size` int64, `last_modified` timestamp, `etag` STRING (one pair of surrounding
quotes removed), `storage_class` STRING, `owner_id` and `owner_display_name` STRING (null unless the run used
`--s3-fetch-owner`), `checksum_algorithm` STRING (the first algorithm S3 lists), `checksum_type` STRING. `etag`,
`storage_class` and the checksums are null when S3 omits them.

**`swath/s3-versions/v1`**: the `swath/s3-objects/v1` columns in the same order, then `version_id` STRING
(verbatim; `"null"` is a real version id), `is_latest` bool (required) and `is_delete_marker` bool (required).
`size`, `etag`, `storage_class` and the checksums are null on a delete marker.

**`swath/gcs-objects/v1`**: `key` (the object name), `size` int64, `last_modified` timestamp (`updated`), `etag`
STRING (the JSON API's opaque entity tag), `generation` and `metageneration` int64, `storage_class` STRING, `md5`
and `crc32c` STRING (base64, as listed), `content_type` STRING, and last `time_created` timestamp
(`timeCreated`). `time_created` is the one column of this id a part may lack: parts written before the lister recorded it
do not have it, and a reader reads it as null there. `last_modified` also moves when only the object's metadata
changes, and `etag` is not a content hash. Compare content with `md5` or `crc32c`. GCS gives no `md5` for composite
objects and neither hash for objects encrypted with customer-managed or customer-supplied keys.

**`swath/gcs-generations/v1`**: the `swath/gcs-objects/v1` columns, then `time_deleted` timestamp: null for the
live generation. There is no `is_latest`.

**`swath/azblob-objects/v1`**: `key` (the blob name), `size` int64 (`Content-Length`), `last_modified` timestamp
(`Last-Modified`, at the wire's 1 s resolution), `etag` STRING (opaque, as listed), `content_md5` STRING (base64
as listed; null when absent, as for blobs uploaded in blocks without one), `content_type` STRING, `blob_type`
STRING (`BlockBlob`, `PageBlob`, `AppendBlob`), `access_tier` STRING (null when absent), `creation_time`
timestamp (`Creation-Time`). As on GCS, `last_modified` moves with metadata-only changes and `etag` is not a
content hash: compare content with `content_md5`, and never compare ETags across stores. Then the columns added
to the schema id later, which a part written before lacks (read them as null there): `access_tier_inferred` bool
(true when the tier is the account's default, not set on the blob), `access_tier_change_time` timestamp,
`archive_status` STRING (`rehydrate-pending-to-hot`, `-cool`, `-cold`), `rehydrate_priority` STRING (`High`,
`Standard`), `last_access_time` timestamp (only with the account's last access tracking on),
`content_encoding` STRING, `server_encrypted` bool, `encryption_scope` STRING, `customer_provided_key_sha256`
STRING, `lease_state` STRING (`available`, `leased`, `expired`, `breaking`, `broken`), `tag_count` int64, and
the columns a run asks for, null unless it did: `metadata` and `tags` STRING (a JSON object of names to
values, keys sorted; `{}` for none; `metadata` null for a blob encrypted with a customer-provided key, whose
metadata a listing cannot read), `immutability_policy_until` timestamp, `immutability_policy_mode` STRING
(`Unlocked`, `Locked`) and `legal_hold` bool (false for none). Each is as Azure lists it, null where it lists
nothing. A timestamp the Azure SDK decodes as `0001-01-01T00:00:00Z` (its value for a missing time) is null too.
The Python comparison reader supplies typed nulls for these added columns when an entire
current-object dataset lacks them, and unions parts by name when only some parts lack them.

**`swath/azblob-versions/v1`**: the `swath/azblob-objects/v1` columns in the same order, then `version_id` STRING
(as listed, a timestamp; null for a snapshot and on an account without versioning), `is_latest` bool (required:
the name's current blob, live, the row a current listing has), `snapshot` STRING (the snapshot's time as listed;
null unless the row is a snapshot), `is_deleted` bool (required: soft-deleted), `deleted_time` timestamp and
`remaining_retention_days` int64 (until the soft-deleted entry is purged; null unless deleted). A soft-deleted
entry keeps its size and metadata: it is stored and billed until purged. There are no delete markers.

**`swath/local-entries/v1`**: one row per entry below the root, directories included (`--no-dirs` omits them and
records `no_dirs` in `coverage.filters`):

| Column | Type | Source |
|---|---|---|
| `key` | binary, required | the path relative to the root, `/`-separated, no leading `/`; a directory's ends in `/`; the name's exact bytes |
| `size` | int64 | `st_size` (apparent bytes); a directory's row carries its own, so a byte total excludes `kind = 'DIR'` (or is `swath du`'s): a plain `sum(size)` exceeds `swath du`'s total |
| `last_modified` | timestamp | `st_mtime` |
| `kind` | STRING, required | `FILE`, `DIR`, `SYMLINK`, `FIFO`, `SOCKET`, `CHAR_DEVICE`, `BLOCK_DEVICE`, `UNKNOWN` |
| `mode` | uint32 | `st_mode & 0o7777` (permissions, setuid, setgid, sticky) |
| `uid`, `gid`, `nlink` | uint32 | `st_uid`, `st_gid`, `st_nlink` |
| `inode`, `dev` | uint64 | `st_ino`, `st_dev` (statx: `makedev(major, minor)`) |
| `allocated_bytes` | int64 | `st_blocks * 512` (a sparse file has `allocated_bytes < size`) |
| `metadata_changed_at`, `atime` | timestamp | `st_ctime`, `st_atime` |
| `birth_time` | timestamp | statx `stx_btime` (`--metadata statx` only, when the filesystem reports it); macOS `st_birthtime` (stat tier); Windows `CreationTime` |
| `link_target` | binary | the `readlink` result, symlinks only (unless `--no-link-target`); exact bytes on Linux and other Unix systems, while the portable engine on Windows writes it with `/` separators (`filepath.ToSlash`) |

Under `--metadata names` (no stat) the stat columns are null.

**Local reductions.** SeekParquet counts selected rows of every kind, including a
directory's own row and version rows where present. A prefix `d/` includes its own
row `d/` once; the root has no row. Summing `size` without a kind filter gives
**entry bytes**, directories' own `st_size` included, rather than files' content
bytes or disk usage (`allocated_bytes`). A directory's `size` and `last_modified`
are its own, never its subtree's. For files-only totals the application selects
`kind` through SeekParquet's `BatchFilter`. Under `--metadata names`, sizes are
null: generic sums skip them, so a caller requiring known sizes also checks
null/absent counters. These interpretation and completeness policies belong to
the application.

## Keys and order

This section is for consumers that read the parts without `reader`: DuckDB, Python, Spark.

- **Read the manifest's exact file list.** The dataset is the parts `manifest.json` lists, in the listed order.
  It is not every file under `data/`: a run in progress publishes parts there before its commit, and an
  overwrite removes them. Read the parts by name: optional columns can be absent from some parts (`read_parquet
  ([...], union_by_name = true)` in DuckDB). Columns a schema id does not define may appear anywhere and are
  ignored.
- **Order is unsigned byte order.** Rows ascend by `key`'s bytes compared as unsigned bytes, within each part and
  across parts in manifest order. Keys are never normalized (Unicode, case, separators). Cloud keys are the
  provider's UTF-8 bytes. A local key is the name's bytes exactly as the kernel returned them, which need not be
  UTF-8. Manifest bounds are standard base64 of the key bytes: decode them and compare bytes, never the base64
  text.
- **Azure order is byte order after adaptation.** List Blobs returns names in UTF-16 code-unit order, which
  puts names with characters at U+10000 and above before those at U+E000 to U+FFFF. swath's adapter reorders
  them, so an Azure dataset is in byte order like every other one, not in the service's listing order.
- **Versions rows are consecutive per key.** In `swath/s3-versions/v1`, `swath/gcs-generations/v1` and
  `swath/azblob-versions/v1` a key
  repeats once per version, on consecutive rows, and a key's group never spans two parts. On S3 the order within
  a group is the service's: newest first, with versions and delete markers interleaved. No column rebuilds it,
  since two entries can share `last_modified`. On GCS rows order by key and then by numeric `generation`, strictly
  ascending; the lister refuses a page that breaks this. The `is_latest`, `is_delete_marker` and `time_deleted`
  values are the service's, per row. In an unfiltered, complete listing, the first row of an S3 group is the one
  with `is_latest` true, and a key whose first row is a delete marker is deleted. On GCS the last row is the newest
  generation, live only when `time_deleted` is null, and a name may have no live generation. A filter (`--min-size`,
  `--include`, ...) can remove a key's latest version, so in a filtered dataset none of those positional inferences
  hold. The current objects are `is_latest AND NOT is_delete_marker` (S3), `time_deleted IS NULL` (GCS) or
  `is_latest` (Azure Blob), row by row. On Azure Blob a group is in the service's order: a name's entries keyed
  by their time within the name (snapshots and versions oldest first; the base blob or current version, the one
  with `is_latest`, last). The reader module describes version roles with `VersionSpec` on `SchemaSpec.Versions`: version ID,
  latest or deleted-at, and optional delete marker ([version roles](../reader/README.md#version-roles)).
  swath's text output, version counts and per-prefix totals read the rows through it. Typed reader decoders and
  schema-specific SQL still bind columns by name.
- **Stored order is physical.** `ORDER BY key` alone does not recover the order among equal keys, and a generic
  SQL scan does not promise stored order. To keep a group's order, read the parts in manifest order and number the
  rows as read (DuckDB: `read_parquet` with `file_row_number = true`, then order by the part's position in the
  manifest, then the row number). A row's offset is its zero-based position in its group of equal keys. A row
  cursor `(key, offset)` continues after that row, at offset + 1 of the same key, then at the next key
  ([SeekParquet PAGE-1](https://github.com/varveio/seekparquet/blob/7264a9f88420/spec/format-1.md)). A cursor is
  always a row's: `Cursor{Key: k}` is the cursor after row 0 of `k`, not "after key `k`". To skip every row
  of `k`, use an exclusive key bound (`seekparquet.Span` with `Start` = `k` and `StartExclusive`), which skips the group's
  remaining versions. An offset is valid only for the same immutable dataset and is not a durable version id.
- **Null is unknown, not zero.** A null `size` or time is not 0 or the epoch. SQL `SUM` and `MAX` skip nulls,
  so a total over a column with nulls is a lower bound. Count the nulls beside it, as `swath du` reports `unsized`.
  SeekParquet reductions also skip nulls and absent values; request their counters to enforce an
  application's completeness policy, and check each result's status before using its value.

## Footer stamps

Every part's footer carries SeekParquet's five stamps, equal to the manifest's: `seekparquet.format_version`,
`seekparquet.draft_revision`, `seekparquet.schema_id`, `seekparquet.key_column` (`key`) and
`seekparquet.key_unique`. Within-part order is the standard `sorting_columns` (`key`, ascending). swath adds no
stamp of its own. A footer identifies the row contract its part claims; it cannot show that the part belongs to
a dataset, whether parts are missing, or what the listing covered. Those are the manifest's.

## Evolution

| Identifier | May change without a new value | Forces a new value | Who decides |
|---|---|---|---|
| SeekParquet `format_version` / `draft_revision` | editorial clarifications | any change to which bytes are accepted or what a query returns | SeekParquet maintainers, through proposal and conformance review |
| `schema_id` | added nullable, absence-tolerant columns that leave existing rows and columns unchanged; writer layout (row groups, pages, encodings, codecs) | a renamed or removed column, a changed type, signedness, time unit or nullability, a changed key encoding or uniqueness, a changed meaning of a row; type widening is not automatically compatible | swath maintainers, with the consumers |
| `extensions.swath.version` | added informative members that readers may ignore | a change to an existing member's meaning. When an older application could misread the dataset, the schema id changes too, because older readers ignore members they do not know | swath maintainers |
| Writer recipe (`parquetout.Recipe`, in the resume identity) | changes that keep its byte-reproduction promise | a change to how a part is reproduced byte for byte | swath writer maintainers |
| Journal version (private to a run) | changes that existing recovery reads safely | a representation existing recovery cannot read | swath recovery maintainers |

Readers ignore unknown columns wherever they are placed, and physical column order is not part of the contract:
columns are looked up by name. Each part's `created_by` names the build that wrote it, so a resumed run can hold
parts from two builds. The tool's release version moves independently of every identifier above.
