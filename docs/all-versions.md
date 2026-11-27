# List every version

`--all-versions` lists every S3 version and delete marker, every GCS generation, or every Azure Blob version,
snapshot and soft-deleted blob, instead of the current objects only.

```bash
swath list --all-versions s3://BUCKET[/PREFIX] -o DIR   # every version and delete marker (ListObjectVersions)
swath list --all-versions gs://BUCKET[/PREFIX] -o DIR   # every generation, live and noncurrent (versions=true)
swath list --all-versions az://ACCOUNT/CONTAINER[/PREFIX] -o DIR  # versions, snapshots, soft-deleted blobs
```

**Rows.** One row per S3 version or delete marker, per GCS generation, or per Azure Blob entry (a version,
a snapshot, a soft-deleted blob, or the current blob), under the [version schemas](dataset.md#schema-registry). A delete marker's absent metadata is null, never 0.
`version_id` is verbatim: `"null"` is a real ID (an object written while versioning was off). The current
objects are `is_latest AND NOT is_delete_marker` (S3) or `is_latest` (GCS and Azure Blob);
a listing of the current objects alone is the default mode, which is cheaper.

**Order.** Keys ascend; a key's rows are consecutive, in the service's order within the key: S3 newest first,
versions and delete markers interleaved; GCS by generation, ascending; Azure Blob as the service lists a name's
entries, keyed by their time within the name: snapshots and versions oldest first, the current blob last. S3 can list two entries of a key with the
same `last_modified` (to the second), so that time does not rebuild S3's order: the file order does.
In an unfiltered, complete listing, the first row of an S3 key's group is the one with `is_latest`, and a
delete marker there means the object is deleted. swath checks this: a store that marks a later entry of a key latest
stops the run (`cause` `protocol`), rather than writing a second `is_latest` row or a key's entries out of order.
Backblaze B2 does both ([S3-compatible stores](internals/providers/s3.md#s3-compatible-stores)). On GCS, the last row of a name's group is the newest
generation; it is live (`is_latest`) only if `deleted_at` is null, and a GCS name may have no live generation. On Azure Blob
a name's `is_latest` row, when it has one, is its last; a versioned blob that was deleted has only previous
versions, and a soft-deleted one only `is_deleted` rows. A filter can
remove a key's latest version, so in a filtered dataset read `is_latest` per row instead of
by position.

**Parts never split a key.** A part's size target, `--part-max-rows` and `--part-interval` cut at the
next key, so a part can pass them by one key's history; parts stay key-disjoint, and a resume never lists half a
key again. The interval does not bound how long a part stays open while one key's rows are still arriving.

**Parallelism.** Ranges split only between keys, so one key with a very long history is listed by one worker,
page after page, while the rest of the bucket proceeds. S3's versions call is slower than ListObjectsV2: about 1.85 times the time per request at the median of
our measurements, and pages of old versions are slow to first byte; history multiplies the pages besides. GCS
lists generations at the cost of live objects. Azure Blob lists every entry with the same call and the same
format (Arrow where the account has it): each request costs a listing operation, as always, and the entries
multiply the pages; Microsoft recommends fewer than 1,000 versions per blob for listing performance.

**Permissions and refusals.** S3 needs `s3:ListBucketVersions` (`s3:ListBucket` alone answers 403, and swath
never falls back to a current-object listing); GCS needs the same `storage.objects.list`; Azure Blob the same
permission to list (a SAS with `l`, anonymous access to a public container). Text rows carry the
versions schema's columns: S3's `version_id`, `is_latest` and `is_delete_marker` (a delete marker's `size` and
`etag` null), GCS's `generation`, `is_latest` and `deleted_at` (null on the live generation), Azure Blob's `version_id`,
`is_latest`, `snapshot`, `is_deleted`, `deleted_at` and `remaining_retention_days`; the table shows the version
ID (empty for an Azure snapshot), `latest` and, on S3, `delete_marker`. `--min-size` and `--max-size` never match a delete marker. A resume
continues in the mode its run started in; a run in the other mode is refused on that output. Directory buckets have
no versions and stay refused.

The run report's version counts (under `output`) describe this attempt after filtering: S3 `versions` and
`delete_markers` sum to emitted rows; GCS and Azure Blob `versions` include the `noncurrent` rows (on Azure every
row that is not a name's latest: previous versions, snapshots, soft-deleted blobs).
`distinct_keys` counts surviving names, and `live_objects` counts live versions among those rows.
Table output includes the version ID and latest state,
and for S3 the delete-marker state, in wire order.
