# Start here

swath lists what an object store or a filesystem holds, and reports what it found. It reads listing metadata
only: names, sizes, times and storage fields. It never reads, changes or deletes the contents.

A source is a local or mounted directory, `s3://bucket/prefix`, `gs://bucket/prefix` or
`az://account/container/prefix` ([sources](sources/README.md)).

## Pick a job

| You want to | Run | Read |
|---|---|---|
| Find where the space went | `swath du -d 1 SOURCE` | [Your first inventory](first-inventory.md) |
| List every entry, or export rows for a script | `swath list SOURCE`, or `-o rows.tsv.zst` | [Text formats](output.md) |
| Save a sorted Parquet inventory to query later | `swath list SOURCE -o DIR` | [Dataset](dataset.md) |

New to swath? [Install](install.md) it, then follow [Your first inventory](first-inventory.md) on a local
directory. It needs no account and no credentials.

## When swath fits

- **`du` and `find`** answer most questions about a small directory. swath walks a directory with many workers
  at once, which helps on a large directory or a slow network mount, and it can save the walk as a dataset.
- **`aws s3 ls`, `gcloud storage ls` and `rclone`** list a bucket one page of keys after another. swath
  divides the key space into ranges and lists them in parallel, which matters once a bucket holds tens of
  millions of keys.
- **Scheduled inventories** (S3 Inventory, GCS Storage Insights, Azure Blob inventory) cost less than any
  live listing when a daily or weekly report is fresh enough. Use swath when you need the answer now, for a
  prefix the report does not cover, or for a source without one, such as a local or mounted filesystem.

swath only lists: it does not copy data, schedule runs, compare inventories or send alerts.
