# Vercel Blob

swath lists the current blobs of one Vercel Blob store. Use
`vercel://store_ID[/prefix]`, with the case-sensitive store ID from Vercel. The
part after the first slash is a literal pathname prefix: `%`, `+`, `?` and `#`
are not decoded or treated as URL parameters.

## Start here

Set `BLOB_READ_WRITE_TOKEN` in your environment to the store's read-write token,
then run:

```sh
swath list vercel://store_ID
swath du vercel://store_ID/uploads/
swath list vercel://store_ID -o ./inventory
```

The token must belong to the store named in the source. A missing, malformed or
mismatched token fails before a listing request. A public delivery URL does not
grant listing access: `--anonymous` is unsupported. swath only lists; it does not
upload, modify or delete blobs. Vercel's token nevertheless grants read-write
access, so keep it in the environment, outside command arguments and source URLs.
OIDC authentication is not supported in this version.

## How it lists

Vercel's folded API returns direct blobs, virtual folder prefixes and an opaque
continuation cursor. swath reads each folder to its end, validates its entries,
sorts by the raw UTF-8 bytes of their pathnames and schedules child folders.
Virtual folders create no inventory rows. A real blob named `a/` remains a real
row, emitted when listing that folder.

`--listing auto` and `--listing tree` use this walk. Vercel supplies no arbitrary
pathname seek, so `--listing range` and range-engine diagnostic controls are
refused. Independent folders can run concurrently, with at most four page requests in
flight under the shared response-memory reservation, regardless of a larger
`--workers` setting. A large flat folder remains one cursor chain. Many tiny folders can require far more listing requests than
an expanded sequential listing. Listing requests may incur service charges;
swath does not estimate Vercel charges.

The shared request timeout, rate cap and adaptive concurrency controls apply.
For a store that throttles, set `--request-rate` to a suitable rate for its quota.
The service endpoint is fixed; `--endpoint-url` is refused. The native Go client
follows the official Blob SDK's version-13 listing wire format, with swath's
bounded responses, retries, cancellation and credential redaction.

## Dataset and metadata

The schema is [`swath/vercel-objects/v1`](../dataset.md#schema-registry): `key`,
`size`, `created_at` and `etag`. `created_at` normalizes Vercel's `uploadedAt`: the
service-reported upload time of the current blob. It is neither a modification
time nor a promise of the first creation time of that pathname. The ETag is opaque. URLs and synthetic folder rows are omitted.
Modification-time and storage-class filters are refused because these fields
are not supplied. Size and pathname filters work normally.

Datasets can be written to a local directory or an existing supported object-store
destination. Vercel Blob itself is not an output destination. `du` works both on
the live source and on a saved dataset; modification times and storage classes
remain unavailable in the saved inventory view.

## Limits and known differences

- Dataset runs cannot be resumed. An interrupted run must be started again.
- Current blobs only: `--all-versions` is unsupported.
- A folder is buffered whole. The defaults allow 64 MiB of staged entries per
  folder and 256 MiB across active folders. Crossing a limit fails with
  `folder_limit`; it never reports a truncated inventory as complete. The limit
  counts retained entries and names, not object content bytes.
- Response decoding has a separate shared admission reservation, not a heap or RSS limit. The run report's
  `tree.*` counters show pages, folders, staging, waits and validation failures.
- Prefixes without a trailing slash are preserved exactly. The walk begins at
  the containing folder and selects entries by raw prefix; it may read siblings
  outside that prefix to establish the selection.
- swath publishes the complete result of the live listing it performed. It is
  not a point-in-time snapshot of a store that changes during the run.

The API supports public and private stores, but the [initial service evidence](../internals/measurements.md#vercel-blob-conformance-2026-11-27) is a private store; public-store behavior and cursor
lifetime across long interruptions have not been established. The folder walker
does not rely on Vercel's native ordering or persist cursors between runs.
