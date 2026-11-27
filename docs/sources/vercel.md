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

Vercel always uses the folder walk. It supplies no arbitrary pathname seek, so
range-engine diagnostic controls are refused. Independent folders can run
concurrently. The default is four workers; `--workers` can raise or lower it,
and the adaptive limiter may reduce request concurrency when the service throttles.
A large flat folder remains one cursor chain. Many tiny folders can require far
more listing requests than an expanded sequential listing. Requests may incur
service charges; swath does not estimate Vercel charges.

The shared request timeout, rate cap and adaptive concurrency controls apply.
For a store that throttles, set `--request-rate` to a suitable rate for its quota.
The service endpoint is fixed; `--endpoint-url` is refused. The native Go client
follows the official Blob SDK's version-13 listing wire format, with swath's
bounded responses, retries, cancellation and credential redaction.

## Dataset and metadata

The schema is [`swath/vercel-objects/v1`](../dataset.md#schema-registry): `key`,
`size`, `last_modified` and `etag`. `last_modified` preserves the list API's
`uploadedAt` value, normalized to UTC microseconds. This mapping follows Vercel's
[SDK `get()` implementation](https://github.com/vercel/storage/blob/bf0c1c5446618fe86d67a9297cd21a58eb9a7274/packages/blob/src/get.ts#L242-L264),
which exposes the HTTP `Last-Modified` header as `uploadedAt`. swath keeps the list
value's finer precision rather than substituting the HTTP header's whole seconds.
This does not establish
a separate creation time, nor a guarantee that metadata-only operations advance
this value. Overwrite behavior has not been independently qualified live.

Size, pathname and modification-time filters work on the reported values;
`--storage-class` is refused because Vercel supplies no storage class. The ETag
is opaque. URLs and synthetic folder rows are omitted.

Datasets can be written to a local directory or an existing supported object-store
destination. Vercel Blob itself is not an output destination. `du` works both on
the live source and on a saved dataset; its newest time uses `last_modified`.
Storage class remains unavailable in the saved inventory view.

## Limits and known differences

- Dataset runs cannot be resumed. An interrupted run must be started again.
- Current blobs only: `--all-versions` is unsupported.
- A folder is buffered whole. The defaults allow 64 MiB of staged entries per
  folder and 256 MiB across active folders. Crossing a limit fails with
  `folder_limit`; it never reports a truncated inventory as complete. The limit
  counts retained entries and names, not object content bytes.
- Responses in flight are extra to the staging limits. Each body is capped at
  16 MiB, with additional decoder buffers and strings. The worker count and
  adaptive limiter control simultaneous requests; lower `--workers` to reduce
  response memory. The report shows HTTP activity and `tree.*` staging counters.
- Prefixes without a trailing slash are preserved exactly. The walk begins at
  the containing folder and selects entries by raw prefix; it may read siblings
  outside that prefix, but Vercel's literal prefix matching keeps byte-distinct
  Unicode spellings separate.
- swath publishes the complete result of the live listing it performed. It is
  not a point-in-time snapshot of a store that changes during the run.

The API supports public and private stores, but the [initial service evidence](../internals/measurements.md#vercel-blob-conformance-2026-11-27) is a private store; public-store behavior and cursor
lifetime across long interruptions have not been established. The folder walker
does not rely on Vercel's native ordering or persist cursors between runs.
