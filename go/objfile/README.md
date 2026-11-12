# objfile

`github.com/varveio/swath/go/objfile` provides random-access reads and create-only
uploads for S3, Google Cloud Storage, and Azure Blob Storage. It turns an object
into an `io.ReaderAt` with a known size and a pinned identity, suitable for
[SeekParquet](https://github.com/varveio/seekparquet), parquet-go, and other file
readers. Writes stream through bounded buffers and return the created object's
identity for read-back or conditional deletion.

This is a nested Go module, independently importable from the swath lister. The
core package uses only the standard library. Provider packages use their official
SDKs; all three SDKs appear in the module graph, but a program links only the
providers it imports. Importing `objfile/remote` links all three.

objfile handles individual objects. It does not list, rename, or overwrite them, or
decide which bytes a reader needs; dataset publication and recovery belong to the
application. Given a batch of ranges a reader needs, it plans the requests that
fetch them: merging, splitting and running them concurrently. An optional byte
cache keeps fetched ranges of pinned versions.

Runnable examples, checked by `go test`: [example_test.go](example_test.go) (locations and
names) and [s3/example_test.go](s3/example_test.go) (pinned ranged reads and create-only
writes against an in-process fake bucket).

## Read a remote file

`remote.Open` builds a read-only store with default credentials. Locations are
`s3://BUCKET[/PREFIX]`, `gs://BUCKET[/PREFIX]`, or
`az://ACCOUNT/CONTAINER[/PREFIX]`. The prefix is a dataset root; object names below
it are literal relative paths, without empty, `.` or `..` segments. Location URIs
cannot carry credentials, queries, or fragments.

This example reads a Parquet footer with ranged reads, not the whole object:

```go
import (
    "context"

    "github.com/parquet-go/parquet-go"
    "github.com/varveio/swath/go/objfile/remote"
)

func rowCount(ctx context.Context) (int64, error) {
    store, err := remote.Open(ctx, "s3://bucket/dataset")
    if err != nil {
        return 0, err
    }
    defer store.Close()

    object, err := store.Open(ctx, "part-00000.parquet")
    if err != nil {
        return 0, err
    }
    defer object.Close()

    file, err := parquet.OpenFile(object.WithContext(ctx), object.Size())
    if err != nil {
        return 0, err
    }
    return file.NumRows(), nil
}
```

`Object` is safe for concurrent reads. Use `WithContext(ctx)` to cancel reads;
plain `ReadAt` uses `context.Background()`. Keep the store open until its objects
are finished. A missing object wraps `fs.ErrNotExist`; reads after an object's
`Close` wrap `fs.ErrClosed`.

Each object pins the identity learned by `Open`: an S3 version id and ETag, a GCS
generation, or an Azure ETag. Reads check the returned identity and offset, and the
size and length a response states. A changed or deleted pinned object yields `ErrObjectChanged`; an invalid
range response yields an error. S3 objects without a unique version id (including
`"null"`) use the ETag as `Identity`: a replacement with the same ETag and size
cannot be distinguished. Pinning does not provide a dataset-wide snapshot.

**Custom GCS clients need care:** after a response body fails partway, the SDK can
retry a range without checking its offset and return wrong bytes without an
error. The clients built by `remote.Open` and `remote.OpenDestination` install a
validating transport, which checks each response against the one byte range its
request names; every objfile request, a batched one included, names one range.
See the [GCS package contract](gcs/gcs.go) before supplying your own client.

## Open a SeekParquet dataset

`spsource.New(opener)` is a SeekParquet `Source` over an `Opener`, for example
`spsource.New(objfile.Pin(objfile.Limit(store, 64), store.Scope()))`. SeekParquet
decides which bytes to read and objfile how: a part opens with its manifest size
(`OpenSized`, no metadata request), the manifest is one whole-object read
(`ReadObject`), and the reader's batches of ranges are `ReadRanges`. objfile's
errors keep their meaning for SeekParquet: `ErrObjectChanged`, a
`*SizeMismatchError` and `ErrTooLarge` also match SeekParquet's own. The adapter
translates SeekParquet's operation cache mode to objfile's: `CacheBypass`
disables byte-cache lookup and admission for scalar and batched reads.

## Read several ranges, or a small object whole

A reader that knows several ranges it needs reads them as one batch. The ranges
may be in any order and may touch or overlap; their buffers must not share memory:

```go
ranges := []objfile.Range{
    {Off: indexOff, Buf: make([]byte, indexLen)},
    {Off: pageOff, Buf: make([]byte, pageLen)},
}
if err := objfile.ReadRanges(ctx, object, ranges); err != nil {
    var batch *objfile.BatchError // which ranges failed; the others are filled
    ...
}
```

The batch's bytes are planned greedily, in object order, under `BatchPolicy`:
touching and overlapping ranges are merged, runs longer than 4 MiB are split into
near-equal requests, and a request joins the one before it across a gap of up to
64 KiB while at most a quarter of it is discarded (a plan that need not be the
fewest requests). The requests run concurrently, at most 16 and 64 MiB a batch
(strictly: a request is split to fit the byte allowance), each pinned and resumed
after a cut body as a `ReadAt` is. A ranged GET costs about one round trip whether
it returns 4 KiB or 4 MiB, so the policy saves round trips at a bounded over-read.
`Batch` sets another policy; a field left 0 takes the default (the zero policy is
the default), and a negative one is refused; `DisableGapBridging` reads no gap,
merging only ranges that touch or overlap. Cancelling the context cancels the
requests in flight, and `ReadRanges` returns once they have stopped. A failed
request fails only the ranges it covers (`BatchError`).

A batch is at most `MaxBatchRanges` ranges (4096) and `MaxBatchBytes` (128 MiB);
a larger one, or a range outside the object, fails whole before any request.
Empty ranges, through the object's end, need none. The buffers of one batch must
not share memory. While an `OpenSized` object is not yet pinned, its reads, of a
batch or not, are sent one at a time until one pins it, so that no response holds
a connection that the metadata request a proof may need is waiting for; one that
shows the object missing, changed or of another size ends a batch, failing every
range it has not read. Through `Pinned`, each of a batch's requests is checked
against the recorded pin as it ends, so a replaced object stops the batch at its
first request.

`ReadObject` reads a small object, such as a manifest, whole in one request with
no metadata request first, refusing one larger than its limit (`ErrTooLarge`,
without reading the body when the response states its length; one that does not
is read up to a byte past the limit), and returns the identity it was served. A
body of known length cut short is read on as a pinned ranged read, never resumed
by the SDK; a body of unknown length that fails, or is cancelled, fails the read.

For more control:

- A `remote.Store`'s connection pool keeps up to 64 idle connections to its host,
  for four concurrent default batches (16 requests each) or a `Limit` of up to 64, so
  a burst after the first finds its connections open; active connections are not
  bounded. For a client of your own, keep idle connections per host for the requests
  it may have in flight: the `Limit` you wrap it in, or else
  `BatchPolicy.MaxConcurrent` times the batches you run at once (Go's default
  transport keeps 2).
- Build an SDK client and pass it to `s3.New`, `gcs.New`, or `azblob.New`; wrap the
  opener with `Root` for path confinement and `Limit` for bounded requests. `Limit`
  holds a slot per request: each `Open`, `OpenSized(…, 0)`, `ReadObject` and `ReadAt`,
  and each request of a batch, so a batch never has more requests in flight than
  the limit.
- An expected size of 0 follows one rule (`OpenEmpty`): no range read proves an
  empty object, so the store's metadata request does, through every wrapper (a
  `Limit` slot held, `Pin` judging it), and its size is compared with 0 before an
  Object is returned or a pin recorded; a wrapper's size hint is never evidence.
- Use `OpenSized` when a manifest already gives the size. For a positive size it
  makes no request (size 0 is the exception above, `OpenEmpty`); the first read
  pins the version, once its response states a total size equal to the
  one given (a response that states none is let go, one metadata request proves the
  size and version, and the range is requested again), and returns no byte before.
  Another size is a `*SizeMismatchError` with the size given and the size observed
  (a range past a shorter object's end is checked by a metadata request), not
  `ErrObjectChanged`, which means the object changed after it was pinned; a missing
  object wraps `fs.ErrNotExist`. `Size()` is only the size given until then, and
  `Identity()` is empty.
- `KnownSizes` turns `Open` of names whose sizes a reader already holds into
  `OpenSized` (a name mapped to 0 then makes the metadata request `OpenEmpty`
  requires), for a reader whose open carries no size (SeekParquet's
  `Source.Open`); an explicit `OpenSized` keeps the size it is given. It composes
  inside or outside `Pin`, which checks an Object its inner `Open` returns unpinned
  at the first read, as it checks `OpenSized`'s.
- Use `Pin` across opens and `Snapshot`/`Restore` across restarts, for example
  `objfile.Pin(objfile.Root(objfile.Limit(o, n), prefix), scope)`; the wrappers
  compose in any order.
  `Pinned.OpenSized` checks the identity at the first read instead of at open (at
  the open for size 0, by `OpenEmpty`'s metadata request), and
  `Pinned.ReadObject` with the read; both re-prove the current object. Against a
  recorded pin, another size is `ErrObjectChanged`. A restored snapshot
  permits only recorded names and rechecks the current objects, rather than
  reopening historical versions. It records an object when its pin is proved: an
  `Open` of a store at the open, a sized open (`OpenSized`, a `KnownSizes` `Open`) at
  its first read that reaches the store, so a snapshot taken before that read
  lacks the name and its `Restore` refuses it. The application must establish
  that every required object is covered. Use the
  store's `Scope()` to bind snapshots to its namespace.

## Observe the requests

An operation that needs to know what it cost puts an `Observer` on its context
(`WithObserver`). Every physical request a read makes under that context, through
every wrapper, is reported as it begins (after any `Limit` slot is granted) and
after its body is consumed or closed and SDK and objfile validation finishes: its kind (metadata, range or whole object),
the store's object name (a `Root` prefix included), the range, whether it retries
earlier work, the bytes consumed and the error. Each batch reports its plan
(ranges in, requests out, gap bytes read and discarded) and a byte cache the bytes
it served. Without an Observer this costs a context lookup and nothing else.

`IOReporter` says whether the observation is complete. A `remote.Store` reports
true: its clients send through a transport that reports every HTTP attempt, the
SDKs' own retries, the GCS reader's reopening of a cut body, and resends inside
Go's HTTP transport included. A store
package over a caller's client reports false: it reports each request it makes,
but not the retries the SDK makes inside one. Every wrapper reports what it wraps.

## Cache fetched bytes

`Cache(opener, scope, cache)` keeps the bytes reads fetch in a `ByteCache`, which
several stores may share, and serves later reads of the same version from it with no
request. Nothing is cached without the wrapper; `NewByteCache(n)` bounds the bytes
the cache owns, copies a reader is making included, dropping the least recently used
ranges. Entry bookkeeping has a separate count bound, including pending admissions.

```go
cache, err := objfile.NewByteCache(64 << 20)
...
o := objfile.Pin(objfile.Cache(store, store.Scope(), cache), store.Scope())
```

Bytes are keyed by the scope, the name given to the wrapper, the object's
`Identity` and the range, and a read uses the cache only when its Object already had
an Identity as the read began. So a cached byte never proves anything: the first read
of an `OpenSized` Object, and every read of one `Pin` has not checked yet, goes to
the store, and a reopened replaced object has another identity. Only complete
successful reads are kept. A batch serves the ranges the cache holds and sends the
rest as one batch. Cache eligibility is frozen for the entire original batch,
through every order of `Cache`, `Batch`, `Limit` and `Pin`. Cached inputs are
excluded from the reported plan; each miss request still takes its own `Limit`
slot. `ReadObject` is never cached. Concurrent reads of the same
uncached range each make their request.

`WithCacheMode(ctx, CacheBypass)` makes an operation neither use nor fill the cache,
for a scan that would only evict what interactive reads need; pin checks still run.
`ReadAt` without a context uses the cache.
An already cancelled context fails scalar and batch reads before cache lookup.

## Upload an object

`remote.OpenDestination` builds an independently authenticated destination with a
`Writer` and an `Opener`. Opening it creates no object. `NewWriter` starts an
upload; `Close` completes it under the `CreateOnly` condition.

```go
import (
    "context"
    "errors"
    "io"

    "github.com/varveio/swath/go/objfile"
    "github.com/varveio/swath/go/objfile/remote"
)

func upload(ctx context.Context, body io.Reader) (objfile.Receipt, error) {
    dst, err := remote.OpenDestination(ctx, "s3://bucket/dataset", remote.DestinationOptions{})
    if err != nil {
        return objfile.Receipt{}, err
    }
    defer dst.Close()

    u, err := dst.Writer().NewWriter(ctx, "part-00000.parquet", objfile.CreateOnly)
    if err != nil {
        return objfile.Receipt{}, err
    }
    if _, err := io.Copy(u, body); err != nil {
        return objfile.Receipt{}, errors.Join(err, u.Abort())
    }
    if err := u.Close(); err != nil {
        return objfile.Receipt{}, err
    }
    receipt, _ := u.Receipt() // valid after a successful Close
    return receipt, nil
}
```

For an in-memory value, `PutIfAbsent` performs the same conditional creation.
For an existing SDK client, use the provider's `NewWriter` and `RootWriter`.
Uploads send one chunk, part, or block at a time, blocking on the store before
accepting more bytes. Buffer sizes, object-size limits, retries, and cleanup
settings belong to each provider's `WriteOptions`.

Handle completion and cleanup explicitly. When classifying errors with
`errors.Is`, check `ErrUnresolved` before `ErrConflict` if both match:

- A nil `Close` confirms creation and makes `Receipt` valid. `ErrExists` means the
  name was occupied; after a lost response and retry, that may be this upload's
  own object. Read back and compare when the distinction matters.
- `ErrUnresolved` means creation may have happened or may still happen. A read
  finding nothing does not prove failure while a request could remain in flight.
  `Abort` cannot settle or undo an uncertain completion. `ErrConflict` means a
  conditional write conflicted; retry the upload from its first byte.
- `Abort` cancels unfinished work and may run concurrently with `Write`. It does
  not delete a completed object. S3 cleanup can return `s3.ErrAbortUnconfirmed`;
  a later `Abort` may retry. **Configure an S3 lifecycle rule to abort incomplete
  multipart uploads:** crashes or failed cleanup otherwise leave billed parts
  indefinitely. GCS sessions and Azure uncommitted blocks expire automatically.
- `DeleteIf(ctx, receipt.Name, receipt.Identity)` uses the receipt's identity.
  S3 version ids and GCS generations select that exact stored version, even after
  replacement; S3 ETags and Azure ETags condition deletion on the current object.
  An S3 ETag cannot distinguish a replacement with the same ETag. Azure soft
  delete or versioning can retain deleted data and charges.

Every error a `Writer` or an `Upload` returns is redacted as `RedactError` redacts:
no request URL's query or user information is in its text, and upload session
handles are removed, while `errors.Is` and `errors.As` still see the chain.

## API and provider details

The source comments own the API contract and provider settings:

- [Objects, openers, roots, limits](objfile.go), [batched reads](batch.go),
  [pins and snapshots](pin.go),
  [uploads, receipts, errors, and conditional deletion](write.go).
- [Remote stores](remote/remote.go) and [destinations](remote/create.go): credentials,
  endpoints, client ownership, timeouts, and `DestinationOptions` (including
  `GCSWrite`, `S3Write`, and `AzureWrite`). Read and write clients have distinct
  credential and endpoint policies.
- [S3 reads](s3/s3.go) / [writes](s3/write.go),
  [GCS reads](gcs/gcs.go) / [writes](gcs/write.go),
  [Azure reads](azblob/azblob.go) / [writes](azblob/write.go).

For dataset reading, see the [swath reader](../reader/README.md). For swath's
publication and recovery protocol, see [durability](../docs/internals/durability.md)
and [Parquet output](../docs/internals/parquet-output.md). Run this module's tests
with `go test -race ./...` from `go/objfile`.
