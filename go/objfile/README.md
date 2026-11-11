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

objfile handles individual objects. It does not list, cache, plan reads, rename,
or overwrite them; dataset publication and recovery belong to the application.

## Read a remote file

`remote.Open` builds a read-only store with default credentials. Locations are
`s3://BUCKET[/PREFIX]`, `gs://BUCKET[/PREFIX]`, or
`az://ACCOUNT/CONTAINER[/PREFIX]`. The prefix is a dataset root; object names below
it are literal relative paths, without empty, `.` or `..` segments. Location URIs
cannot carry credentials, queries, or fragments.

This example reads a Parquet footer without downloading the whole object:

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
generation, or an Azure ETag. Reads check the returned identity, size, offset, and
length. A changed or deleted pinned object yields `ErrObjectChanged`; an invalid
range response yields an error. S3 objects without a unique version id (including
`"null"`) use the ETag as `Identity`: a replacement with the same ETag and size
cannot be distinguished. Pinning does not provide a dataset-wide snapshot.

**Custom GCS clients need care:** after a response body fails partway, the SDK can
retry a range without checking its offset and return wrong bytes without an
error. The clients built by `remote.Open` and `remote.OpenDestination` install a
validating transport. See the [GCS package contract](gcs/gcs.go) before supplying
your own client.

For more control:

- Build an SDK client and pass it to `s3.New`, `gcs.New`, or `azblob.New`; wrap the
  opener with `Root` for path confinement and `Limit` for bounded requests.
- Use `OpenSized` when a manifest already gives the size. It skips the metadata
  request and pins on the first read; `Identity()` is empty until then.
- Use `Pin` across opens and `Snapshot`/`Restore` across restarts. Compose it
  outermost: `objfile.Pin(objfile.Root(objfile.Limit(o, n), prefix), scope)`.
  `Pinned` has no `OpenSized`: it must check identity at open. A restored snapshot
  permits only recorded names and rechecks the current objects, rather than
  reopening historical versions. It records only objects actually opened; the
  application must establish that every required object is covered. Use the
  store's `Scope()` to bind snapshots to its namespace.

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

Every error a `Writer` or an `Upload` returns is redacted as `RedactError` redacts: no request URL's query or
user information is in its text, and upload session handles are removed, while `errors.Is` and `errors.As`
still see the chain.

## API and provider details

The source comments own the API contract and provider settings:

- [Objects, openers, roots, limits](objfile.go), [pins and snapshots](pin.go),
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
