# objfile

`github.com/varveio/swath/go/objfile` opens an object in an object store as a file read by byte range, with
one stored version of the object pinned for the file's lifetime. A reader that takes an `io.ReaderAt` and a
size (the [SeekParquet](https://github.com/varveio/seekparquet) module's `OpenDataset`, parquet-go's
`OpenFile`) can then read a remote dataset without seeing two versions of one file.

It is a nested Go module. It does not import the swath lister (`github.com/varveio/swath/go`), so depending
on it brings in only the SDK of the store you use.

## API

```go
type Object interface {
	io.ReaderAt                                 // io.EOF at the end, as io.ReaderAt says
	Size() int64                                // the size when opened
	Identity() string                           // the pinned version
	Close() error                               // later reads fail with fs.ErrClosed
	WithContext(ctx context.Context) io.ReaderAt // reads bounded by ctx
}

type Opener interface {
	Open(ctx context.Context, name string) (Object, error)                  // one metadata request, then pinned
	OpenSized(ctx context.Context, name string, size int64) (Object, error) // no request; the first read pins
}

var ErrObjectChanged error                     // the pinned version is gone
func CheckPath(name string) error              // SeekParquet's PART-1 path rule
func Root(o Opener, prefix string) Opener      // a dataset root: CheckPath, then prefix/name
func Limit(o Opener, n int) Opener             // at most n requests in flight

func s3.New(client *s3.Client, bucket string) objfile.Opener           // AWS SDK for Go v2
func gcs.New(client *storage.Client, bucket string) objfile.Opener     // cloud.google.com/go/storage
func azure.New(client *azblob.Client, container string) objfile.Opener // azblob
```

The caller passes its own SDK client, with its credentials, endpoint, transport and retry settings; objfile
has no credential handling. A missing object fails `Open`, or the first read of an `OpenSized` file, with an
error wrapping `fs.ErrNotExist`. Requests are retried by the SDK; objfile adds only this: a response body cut
short is requested again from where it stopped, at most twice, which is safe because the request is pinned.

An `Object` satisfies SeekParquet's `SourceFile` and `ContextBinder`. Its `Source.Open` returns SeekParquet's own
interface type, which Go does not match against `Object`, so a reader adapts an `Opener` in four lines:

```go
type source struct{ objfile.Opener }

func (s source) Open(ctx context.Context, name string) (seekparquet.SourceFile, error) {
	f, err := s.Opener.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

d, err := seekparquet.OpenDataset(ctx, source{objfile.Root(s3.New(client, "bucket"), "datasets/ds")}, seekparquet.Options{})
```

## The pinning guarantee

Every read of a `Object` returns bytes of the version it pinned, or fails with `ErrObjectChanged` and returns
none of another version's bytes. A read also fails with `ErrObjectChanged` when the response states a total
size other than the file's.

What each store pins with, taken from the metadata request of `Open` or the first read of an `OpenSized` file:

- **S3:** the ETag, sent as `If-Match` on every ranged GetObject, and in a versioned bucket also the version id
  (`VersionId`), so a replaced key still reads the pinned version.
- **GCS:** the generation, set on the object handle (`Generation`) of every range reader, which reads the stored
  bytes (`ReadCompressed`, no decompressive transcoding).
- **Azure:** the ETag, sent as `If-Match` on every ranged download.

A 412, a 404 after open, or a response naming another ETag or version is `ErrObjectChanged`. Pinning is a
cheap guard over the reads, not a versions API: there are no options to choose a pinning mode, and no Azure
snapshot or version-id reads.

`Identity` is the S3 version id (else the ETag), the GCS generation, or the Azure ETag; an `OpenSized` file
returns `""` until its first read.

## What it is not

There is no listing, no caching, no read planning and no writing. Reading a dataset from a store has three
layers, and objfile is the first:

1. **Storage source** (objfile): ranged reads of one pinned version, cancellation, bounded concurrency.
2. **Reader-side planning** (the SeekParquet module, storage-agnostic): which ranges to read, read-ahead for
   index loads and page walks, coalesced range reads.
3. **Caching** (later, an optional wrapper around an `Opener`): bytes keyed by name and `Identity`, so a
   cached range can never be served for another version.

## Tests

`go test -race ./...` in this directory. Each store runs one behaviour suite (`internal/objfiletest`) through
both `Open` and `OpenSized` against an in-process fake: gofakes3 for S3 (wrapped to answer a mismatched
`If-Match` on GET with 412, which gofakes3 checks only on writes) and small `httptest` servers for the GCS
JSON and XML APIs and Azure Blob. `s3/dataset_test.go` writes a dataset with SeekParquet's `local.WriteDir`,
uploads it to gofakes3 and compares a Range and a List read through objfile with the local read.
