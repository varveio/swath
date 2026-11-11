# objfile

`github.com/varveio/swath/go/objfile` opens an object in an object store as a file read by byte range, with
one stored version of the object pinned for the file's lifetime. A reader that takes an `io.ReaderAt` and a
size (the [SeekParquet](https://github.com/varveio/seekparquet) module's `OpenDataset`, parquet-go's
`OpenFile`) can then read a remote dataset without seeing two versions of one file.

It is a nested Go module. It does not import the swath lister (`github.com/varveio/swath/go`). Its go.mod
lists all three SDKs, so they are in a consumer's module graph, but a program links only the SDK of the store
package it imports.

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
func azblob.New(client *azsdk.Client, container string) objfile.Opener // azblob SDK

func Pin(o Opener, scope string) *Pinned                                 // pins across opens
func Restore(o Opener, scope string, snapshot []byte) (*Pinned, error)   // pins across restarts
func (p *Pinned) Open(ctx context.Context, name string) (Object, error)
func (p *Pinned) Snapshot() ([]byte, error)
var ErrInvalidSnapshot error

type Location struct{ Scheme, Account, Container, Prefix string }       // String() is the canonical URI
func ParseLocation(raw string) (Location, error)

func remote.Open(ctx context.Context, location string) (*remote.Store, error) // default credentials
type remote.Store struct{ ... }                                                // objfile.Opener and io.Closer
func (s *remote.Store) Scope() string                                          // the scope for Pin and Restore

// The write side (GCS only): create-only uploads, conditional deletes.
type Condition int
const CreateOnly Condition                              // ifGenerationMatch=0 (S3: If-None-Match: *)
var ErrExists error                                    // a CreateOnly write found an object
type Receipt struct{ Name string; Size int64; Identity string }
type Upload interface {
	io.Writer
	Close() error // completes: the object exists only after a nil Close
	Abort() error // no object; nothing after a successful Close
	Receipt() (Receipt, bool)
}
type Writer interface {
	NewWriter(ctx context.Context, name string, c Condition) (Upload, error)
	PutIfAbsent(ctx context.Context, name string, data []byte) (Receipt, error)
	DeleteIf(ctx context.Context, name, identity string) error // only that version
}
func RootWriter(w Writer, prefix string) Writer
func gcs.NewWriter(client *storage.Client, bucket string, o gcs.WriteOptions) objfile.Writer
func remote.Create(ctx context.Context, location string, o remote.CreateOptions) (*remote.Destination, error)
```

The store packages take the caller's own SDK client, with its credentials, endpoint, transport and retry
settings, and have no credential handling; `objfile/remote` builds clients with the default credentials for a
caller that has no settings to give. A missing object fails `Open`, or the first read of an `OpenSized` file, with an
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

`Identity` is the S3 version id, the GCS generation, or the Azure ETag; an `OpenSized` file returns `""` until
its first read. An S3 object without a version id, or with the version id `null` (written before versioning was
enabled, or while it is suspended), is identified by its ETag instead: S3 keeps the id `null` when it overwrites
such an object, so a replacement of the same size would otherwise keep the same `Identity`.

## The range guarantee

A read returns exactly the requested bytes `[off, off+n)` of the pinned object, or fails: with
`ErrObjectChanged` when the object changed, and with a plain error when the store ignored the range or served
another one. Every response is checked for the offset it starts at (its `Content-Range`, or for GCS the
reader's `StartOffset`; a response without one starts at byte 0), the total size and its length.

The GCS SDK's reader also requests the rest of a range by itself, when a body fails part way, checks only that
answer's status, not its range, and for an answer labelled `Content-Encoding: gzip` skips as many of its bytes
as it had already read. The client `remote.Open` builds has a transport that refuses every answer to a range
request unless it is a 206 of exactly that range, of that length, not decompressed in transit, and that ends a
body failing part way where it failed, so the SDK never requests the rest: objfile does, checked as every
request is. **On a GCS client the caller builds, a read whose body fails part way may return wrong bytes without
an error** (another range's, or two parts of the object joined), unless the caller's transport does the same.
The S3 and Azure SDKs retry whole requests only, so the response objfile checks is the one served.

## Pins across opens and restarts

An `Object` pins one version for its lifetime. `Pin` extends that across opens: a `Pinned` records the size and
`Identity` of each name at its first successful `Open`, and every later `Open` of that name must find the same,
or it fails with `ErrObjectChanged` (a deleted object too). A recorded pin is never replaced, and an `Open` that
loses a race to record one is checked against the winner's and closes its object when they differ. This
re-proves that the current object is the one pinned; it never reads an older version by its recorded identity.

`Snapshot` returns the pins as versioned JSON, the same bytes for the same pins:

```json
{"version":1,"scope":"s3://bkt/datasets/ds/","objects":{"manifest.json":{"size":812,"identity":"3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"}}}
```

`Restore` reads one back for a restart. Its `Pinned` is sealed: a name the snapshot lacks fails with an error
wrapping `fs.ErrNotExist` before any request. The decode is strict: exactly these members, each once, version 1,
names `CheckPath` accepts, sizes of at least 0, identities non-empty without control characters, and the scope
the caller passes; anything else is `ErrInvalidSnapshot`. The scope names the storage namespace (provider,
bucket or account and container, a custom endpoint, the root prefix), so a snapshot is never applied to another
store; `remote.Store.Scope` is one, and holds no credentials. Restore reads version 1 only: a change to the
format is a new version number, which an older objfile refuses, with no fallback.

A snapshot proves the files that were opened, nothing more: that it holds every file a dataset needs is the
application's to establish, by opening every one of them (`reader.Dataset.CheckSchema` opens every part), not
objfile's. `Pinned` has no `OpenSized`, whose
Object learns its identity only at its first read. Compose it outermost:
`objfile.Pin(objfile.Root(objfile.Limit(o, n), prefix), scope)`.

## Locations and default credentials

`ParseLocation` reads a dataset root: `s3://BUCKET[/PREFIX]`, `gs://BUCKET[/PREFIX]` or
`az://ACCOUNT/CONTAINER[/PREFIX]`. The prefix is literal (nothing is decoded) and a dataset root: it gains a
trailing `/`, and its path must be one `CheckPath` accepts. A query, fragment, user information, port, empty
or upper-case bucket, a bucket, account or container named `.` or `..`, or a `.` or `..` prefix segment is
refused; `Location.String` is the canonical URI. Refusal errors show clean locations and replace
a location with `[redacted]` when it contains `?` or `#`, when its authority (the bucket, or the
account) contains `@` or a colon, or when it has no `://` and contains `@`. An `@` in the prefix is
literal and is shown.
Ranged-response validation errors keep the request scheme, host and escaped path, excluding
user information, query and fragment.

`remote.Open` opens a location with each provider's default credentials, read-only: the AWS SDK's default
configuration (which also gives the region and an `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL` endpoint), Google
Application Default Credentials with the read-only scope or the `STORAGE_EMULATOR_HOST` emulator, and Azure's
`DefaultAzureCredential` on `ACCOUNT.blob.core.windows.net`. The `Store` is `Root(provider.New(client,
container), prefix)` over clients and a connection pool it owns; `Close` releases them and closes the pool's
idle connections. An endpoint from the environment that carries user information, a query or a fragment is
refused, so `Scope` never holds a credential. It takes no options and does not bound
concurrency: a program that needs a region, endpoint, anonymous access or a test transport builds the SDK client
itself and uses `s3.New`, `gcs.New` or `azblob.New` with `Root`; one that needs a bound wraps the `Store` in
`Limit`. Importing `objfile/remote` links all three providers' SDKs; a program that reads from one provider
builds that SDK's client and uses its `New` with `Root` the same way.

The `Store` outlives the context `Open` is given: every request runs under the context of the `Open` or read
that makes it, so a `Store` opened while serving one request can be kept for later ones. Google's credentials
come from the newer auth library (`cloud.google.com/go/auth`), which fetches a token a request waits for with
that request's context, so cancelling the request cancels that exchange; a refresh the library starts early,
before a token expires, runs in the background with its own context and is not cancelled by any request.

The swath CLI parses the same schemes for a listing root, with other semantics: a listing prefix is a key
prefix, not a directory, and an `az://` root may carry a SAS query. It keeps its own parser.

## Writing

The write side creates whole objects under a precondition and nothing else: no rename, no listing, no
overwrite, and a delete only of the version named by its identity. `NewWriter` starts an upload whose bytes are
sent as they are written; the object exists only once `Close` returns nil, and its `Receipt` then names the
created version (the GCS generation). `Abort` ends an upload without an object. `CreateOnly` is GCS
`ifGenerationMatch=0`, checked by the service when the object is created, so of two writers of one name exactly
one succeeds and the other gets `ErrExists`. After a lost response the SDK sends the request again, and a
`PutIfAbsent` retry then finds the write's own object and returns `ErrExists`: a caller that must tell the two
apart reads the object back and compares it with what it wrote.

`gcs.NewWriter` uploads in resumable sessions of `WriteOptions.ChunkSize` (8 MiB by default; the SDK rounds to
256 KiB): the SDK buffers one chunk an upload and blocks `Write` while it is in flight, so memory is a chunk per
open upload and a slow store holds the writer back. An object smaller than a chunk goes in one request. Retries
are the SDK's, bounded: `MaxAttempts` a request (5 by default), and `ChunkRetryDeadline` for a session's chunk;
`Retried` is told each attempt its policy retries. `Abort` cancels the session's request; a session the service
holds expires on its own (GCS keeps one for a week), and its URI, a bearer credential for the upload, never
leaves the process.

`remote.Create` builds a destination: a `Writer` and an `Opener` under the location's prefix, through a
read-write client it owns, with its own credentials (`CreateOptions.CredentialsFile`, or Application Default
Credentials) and endpoint, shared with no read client. Only `gs://` is written; `s3://` and `az://` are
refused. The SDK's logger is silenced, since its debug output writes request headers unredacted.

`gcs/gcsfake` is the in-process fake the GCS tests use: JSON metadata and deletes, XML ranged reads, and
uploads both single-request and resumable, with `ifGenerationMatch` evaluated atomically at creation and a
fault hook that fails a request before it acts, or loses its response after.

## What it is not

There is no listing, no caching and no read planning. Reading a dataset from a store has three
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
JSON and XML APIs (`gcs/gcsfake`, which also takes uploads) and Azure Blob. `gcs/write_test.go` uploads
through a single request and resumable sessions, races a creation against an upload, aborts and cancels, fails
and loses responses at each upload step, and deletes by generation. `s3/dataset_test.go` writes a dataset with SeekParquet's `local.WriteDir`,
uploads it to gofakes3 and compares a Range and a List read through objfile with the local read. `gcs/range_test.go`
serves ignored, shifted and cut-then-shifted ranges, with and without the transport `remote.Open` installs.
`remote` opens each provider's location against an in-process server through the environment (the S3 endpoint
variables, `STORAGE_EMULATOR_HOST`; the Azure client is replaced in the test).
`TestOpenGCSCredentialsOutliveContext` opens a `gs://` Store with user credentials (an application-default
login), their token endpoint dialled to a test server, cancels the opening context, and reads twice across a
token expiry, so the token is refreshed after that context ended (on Google's older auth path the exchange
fails with the opening context's cancellation); it then cancels a read during its token exchange and checks
the exchange was cancelled.
