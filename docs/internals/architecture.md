# Architecture

swath lists a source (a local directory tree, an S3 or GCS bucket, or an Azure Blob container) and writes a
globally key-sorted Parquet dataset ([../dataset.md](../dataset.md)). It is one pure-Go (`CGO_ENABLED=0`) static binary.
What it promises, and the invariants I1–I12 that keep it true, are in [contracts.md](contracts.md).

## Pipeline

```text
provider ──ranges──▶ order ──ascending row segments──▶ parquet router ──parts──▶ encoders ──▶ dataset publish
(source providers)     (bounded reorder)                 (boundaries, ordinals)   (one goroutine per part)
                                                    └───▶ text sink (formatters ──▶ one ordered stream, one file or stdout)
```

Order comes from the ranges, not from a sort afterwards. A provider expands a *range* into rows that are
already ascending and into child ranges that sort between them. The order stage walks that tree depth-first,
which is global key order, and streams rows at the frontier with bounded memory. The router cuts part
boundaries by size (deterministic for a given listing) and, with `--part-interval` (30 s by default), by age (timing-dependent);
per-part encoders write them in parallel; the dataset package publishes them in order. There is no external
sort: no sorted runs staged on disk and no merge pass. What does sit on disk before publication is ordinary
pipeline state: closed Parquet parts in `.swath/parts/` (claimed while their journal record is written,
then renamed into `data/`) and, with spill, rows ahead of the frontier, read back in key order.

A count (`swath du`, `swath diag count`) has no sink that needs order, so `order.Count` takes the place of the order stage
and everything after it: workers expand the same ranges and the rows are counted and dropped as they arrive,
with no reorder buffer, no spill and no file ([ordering.md](ordering.md#counting-without-order)).

**The split decision** (range engine, after each full page of a range): the owner places a pivot outside the
engine's mutex (the further of the anchor reflection and the galloped density step, never a sliver), then,
under the mutex, splits only if workers are idle (demand), the range ranks among the largest remainders, a
structure boundary or the pivot passes the guards and, for a bounded range, the admission gates estimate
that the remainder and both children are worth their requests. It then emits `(cursor, pivot]` and
`(pivot, hi]` and returns, and the children become claimable
([range-engine.md](range-engine.md)).

## Packages

| Package | Role | Imports |
|---|---|---|
| `cmd/swath` | the commands (`list`, `du`, `resume`, `show`, `diag list`, `diag count`); flags into `listing.Config`'s three groups (`Source`, `Output`, `Limits`, validated once by `Run`: a `*listing.ConfigError` for a bad value or combination), with each provider's own flag group; `signal.Notify` on a channel, the run canceled through `context.WithCancelCause` with the signal as cause (so SIGINT exits 130 and SIGTERM 143), and a forced exit after the shutdown grace; usage generated from the flag set | listing, registry, journal, dataset; providers only to register them (`_` imports) |
| `internal/listing` | `Run(ctx, Config) (Report, error)`: opens the source by its root's scheme, wires it to order, router, dataset; the run report | source and all below, no provider |
| `internal/source` | the provider contract: `Provider[R, P]`, `Emitter[R, P]`, `Disposer[R]`, `Erase`; the optional interfaces (`Identified`, `Resumable`, `Capable`, `Reporter` among them: see [providers.md](providers.md#registry-and-optional-interfaces)) | schema |
| `internal/registry` | `Factory` by URL scheme, `Register`, `Lookup`; `Source`, an opened provider bound to the typed stages (`Bind[R, P]`, `Run`); the column bindings of the filters' sizes, the object bytes, the versions counts and the per-prefix totals | source, schema, order, parquetout, dataset, totals |
| `internal/source/localfs` | filesystem provider: Linux `getdents64` engine, portable engine; its row payload `Entry` and schema `swath/local-entries/v1`; registers the scheme-less roots | source, schema, registry, dataset |
| `internal/source/keyrange` | the range engine: `(lo, hi]` ranges, seed, owner-side splits (demand, rank, admission gates), safe pivots, guards, structure probes, requeue, counters, group mode (a versions listing's repeated keys: groups.go); behind a page fetch and a provider description; `Registered`, the optional interfaces an engine provider gives its registered source | source, schema, counters, faults |
| `internal/source/keyrange/keyrangetest` | test fixtures: a fake listing API over sorted keys, the fixture and hostile keyspaces; imported by tests only | – |
| `internal/source/s3` | S3 provider on the range engine: the ListObjectsV2 page fetch, its row payload `Object` and schema `swath/s3-objects/v1`; registers `s3://`. With `--all-versions` (`NewVersions`): the ListObjectVersions page fetch, its payload `Version` and schema `swath/s3-versions/v1` (grouped), its native page and continuation token | source, schema, registry, keyrange, s3/client |
| `internal/source/azblob` | Azure Blob provider on the range engine: flat-namespace admission (Get Account Information), the List Blobs page fetch through the byte-order adapter (one per `Expand`), its row payloads `Object` and `Version` and schemas `swath/azblob-objects/v1` and `swath/azblob-versions/v1` (grouped: `--all-versions`, `NewVersions`); registers `az://` | source, schema, registry, keyrange, azblob/client, azblob/order |
| `internal/source/azblob/order` | the byte-order adapter: serves the range engine's queries in byte order over List Blobs' UTF-16 order (spans cut at names at U+E000 and above, lazy splits, page-atomic chains with their own tokens, its `order_*` counters) | source, keyrange, counters, azblob/client, utf16order |
| `internal/utf16order` | Azure's listing order (UTF-16 code units) on UTF-8 bytes: the comparator the Azure client, the byte-order adapter and the replay's Azure route share | – |
| `internal/source/gcs` | GCS provider on the range engine: the objects.list page fetch, its row payloads `Object` and `Generation` and schemas `swath/gcs-objects/v1` and `swath/gcs-generations/v1` (grouped: `--all-versions`, `NewGenerations`); registers `gs://` | source, schema, registry, keyrange, gcs/client |
| `internal/source/objhttp` | what all object-store clients share: the retry loop (backoff, exhaustion), failure classes and transport classification, credential errors, client `Meter`, token waits, Guard and capture, transport pool, `Activity` (a client's request activity, each part zero where a supplied client does not count it), route canonicalization; `objhttptest` holds the retry contract all three clients pass | source, counters, tokensource |
| `internal/source/s3/client` | ListObjectsV2 and ListObjectVersions: SigV4, credentials, S3's answers and their classes, the page parser | objhttp |
| `internal/source/gcs/client` | objects.list: OAuth2 credentials, a transport pool of one HTTP/2 transport per 16 workers, the JSON API's answers and their classes, SDK decode checks | objhttp |
| `internal/source/azblob/client` | List Blobs and account/namespace checks: credential and SAS handling, API-version and start bound policy, Azure status/code precedence, SDK decode checks | objhttp, utf16order |
| `internal/source/tokensource` | shared OAuth token cache and one refresh in flight, cancellable waits, the shell-command bearer token source | oauth2, singleflight |
| `internal/order` | bounded reorder of a range tree into one ascending stream, with the stream-wide order guard (`Run`); the unordered count of a dry run (`Count`) | source, schema, dataset |
| `internal/schema` | provider-neutral: `Row[P]` (a key and a typed payload), `Schema[P]` (columns as accessors of P, logical and heap size, spill `Codec[P]` and the optional contextual `DeltaCodec[P]`, `Container`: a row that is no object, a local directory), `Info` (with the [version roles](parquet-output.md#version-roles)); `Bind` takes a schema id's key settings, version roles and its columns' names, types and nullability from the reader module's spec; `BindVersions` derives the version roles' accessors | reader (the nested module) |
| `internal/textout` | the text sink: the schema's columns as TSV, JSONL or the table, to a file (gzip, zstd) or stdout, rows formatted by parallel formatters into one ordered stream ([ordering.md](ordering.md#the-waits)); `swath du`'s prefix rows (TSV, JSONL with `prefix_base64` for a name that is not UTF-8, or the readable table) | schema, dataset, totals |
| `internal/filter` | the row filters (globs, regular expressions, sizes, times, storage classes) and the includes' prefix narrowing | schema, source |
| `internal/totals` | `swath du`: per-prefix sums (objects, directories, bytes, unsized objects, newest time) below the source's prefix, from concurrent batches, capped at `--max-prefixes` (and `--max-inodes` for `--count-links-once`) | – |
| `internal/parquetout` | schema binding, codec, part router (`Router[P]`), encoders over the seekparquet module's part writer | schema, dataset, buildinfo, seekparquet |
| `internal/dataset` | layout, overwrite safety, manifest, footer stamp names, publication gate and order, the part cut rule a sink states for the segment tail (`SegmentPlan`) | faults |
| `internal/journal` | the journal (CRC-framed records of committed parts, synchronous appends by the ordered committer), resume identity | dataset |
| `internal/counters` | the counter table (definitions + one atomic each); summaries and docs are generated from it | – |
| `internal/faults` | named crash points for the crash-matrix tests (one atomic load in production) | – |
| `internal/buildinfo` | the binary's identity (tool, version, VCS revision; the linker's `Version` and `Revision` first): the report's `build`, `--version`, the resume identity's tool, the footer's `created_by`, the clients' User-Agent | – |

The list clients are built on the vendors' SDK service clients; what is the SDK's, what swath keeps and why:
[clients.md](clients.md).

`order` and `parquetout` know nothing about sources; providers know nothing about Parquet. The pipeline packages
(listing, order, parquetout, dataset, schema, journal, registry) import nothing under `internal/source/`, and
`internal/source` and `internal/registry` import no provider: `internal/source` `TestImportBoundary` checks it,
transitively.

**Nested modules.** Two packages are modules of their own, so a program that reads swath datasets does not
inherit the lister's dependencies: `reader` (`github.com/varveio/swath/reader`: the schema ids and their
columns, the manifest's `extensions.swath`, typed rows decoded from SeekParquet rows, and a `Dataset` that opens a
dataset, hands on its metadata, checks its schema, and exposes the underlying SeekParquet dataset for reads; the SeekParquet module
and the standard library only; [../../reader/README.md](../../reader/README.md)) and `objfile` (a dataset in an object
store opened as files read by byte range, pinned across opens and process restarts, by URI with default
credentials through `objfile/remote`, and its create-only write side; [../../objfile/README.md](../../objfile/README.md)). The lister imports
`reader` through a `replace` to `./reader` in `go.mod`: its producers' schemas are bound to the reader's
specs (`schema.Bind`), so what swath writes and what a reader decodes are described once. It imports
`objfile` the same way (`./objfile`) for a dataset written to a bucket: the uploads, the manifest's
conditional creation and the read-back of retained parts go through `objfile/gcs`, `objfile/s3` or `objfile/azblob` and `objfile/remote.OpenDestination`,
which links `cloud.google.com/go/storage` into the swath binary
([durability.md](durability.md#a-dataset-in-a-bucket)). Neither nested module imports the lister or the other nested module.
The optional objfile byte cache rejects an already cancelled read context before
lookup, for both scalar and batched reads ([cache contract](../../objfile/cache.go)).
The reader and SeekParquet adapter preserve the caller's cache policy in the
context. Callers of SeekParquet bulk scans and aggregations choose whether their
Source should cache those reads;
with objfile they can mark `objfile.CacheBypass` to avoid byte-cache lookup and
admission. The policy also covers planning, splitting and exact-key projections. Point lookups, browse pages, counts and
aggregate sums use the policy their caller supplies. SeekParquet reports logical
and format work; objfile's observer reports physical requests and byte-cache hits,
and `objfile.Limit` bounds request concurrency. See the
[reader API](../../reader/README.md) and [objfile API](../../objfile/README.md).

**Typed rows.** A row is `schema.Row[P]`: its key and the producer's payload `P` (`localfs.Entry`, `s3.Object`), so
an S3 row carries no filesystem slots and the reverse. The producer defines `P` and its `schema.Schema[P]`
beside its code: columns as accessors of `P`, the logical size the router cuts parts by, the heap bytes the
budget counts, and the spill codec ([shared payload delta helper](ordering.md#payload-delta-codec)). The order stage and the router are generic over `P`; the pipeline
(`listing`) never names it. The one place that does is `registry.Bind[R, P]`, which a provider calls when it
opens: it erases the range type and returns a `Source` whose `Run` instantiates the order stage and the
router for `P` (Go has no generic methods, so the binding happens where `P` is known).

## Goroutines and ownership

- **Workers** (`order.Options.Workers`) take ranges from a LIFO stack and call `Provider.Expand`; ranges of
  different nodes expand concurrently. A worker never waits on child work: children only become claimable.
- **One drainer** (the goroutine that called `order.Run`) walks the node tree depth-first and calls the sink
  (the router) with contiguous ascending segments. It expands a queued range itself when it needs it and no
  worker has claimed it, so a full buffer cannot deadlock the walk.
- **Publication**: with a journal (a resumable object store with an output directory), each encoder goroutine publishes its part when it closes,
  after the previous part's publication (a channel per part keeps ordinal order): journal record, then
  rename. That chain is the one ordered committer; the journal's append is a synchronous call. See [durability.md](durability.md).
- **Encoders**: the router starts one goroutine per open part, at most `Encoders` at a time (a semaphore
  channel), each with its own writer and zstd encoder, fed through a bounded channel. When fewer parts than
  encoders are open, a part encodes a page's columns on several goroutines; the bytes are identical.
- **Ownership.** A slice passed to `Emitter.Rows` belongs to the order stage from then on; the provider must not
  touch it. Segments handed to the sink belong to the router until the encoder has written them. parquet-go
  copies byte-array values, so rows may alias provider buffers (keys are zero-copy views into a per-page or
  per-directory arena). Anything that outlives a part (its min and max key) is cloned so it does not pin an
  arena.
- **Cancellation.** Every stage takes the `context.Context`. The first asynchronous failure (an encoder, the
  disk, the journal) cancels the pipeline's context with that error as its cause, so listing stops at once
  and `Run` returns that error. The first error stops the walk (an atomic flag,
  checked without taking `ctx`'s mutex), wakes every waiter, closes the encoders, removes temporary parts and
  is returned wrapped with its path or phase. `manifest.json`, the commit, is installed only after a complete publish.

## Errors

Errors are wrapped Go errors (`errors.Is`/`As`, cancellation causes), with types only where a caller decides
something: `*listing.ConfigError` (an invalid value or combination, before anything is opened),
`*journal.MismatchError` (a resume asked with frozen settings changed), `*journal.VersionError` (a journal of
another build), `*dataset.InvariantError` (the publication gate or the row count found a violated invariant;
nothing is published), `*dataset.ReportError` (the report failed after the commit: the dataset stands),
`listing.ErrComplete`, `ErrNothingToResume`, `ErrInterrupted`, and the S3 client's `*client.Error` (status,
code, request id, the bucket's region). `cmd/swath`'s `explain` adds actionable flag advice. `Run`
returns the run report with every error, and `--report -` prints it on failure too (outcome
`failed` with the error).

## How a provider plugs in

Implement `source.Provider[R, P]` for a range type `R` and a row payload `P`, define `P`'s
`schema.Schema[P]`, and register a `registry.Factory` from `init` whose `Open` returns the opened source: a
struct embedding `registry.Bind(provider, &schema)` and, for a provider on the range engine, `keyrange.Registered`
set to the same provider (its `Listed` and the engine's optional interfaces, promoted), with the provider's own
`Locator`, `Identity`, `Capabilities`, `Report` and, on its own service, `Tariff` (see
[providers.md](providers.md)). The factory also declares the provider's flags (its own, and
the shared groups it names: the store's and range engine's),
which of them a resume may change, its secrets, its help entries and its root form; the command derives its flag
set, help, resume rules and redaction from the registered factories and names no provider. The command imports
the package for that registration only; `listing` does the rest. The [provider integration checklist](providers.md#adding-a-provider)
lists reader, verifier, test and dependency touch points outside the provider directory. Restart belongs to
[the engine](range-engine.md#resume); Azure's per-expansion adapter lifetime belongs to
[its provider](providers/azblob.md#order-the-byte-order-adapter).

## Memory

One `--sort-buffer`: a byte budget for rows in flight, shared by the order stage's buffers and the encoders'
queues. Pages being parsed, the encoders' writers (admitted separately), codecs and the garbage collector are
outside it. A waiting range's bounds are copies, so a queued range never keeps the page it was cut from alive.
A dry run buffers no rows. See [ordering.md](ordering.md) for what may exceed the
budget.

## Further reading

[ordering.md](ordering.md) · [parquet-output.md](parquet-output.md) ·
[durability.md](durability.md) ·
[providers.md](providers.md) · [contracts.md](contracts.md) ·
[dependencies.md](dependencies.md) ·
[testing.md](testing.md) · [../commands/swath.md](../commands/swath.md) · [run-lifecycle.md](run-lifecycle.md) · [measurements.md](measurements.md)
