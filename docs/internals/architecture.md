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
| `cmd/swath` | the swath binary: `command.Exit(command.Main(...))` with the process's arguments and standard streams | command |
| `internal/command` | the command line as a function, internal to swath: `Main` runs one command line and returns its exit code, `Exit` exits with it as the binary does, and `Main`'s host argument names the program a copy is built into, which `--version` then states. The commands (`list`, `du`, `resume`, `show`, `diag list`, `diag count`; `du --from-dataset` reads a committed dataset through `reader` and `registry.DatasetTotals` instead of running a listing); flags into `inventory.Config`'s three groups (`Source`, `Output`, `Limits`, validated once by `Run`: a `*inventory.ConfigError` for a bad value or combination), with each provider's own flag group; usage generated from the flag set. Terminal detection (`--format auto`, color, the live progress line) follows the stdout and stderr given to `Main`: a writer that is not an `*os.File` is not a terminal. Process-wide policy is set when `Main` is called, never when the package is imported, for a process dedicated to the call (the binary, or a host's child): the Azure SDK's own log turned off, SIGPIPE ignored, `signal.Notify` on a channel, the run canceled through `context.WithCancelCause` with the signal as cause (so SIGINT is class `interrupted`, 130, SIGTERM `terminated`, 143, and SIGHUP `terminated`, 129, unless ignored at start (`nohup`); on Unix `Exit` then kills the process by the signal it got), and a forced exit after the shutdown grace, the one path on which `Main` ends the process instead of returning | listing, registry, cli, journal, dataset; no provider (`cli` registers them) |
| `internal/inventory` | `Run(ctx, Config) (Report, error)`: opens the source by its root's scheme, wires it to order, router, dataset; the run report | source and all below, no provider |
| `internal/cli` | the command line's flag table (`Flag`, `Define`, `Described`, the flag values) and help groups; the flag groups the providers share, `StoreGroup` (filling `source.Store`) and `EngineGroup` (filling `keyrange.Common`); each provider's registration, one file each (`s3.go`, `gcs.go`, `azblob.go`, `localfs.go`): its factory, registered from `init`, its flags and their environment variables and conflicts turned into the provider's options, and the source a run takes (the provider's opened view joined with `registry.From`) | registry, source, keyrange, objhttp, parquetout, pflag, every provider |
| `internal/source` | the provider contract: `Provider[R, P]`, `Emitter[R, P]`, `Disposer[R]`, `Erase`; what was not read (`Unreader`); the optional interfaces (`Identified`, `Resumable`, `Capable`, `Reporter`, `Traced` among them: see [providers.md](providers.md#registry-and-optional-interfaces)) | schema, runtrace |
| `internal/registry` | `Factory` by URL scheme, `Register`, `Lookup`; `Source`, an opened provider bound to the typed stages (`From`, `Bind[R, P]`, `Run`: a `bind.Bound` whose rows go where the run's `Env` sends them, the part router, a text stream or nowhere); `SchemaInfo`, the producers' schema declarations by id (`Factory.Schemas`), and `DatasetTotals`, the same totals and filters over a committed dataset's rows (`swath du --from-dataset`) | bind, source, schema, order, parquetout, textout, dataset, totals, reader, seekparquet |
| `internal/bind` | a provider bound to its row schema (`Provider[R, P]`) and listed into a sink its caller supplies: unordered (`Count`, `order.CountWithSink`) or through the order stage (`Run`); the column bindings of the filters, the object bytes, the versions counts and the per-prefix totals between provider and sink. It chooses no sink | source, schema, order, filter, totals |
| `internal/source/localfs` | filesystem provider: Linux `getdents64` engine, portable engine; its row payload `Entry` and schema `swath/local-entries/v1`; `Open`, its opened view, whose `Begin` readies a listing (pruning, a count's processors) | source, schema, bind, dataset, longpath |
| `internal/source/keyrange` | the range engine: `(lo, hi]` ranges, seed, owner-side splits (demand, rank, admission gates), safe pivots, guards, structure probes, requeue, counters, group mode (a versions listing's repeated keys: groups.go), warm start and a counted prior's plan of tiles (prior.go, plan.go); behind a page fetch and a provider description; `Registered`, the optional interfaces an engine provider gives its registered source; the run trace's events ([trace.md](trace.md)) | source, schema, counters, faults, runtrace, buildinfo |
| `internal/source/keyrange/keyrangetest` | test fixtures: a fake listing API over sorted keys, the fixture and hostile keyspaces; imported by tests only | – |
| `internal/source/s3` | S3 provider on the range engine: the ListObjectsV2 page fetch, its row payload `Object` and schema `swath/s3-objects/v1`, for `s3://` roots. With `--all-versions` (`NewVersions`): the ListObjectVersions page fetch, its payload `Version` and schema `swath/s3-versions/v1` (grouped), its native page and continuation token; `Open` and `OpenVersions`, its opened view; `OpenLive`, a bucket's client for the library's live source, with no range engine | source, schema, bind, keyrange, s3/client, live |
| `internal/source/azblob` | Azure Blob provider on the range engine: flat-namespace admission (Get Account Information), the List Blobs page fetch through the byte-order adapter (one per `Expand`), its row payloads `Object` and `Version` and schemas `swath/azure-objects/v1` and `swath/azure-versions/v1` (grouped: `--all-versions`, `NewVersions`); `Open` and `OpenVersions`, its opened view; `OpenLive`, a container's client for the library's live source, with no admission and no range engine | source, schema, bind, keyrange, azblob/client, azblob/order, live |
| `internal/source/azblob/order` | the byte-order adapter: serves the range engine's queries in byte order over List Blobs' UTF-16 order (spans cut at names at U+E000 and above, lazy splits, page-atomic chains with their own tokens, its `order_*` counters) | source, keyrange, counters, azblob/client, utf16order |
| `internal/utf16order` | Azure's listing order (UTF-16 code units) on UTF-8 bytes: the comparator the Azure client, the byte-order adapter and the replay's Azure route share | – |
| `internal/source/gcs` | GCS provider on the range engine: the objects.list page fetch, its row payloads `Object` and `Generation` and schemas `swath/gcs-objects/v1` and `swath/gcs-generations/v1` (grouped: `--all-versions`, `NewGenerations`); `Open` and `OpenGenerations`, its opened view | source, schema, bind, keyrange, gcs/client |
| `internal/source/objhttp` | what all object-store clients share: the retry loop (backoff, exhaustion), failure classes and transport classification, credential errors, client `Meter`, token waits, Guard and capture, transport pool, `Activity` (a client's request activity, each part zero where a supplied client does not count it), route canonicalization; a failure's class in the listing contract (`ListingClass`); `objhttptest` holds the retry contract all three clients pass | source, counters, tokensource, reader/listing |
| `internal/source/s3/client` | ListObjectsV2 and ListObjectVersions, and HeadObject and HeadBucket for the live source's lookups: SigV4, credentials, S3's answers and their classes, the page parser | objhttp |
| `internal/source/gcs/client` | objects.list: OAuth2 credentials, a transport pool of one HTTP/2 transport per 16 workers, the JSON API's answers and their classes, SDK decode checks | objhttp |
| `internal/source/azblob/client` | List Blobs and account/namespace checks, and Get Blob Properties for the live source's lookups: credential and SAS handling, API-version and start bound policy, Azure status/code precedence, SDK decode checks | objhttp, utf16order |
| `internal/source/tokensource` | shared OAuth token cache and one refresh in flight, cancellable waits, the shell-command bearer token source | oauth2, singleflight |
| `internal/order` | bounded reorder of a range tree into one ascending stream, with the stream-wide order guard (`Run`): spill and durable spill, the frontier window that admits a planned run's tiles, the segment tail after listing; the unordered count of a dry run (`Count`) | source, schema, dataset |
| `internal/schema` | provider-neutral: `Row[P]` (a key and a typed payload), `Schema[P]` (columns as accessors of P, logical and heap size, spill `Codec[P]` and the optional contextual `DeltaCodec[P]`, `Container`: a row that is no object, a local directory, derived from `Info.Containers`), `Info` (with the [version roles](parquet-output.md#version-roles)); `Bind` takes a schema id's key settings, version roles and its columns' names, types and nullability from the reader module's spec; `BindVersions` derives the version roles' accessors | reader (the nested module) |
| `internal/textout` | the text sink: the schema's columns as TSV, JSONL or the table, to a file (gzip, zstd) or stdout, rows formatted by parallel formatters into one ordered stream ([ordering.md](ordering.md#the-waits)); `swath du`'s prefix rows (TSV, JSONL with `prefix_base64` for a name that is not UTF-8, or the readable table), and its view's headings over `reader/du/tree`, which draws it | schema, dataset, totals, reader/du/tree |
| `internal/filter` | the row filters (globs, regular expressions, sizes, times, storage classes) and the includes' prefix narrowing | schema, source |
| `internal/totals` | `swath du`: per-prefix sums (objects, directories, the size and the allocated bytes each with the objects missing it, newest time) below the source's prefix, from concurrent batches, capped at `--max-prefixes` (and `--max-inodes` for the hard-linked files a local tree counts once); the adaptive view (`ViewRun`, `View`: the 100 largest nodes of each depth, exact, from runs of rows, [ordering](ordering.md)), handed to `reader/du/tree` as a `Tree` (`View.Tree`) | reader/du/tree |
| `internal/parquetout` | schema binding, codec, part router (`Router[P]`), encoders over the seekparquet module's part writer | schema, dataset, buildinfo, seekparquet |
| `internal/dataset` | layout, overwrite safety, manifest, footer stamp names, publication gate and order, the part cut rule a sink states for the segment tail (`SegmentPlan`) | faults, longpath |
| `internal/journal` | the journal (CRC-framed records of committed parts, synchronous appends by the ordered committer), resume identity | dataset |
| `internal/counters` | the counter table (definitions + one atomic each); summaries and docs are generated from it | – |
| `internal/runtrace` | the run trace's writer (`swath diag list --trace`): events encoded by their caller, written by one goroutine through a bounded buffer, dropped and counted rather than waited for ([trace.md](trace.md)) | – |
| `internal/faults` | named crash points for the crash-matrix tests (one atomic load in production) | – |
| `internal/longpath` | `Stat` and `Open` of a path past PATH_MAX, resolved one name at a time when the kernel refuses it by its length (Linux, macOS): a local root that deep, and the output's checks against it | – |
| `internal/buildinfo` | the binary's identity (tool, version, VCS revision; the linker's `Version` and `Revision` first; in a host that embeds `command`, swath's dependency version and no revision, since the toolchain's stamps are the host's): the report's `build`, `--version`, the resume identity's tool, the footer's `created_by`, the clients' User-Agent | – |

The list clients are built on the vendors' SDK service clients; what is the SDK's, what swath keeps and why:
[clients.md](clients.md).

**The library** (experimental, [library.md](../library.md)). Package `github.com/varveio/swath/source` holds
`Source`, an opened live location that answers the listing contract's Page, Lookup and Walk (`reader/listing`) with the
checks and page tokens every object store's pages share; it links no provider and no cloud SDK. A provider's
public package, under it, opens one with typed options and no flags: `github.com/varveio/swath/source/s3` (`s3.Open`) over
`internal/source/s3.OpenLive`, whose one-level call is one delimiter ListObjectsV2 and whose lookup is HeadObject,
then HeadBucket after a 404; `github.com/varveio/swath/source/azblob` (`azblob.Open`) over
`internal/source/azblob.OpenLive`, whose one-level call is one delimiter List Blobs served in byte order
([its live source](providers/azblob.md#the-live-source)) and whose lookup is Get Blob Properties, then a one-entry
List Blobs after a 404; `github.com/varveio/swath/source/gcs` (`gcs.Open`) over `internal/source/gcs.OpenLive`, whose
one-level call is one delimiter objects.list and whose lookup is objects.get, then buckets.get after a 404 with reason
`notFound`; and `github.com/varveio/swath/source/local` (`local.Open`) over `internal/source/localfs.OpenLive`, a directory
the caller holds, whose one-level call reads one directory relative to it and whose lookup resolves a key one name
at a time. `internal/live` joins the two: the `Store` a provider implements, and the `Source` over one
(`live.NewSource`), which package `source` defines `source.Source` on, with methods that call it. A provider's failures take the
contract's classes through `objhttp.ListingClass`, which reads them as the command's `objhttp.Label` does. Neither
package links the command, pflag, the registry, the run pipeline or a writer (`TestLibraryImports`).

`Source.Walk` lists a prefix flat. A store (`live.Store`) builds a provider of its own for each call,
over its shared client (S3's, GCS's and Azure's `Live.Walk`: the range engine with no seed and no probes, Azure's
after the namespace admission of `New`), binds it to its schema
and runs the order stage's unordered count into the walk's sink (`live.Count`: `bind`,
`order.CountWithSink`, each range's keys checked ascending, and each run of a range's keys checked apart from every
other run's before the sink sees it, so no key reaches it twice). The count's workers hand that sink batches of
entries, which it checks and hands to the caller's function one at a time from the goroutine that called
`Walk`, each worker waiting until its batch is taken; the extents not read reach it through the count's
`order.UnreadSink`.

`order` and `parquetout` know nothing about sources; providers know nothing about Parquet. The pipeline packages
(listing, order, parquetout, dataset, schema, journal, registry) import nothing under `internal/source/`, and
`internal/source` and `internal/registry` import no provider: `internal/source` `TestImportBoundary` checks it,
transitively. The other way round, what providers are built from (`internal/source`, `keyrange`, `objhttp`,
`tokensource`) imports none of the command's composition (pflag, `registry`, `parquetout`, `textout`, `cli`),
transitively, nor does any provider, which `cli` registers; their parquet-go arrives only through
SeekParquet's reader (`schema` → `reader`): `cli` `TestProviderImportBoundary`.
`internal/order` and `internal/dataset` reach no cloud SDK module either (`internal/order` `TestNoCloudSDK`): a store's
failure is classed by its `HTTPStatusCode`, which `objfile`'s GCS and Azure writers attach to the errors they report
(`objfile.WithHTTPStatus`) and the AWS SDK's response errors carry, so `dataset.FailureClass` names no SDK error type.

**Nested modules.** Two packages are modules of their own, so a program that reads swath datasets does not
inherit the lister's dependencies: `reader` (`github.com/varveio/swath/reader`: the schema ids and their
columns, the manifest's `extensions.swath`, typed rows decoded from SeekParquet rows, and a `Dataset` that opens a
dataset, hands on its metadata, checks its schema, and exposes the underlying SeekParquet dataset for reads; the SeekParquet module
and the standard library only; [../../reader/README.md](../../reader/README.md)), with four experimental packages:
`reader/du`, a du-style tally of listing entries per key prefix, `reader/du/tree`, the view `swath du` draws (its rows chosen and written, over the standard library and
`golang.org/x/text`'s width tables only), `reader/listing`, the contract of the questions (Page, Lookup, Walk, and an
inventory's Summarize) as types over the standard library only, and `reader/view`, which answers them from an opened dataset through SeekParquet and `objfile` (a dataset in an object
store opened as files read by byte range, pinned across opens and process restarts, by URI with default
credentials through `objfile/remote`, and its create-only write side; [../../objfile/README.md](../../objfile/README.md)). The root
`go.mod` requires both by released version and carries no `replace`, so the root module is importable as a
library; `go.work` at the repository root joins the three modules, so every build in the repository compiles
the local `./reader` and `./objfile`, and a release first tags them and then points `go.mod` at those tags
([RELEASING.md](../../RELEASING.md#versioning)). The lister imports `reader` because its producers' schemas
are bound to the reader's specs (`schema.Bind`), so what swath writes and what a reader decodes are described
once. It imports `objfile` for a dataset written to a bucket: the uploads, the manifest's
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
(`inventory`) never names it. Two places do, each where `P` is known (Go has no generic methods): a provider's
opener binds the provider to its schema (`bind.Provider[R, P]`, which erases the range type), and the
command's composition joins that binding with the writers (`registry.From`), a `Source` whose `Run`
instantiates the order stage and the router for `P`. Code that knows `P` lists the bound provider into a sink
of its own (`bind.Prepared.Count`, an unordered listing; the version counts and the totals observe the rows
before it and keep its runs, their intervals and its subtrees' ends), and `registry.Bound` adds the choice of
the run's sink.

## Goroutines and ownership

- **Workers** (`order.Options.Workers`) take ranges from a LIFO stack and call `Provider.Expand`; ranges of
  different nodes expand concurrently; in a run planned from a counted prior, a worker with nothing queued
  admits the plan's next tile when the [frontier window](ordering.md#the-frontier-window) allows it, else
  waits there holding no range. A worker never waits on child work: children only become claimable.
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
something: `*inventory.ConfigError` (an invalid value or combination, before anything is opened),
`*journal.MismatchError` (a resume asked with frozen settings changed), `*journal.VersionError` (a journal of
another build), `*dataset.InvariantError` (the publication gate or the row count found a violated invariant;
nothing is published), `*dataset.ReportError` (the report failed after the commit: the dataset stands),
`inventory.ErrComplete`, `ErrNothingToResume`, `ErrInterrupted`, and the S3 client's `*client.Error` (status,
code, request id, the bucket's region). `command`'s `explain` adds actionable flag advice. `Run`
returns the run report with every error, and `--report -` prints it on failure too (outcome
`failed` with the error).

## How a provider plugs in

Implement `source.Provider[R, P]` for a range type `R` and a row payload `P`, define `P`'s
`schema.Schema[P]`, and export a typed opener, `Open(ctx, root, Options)`, which returns the provider's opened
view: the provider bound to its schema (`Bound`, from `bind.Provider`) and, for a provider on the range engine,
`keyrange.Registered` set to the same provider (its `Listed` and the engine's optional interfaces, promoted),
with the provider's own `Locator`, `Identity`, `Capabilities`, `Report` and, on its own service, `Tariff` (see
[providers.md](providers.md)). The provider imports no flag package, registry or writer. Its registration is
one file in `internal/cli`, which registers a `registry.Factory` from `init`: the provider's flags (its own,
and the shared groups it names: the store's and range engine's), which of them a resume may change, its
secrets and its root form, the conversion of the parsed flags into the provider's options, and an `Open` that
calls the provider's and returns the view joined with `registry.From(view.Bound())`. The command derives its
flag set, help, resume rules and redaction from the registered factories and names no provider; it imports
`internal/cli`, not the provider, and `inventory` does the rest. The [provider integration checklist](providers.md#adding-a-provider)
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
