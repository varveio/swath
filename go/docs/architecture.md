# Architecture

`swath` lists a source (a local directory tree, an S3 bucket or a GCS bucket) and writes a globally key-sorted
Parquet dataset in swath's sorted-dataset layout. It is one pure-Go (`CGO_ENABLED=0`) static binary.
The language-neutral contracts in [`../../docs/internals/`](../../docs/internals/) bind it; the Go statement of
them is [internals/contracts.md](internals/contracts.md).

## Pipeline

```text
provider ──ranges──▶ order ──ascending row segments──▶ parquet router ──parts──▶ encoders ──▶ dataset publish
(localfs, s3)           (bounded reorder)                 (boundaries, ordinals)   (one goroutine per part)
```

Order comes from the ranges, not from a sort afterwards. A provider expands a *range* into rows that are
already ascending and into child ranges that sort between them. The order stage walks that tree depth-first,
which is global key order, and streams rows at the frontier with bounded memory. The router cuts deterministic
part boundaries; per-part encoders write them in parallel; the dataset package publishes them in order.
There is no external sort, no staging and no merge.

## Packages

| Package | Role | Imports |
|---|---|---|
| `cmd/swath` | the commands (`list`, `resume`, `show`, `diag list`); flags into `listing.Config`'s three groups (`Source`, `Output`, `Limits`, validated once by `Run`: a `*listing.ConfigError` for a bad value or combination), with each provider's own flag group; `signal.NotifyContext`; usage generated from the flag set | listing, registry, journal, dataset; providers only to register them (`_` imports) |
| `internal/listing` | `Run(ctx, Config) (Report, error)`: opens the source by its root's scheme, wires it to order, router, dataset; the run report | source and all below, no provider |
| `internal/source` | the provider contract: `Provider[R, P]`, `Emitter[R, P]`, `Disposer[R]`, `Erase`; the optional interfaces (`Identified`, `Resumable`, `Capable`, `Excluder`, `Reporter`) | schema |
| `internal/registry` | `Factory` by URL scheme, `Register`, `Lookup`; `Source`, an opened provider bound to the typed stages (`Bind[R, P]`, `Run`) | source, schema, order, parquetout |
| `internal/source/localfs` | filesystem provider: Linux `getdents64` engine, portable engine; its row payload `Entry` and schema `swath-local/v1`; registers the scheme-less roots | source, schema, registry, dataset |
| `internal/source/keyrange` | the range engine: `(lo, hi]` ranges, seed, owner-side splits, safe pivots, guards, probes, requeue, counters; behind a page fetch and a provider description | source, schema, counters, faults |
| `internal/source/keyrange/keyrangetest` | test fixtures: a fake listing API over sorted keys, the fixture and hostile keyspaces; imported by tests only | – |
| `internal/source/s3` | S3 provider on the range engine: the ListObjectsV2 page fetch, its row payload `Object` and schema `swath/s3-objects/v1`; registers `s3://` | source, schema, registry, keyrange, s3/client |
| `internal/source/gcs` | GCS provider on the range engine: the objects.list page fetch, its row payload `Object` and schema `swath/gcs-objects/v1`; registers `gs://` | source, schema, registry, keyrange, gcs/client |
| `internal/source/objhttp` | what both object-store clients share: the retry loop (backoff, exhaustion), failure classes and transport classification, credential errors, client counters, route canonicalization; `objhttptest` holds the retry contract both clients pass | counters |
| `internal/source/s3/client` | ListObjectsV2: SigV4, credentials, S3's answers and their classes, the page parser | objhttp |
| `internal/source/gcs/client` | objects.list: OAuth2 credentials, the JSON API's answers and their classes, the page parser | objhttp |
| `internal/source/s3/sdkoracle` | the AWS SDK behind the provider's `API`; imported by tests only | s3/client |
| `internal/order` | bounded reorder of a range tree into one ascending stream | source, schema |
| `internal/schema` | provider-neutral: `Row[P]` (a key and a typed payload), `Schema[P]` (columns as accessors of P, logical and heap size, spill `Codec[P]`), `Info` | – |
| `internal/parquetout` | file writer, schema binding, codec, part router (`Router[P]`), encoders | schema, dataset |
| `internal/dataset` | layout, overwrite safety, manifest, footer stamp names, publication gate and order | faults |
| `internal/journal` | the journal (CRC-framed records of committed parts, synchronous appends by the ordered committer), resume identity | dataset |
| `internal/counters` | the counter table (definitions + one atomic each); summaries and docs are generated from it | – |
| `internal/faults` | named crash points for the crash-matrix tests (one atomic load in production) | – |

`order` and `parquetout` know nothing about sources; providers know nothing about Parquet. The pipeline packages
(listing, order, parquetout, dataset, schema, journal, registry) import nothing under `internal/source/`, and
`internal/source` and `internal/registry` import no provider: `internal/source` `TestImportBoundary` checks it,
transitively.

**Typed rows.** A row is `schema.Row[P]`: its key and the producer's payload `P` (`fs.Entry`, `s3.Object`), so
an S3 row carries no filesystem slots and the reverse. The producer defines `P` and its `schema.Schema[P]`
beside its code: columns as accessors of `P`, the logical size the router cuts parts by, the heap bytes the
budget counts, and the spill codec. The order stage and the router are generic over `P`; the pipeline
(`listing`) never names it. The one place that does is `registry.Bind[R, P]`, which a provider calls when it
opens: it erases the range type and returns a `Source` whose `Run` instantiates the order stage and the
router for `P` (Go has no generic methods, so the binding happens where `P` is known).

## Goroutines and ownership

- **Workers** (`order.Options.Workers`) take ranges from a LIFO stack and call `Provider.Expand`; ranges of
  different nodes expand concurrently. A worker never waits on child work: children only become claimable.
- **One drainer** (the goroutine that called `order.Run`) walks the node tree depth-first and calls the sink
  (the router) with contiguous ascending segments. It expands a queued range itself when it needs it and no
  worker has claimed it, so a full buffer cannot deadlock the walk.
- **Publication**: with a journal (S3 or GCS with an output directory), each encoder goroutine publishes its part when it closes,
  after the previous part's publication (a channel per part keeps ordinal order): journal record, then
  rename. That chain is the one ordered committer; the journal's append is a synchronous call. See [internals/durability.md](internals/durability.md).
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
code, request id, the bucket's region). No package names a flag: `cmd/swath`'s `explain` adds the advice. `Run`
returns the run report with every error, and `--report-format json` prints it on failure too (outcome
`failed` with the error).

## How a provider plugs in

Implement `source.Provider[R, P]` for a range type `R` and a row payload `P`, define `P`'s
`schema.Schema[P]`, and register a `registry.Factory` from `init` whose `Open` returns
`registry.Bind(provider, &schema)` embedded in a value with the `Locator`, `Listed` and the optional interfaces
(see [internals/providers.md](internals/providers.md)). The command imports the package for that registration
only; `listing` does the rest.

## Memory

One `-memory` budget: half of it is a byte budget for rows in flight, shared by the order stage's buffers
and the encoders' queues. The other half is headroom for pages being parsed, codecs and the garbage
collector. See [internals/ordering.md](internals/ordering.md) for what may exceed the budget.

## Further reading

[internals/ordering.md](internals/ordering.md) · [internals/parquet-output.md](internals/parquet-output.md) ·
[internals/durability.md](internals/durability.md) ·
[internals/providers.md](internals/providers.md) · [internals/contracts.md](internals/contracts.md) ·
[testing.md](testing.md) · [cli.md](cli.md) · measurements: [../REPORT.md](../REPORT.md)
