# Architecture

`swath` lists a source (a local directory tree or an S3 bucket today) and writes a globally key-sorted
Parquet dataset in swath's sorted-dataset layout. It is one pure-Go (`CGO_ENABLED=0`) static binary.
The language-neutral contracts in [`../../docs/internals/`](../../docs/internals/) bind it; the Go statement of
them is [internals/contracts.md](internals/contracts.md).

## Pipeline

```text
provider ──ranges──▶ order ──ascending row segments──▶ parquet router ──parts──▶ encoders ──▶ dataset publish
(source/fs, source/s3) (bounded reorder)                 (boundaries, ordinals)   (one goroutine per part)
```

Order comes from the ranges, not from a sort afterwards. A provider expands a *range* into rows that are
already ascending and into child ranges that sort between them. The order stage walks that tree depth-first,
which is global key order, and streams rows at the frontier with bounded memory. The router cuts deterministic
part boundaries; per-part encoders write them in parallel; the dataset package publishes them in order.
There is no external sort, no staging and no merge.

## Packages

| Package | Role | Imports |
|---|---|---|
| `cmd/swath` | flags into `listing.Config`, with each provider's own flag group; `signal.NotifyContext`; usage generated from the flag set | listing, source; providers only to register them (`_` imports) |
| `internal/listing` | `Run(ctx, Config) (Summary, error)`: opens the source by its root's scheme, wires it to order, router, dataset; the run summary | source and all below, no provider |
| `internal/source` | the provider contract: `Provider[R]`, `Emitter[R]`, `Disposer[R]`; the registry (`Factory` by URL scheme, `Source`, `Erase`) and the optional interfaces (`Identified`, `Resumable`, `Capable`, `Excluder`, `Reporter`) | schema |
| `internal/source/fs` | filesystem provider: Linux `getdents64` engine, portable engine; registers the scheme-less roots | source, schema, dataset |
| `internal/source/s3` | S3 provider: `(lo, hi]` ranges, seed, owner-side splits, pivots; registers `s3://` | source, schema, s3/client |
| `internal/source/s3/client` | ListObjectsV2: SigV4, credentials, retries, the page parser | – |
| `internal/source/s3/sdkoracle` | the AWS SDK behind the provider's `API`; imported by tests only | s3/client |
| `internal/order` | bounded reorder of a range tree into one ascending stream | source, schema |
| `internal/schema` | `Row`, schemas as data (`Local`, `S3`), the sort rule, the Parquet message string | – |
| `internal/parquet` | file writer, schema binding, codec, part router, encoders | schema, dataset |
| `internal/dataset` | layout, overwrite safety, manifest, footer stamp names, publication gate and order | faults |
| `internal/durable` | the journal (CRC-framed records, one writer goroutine), resume identity | dataset |
| `internal/counters` | the counter table (definitions + one atomic each); summaries and docs are generated from it | – |
| `internal/faults` | named crash points for the crash-matrix tests (one atomic load in production) | – |

`order` and `parquet` know nothing about sources; providers know nothing about Parquet. The pipeline packages
(listing, order, parquet, dataset, schema, durable) import nothing under `internal/source/`, and
`internal/source` imports no provider: `internal/source` `TestImportBoundary` checks it, transitively. `schema.Row` is shared:
filesystem fields inline, object-store fields behind one pointer (`Row.Object`, nil for filesystem rows).

## Goroutines and ownership

- **Workers** (`order.Options.Workers`) take ranges from a LIFO stack and call `Provider.Expand`; ranges of
  different nodes expand concurrently. A worker never waits on child work: children only become claimable.
- **One drainer** (the goroutine that called `order.Run`) walks the node tree depth-first and calls the sink
  (the router) with contiguous ascending segments. It expands a queued range itself when it needs it and no
  worker has claimed it, so a full buffer cannot deadlock the walk.
- **Publication**: with a journal (S3 with `-out`), each encoder goroutine publishes its part when it closes,
  after the previous part's publication (a channel per part keeps ordinal order): journal record, then
  rename. See [internals/durability.md](internals/durability.md).
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

## How a provider plugs in

Implement `source.Provider[R]` for a range type `R` (see [internals/providers.md](internals/providers.md)),
choose a `schema.Schema`, and add a branch in `listing.Run` that builds the provider and fills
`dataset.Meta` (source kind, root or bucket, schema id). The generic `pipeline[R]` in `internal/listing` does
the rest. A provider whose listing API is not sorted (Google Drive) sorts within each range; one whose order
is not unsigned byte order (Azure's UTF-16 order) must keep its range boundaries where the two orders agree.

## Memory

One `-memory` budget: half of it is a byte budget for rows in flight, shared by the order stage's buffers
and the encoders' queues. The other half is headroom for pages being parsed, codecs and the garbage
collector. See [internals/ordering.md](internals/ordering.md) for what may exceed the budget.

## Further reading

[internals/ordering.md](internals/ordering.md) · [internals/parquet-output.md](internals/parquet-output.md) ·
[internals/durability.md](internals/durability.md) ·
[internals/providers.md](internals/providers.md) · [internals/contracts.md](internals/contracts.md) ·
[testing.md](testing.md) · [cli.md](cli.md) · measurements: [../REPORT.md](../REPORT.md)
