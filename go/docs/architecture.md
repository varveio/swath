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
| `cmd/swath` | flags into `listing.Config`; `signal.NotifyContext`; usage generated from the flag set | listing |
| `internal/listing` | `Run(ctx, Config) (Summary, error)`: wires provider, order, router, dataset; the run summary | all below |
| `internal/source` | the provider contract: `Provider[R]`, `Emitter[R]` | schema |
| `internal/source/fs` | filesystem provider: Linux `getdents64` engine, portable engine | source, schema |
| `internal/source/s3` | S3 provider: `(lo, hi]` ranges, seed, owner-side splits, pivots | source, schema |
| `internal/order` | bounded reorder of a range tree into one ascending stream | source, schema |
| `internal/schema` | `Row`, schemas as data (`Local`, `S3`), the sort rule, the Parquet message string | – |
| `internal/parquet` | file writer, schema binding, codec, part router, encoders | schema, dataset |
| `internal/dataset` | layout, overwrite safety, manifest, footer stamp names, publication gate and order | – |

`order` and `parquet` know nothing about sources; providers know nothing about Parquet. `schema.Row` is shared:
filesystem fields inline, object-store fields behind one pointer (`Row.Object`, nil for filesystem rows).

## Goroutines and ownership

- **Workers** (`order.Options.Workers`) take ranges from a LIFO stack and call `Provider.Expand`; ranges of
  different nodes expand concurrently. A worker never waits on child work: children only become claimable.
- **One drainer** (the goroutine that called `order.Run`) walks the node tree depth-first and calls the sink
  (the router) with contiguous ascending segments. It expands a queued range itself when it needs it and no
  worker has claimed it, so a full buffer cannot deadlock the walk.
- **Encoders**: the router starts one goroutine per open part, at most `Encoders` at a time (a semaphore
  channel), each with its own writer and zstd encoder, fed through a bounded channel. When fewer parts than
  encoders are open, a part encodes a page's columns on several goroutines; the bytes are identical.
- **Ownership.** A slice passed to `Emitter.Rows` belongs to the order stage from then on; the provider must not
  touch it. Segments handed to the sink belong to the router until the encoder has written them. parquet-go
  copies byte-array values, so rows may alias provider buffers (keys are zero-copy views into a per-page or
  per-directory arena).
- **Cancellation.** Every stage takes the `context.Context`. The first error stops the walk (an atomic flag,
  checked without taking `ctx`'s mutex), wakes every waiter, closes the encoders, removes temporary parts and
  is returned wrapped with its path or phase. `_SUCCESS` is written only after a complete publish.

## How a provider plugs in

Implement `source.Provider[R]` for a range type `R` (see [internals/providers.md](internals/providers.md)),
choose a `schema.Schema`, and add a branch in `listing.Run` that builds the provider and fills
`dataset.Meta` (source kind, root or bucket, schema id). The generic `pipeline[R]` in `internal/listing` does
the rest. A provider whose listing API is not sorted (Google Drive) sorts within each range; one whose order
is not unsigned byte order (Azure's UTF-16 order) must keep its range boundaries where the two orders agree.

## Memory

One `-memory` budget: the reorder bound is `memory / 2 KiB` rows; encoder channels hold at most
`Encoders × 16` segments. See [internals/ordering.md](internals/ordering.md) for what may exceed the budget.

## Further reading

[internals/ordering.md](internals/ordering.md) · [internals/parquet-output.md](internals/parquet-output.md) ·
[internals/providers.md](internals/providers.md) · [internals/contracts.md](internals/contracts.md) ·
[testing.md](testing.md) · [cli.md](cli.md) · measurements: [../REPORT.md](../REPORT.md)
