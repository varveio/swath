# swath (Go port) design

The Go port of swath (`go/`, command `cmd/swath`) lists a tree and writes a globally key-sorted Parquet
dataset in Swath's sorted layout. The local filesystem is the first provider; the core (ordering, encoding,
publication) is provider-agnostic so that S3, GCS, Azure Blob or Google Drive can be added as further
providers.

## Pipeline

```text
provider ──ranges──▶ order ──sorted segments──▶ parquet router ──parts──▶ encoders ──▶ dataset publish
 (source/fs)         (bounded reorder)          (boundaries, ordinals)   (N writers)   (manifest, _SUCCESS)
```

There is one pipeline. Rows are never globally sorted: the provider produces a tree of ranges whose rows are
already ordered, and the order stage emits them depth-first, which is global key order. There is no external
sort, no spill run and no staging other than the unpublished `data/*.parquet.tmp` parts.

`listing.Run(ctx, Config) (Summary, error)` runs it; `cmd/swath` only parses flags and installs
`signal.NotifyContext`. Every stage takes the context; the first error (or cancellation) stops the walk,
closes the encoders, removes the temporary parts and is returned wrapped with its path or phase.

## Packages

| Package | Role | Imports |
|---|---|---|
| `cmd/swath` | flags → `listing.Config`, usage generated from the flag set | listing |
| `internal/listing` | `Run`: wire provider, order, router, dataset; summary; resource usage | all below |
| `internal/source` | the provider contract (`Provider`, `Emitter`) | schema |
| `internal/source/fs` | local filesystem provider: Linux getdents engine, portable engine | source, schema |
| `internal/order` | bounded reorder of a range tree into one ascending stream | source, schema |
| `internal/schema` | `Row`, column list as data, logical types, sort rule, message string | – |
| `internal/parquet` | file writer, schema binding, codec, part router and encoders | schema, dataset |
| `internal/dataset` | layout, overwrite safety, manifest, sort stamps, publication gate and order | – |

`order` and `parquet` know nothing about filesystems; `source/fs` knows nothing about Parquet. The row
type (`schema.Row`) is shared by all providers: filesystem fields inline, object-store fields behind one
pointer (`Row.Object`, nil for filesystem rows), so adding S3 changed the row type but not the contract.

## The provider contract (`internal/source`)

```go
type Emitter[R any] interface {
    Rows(rows []schema.Row) error // next rows of this range, ascending; ownership passes
    Child(r R) error              // a child range whose whole subtree sorts here
}
type Provider[R any] interface {
    Root() R
    Expand(ctx context.Context, r R, e Emitter[R]) error
}
```

A **range** is a provider-defined value. Its content is a sequence of rows and child ranges in ascending key
order: every key of a child's subtree sorts after the rows and children emitted before it and before
everything emitted after it. `Expand` may call `Rows` many times while the range is still open (streaming),
and is called concurrently for different ranges. Per-entry problems (permission denied, vanished entry) are
the provider's to count; `Expand` returns an error only when the listing must stop (including errors returned
by the emitter, which signal cancellation or a failed sink).

- **Filesystem.** A range is a directory. Its entries are sorted by key, where a directory's key is
  `prefix/name/` (its DIR row key) and anything else is `prefix/name`; byte order puts `a-b` < `a.b` < `a/` <
  `a/…` < `a0`, so emitting the DIR row and then its subtree before the next sibling is global order. A
  directory with more than 4096 entries is split into chunk ranges of 2048 sorted names that share one
  reference-counted directory fd, so other workers stat them in parallel.
- **S3** (`internal/source/s3`): the basics of Swath's parallel engine, without the full engine.
  - **Ranges are `(lo, hi]`.** S3 `start-after` is exclusive, so a boundary key belongs to the left range;
    `lo = ""` is ⊥ and the open frontier has hi = ⊤. A range's first page uses `StartAfter=lo` and later
    pages use continuation tokens. Keys past `hi` are trimmed (`overfetch_keys`), and a page whose last key
    equals `hi` ends the range without another request. Paging stops on `IsTruncated == false`. A truncated
    page without a token, a truncated empty page, or a key not after the previous one is an error.
  - **Root.** `(⊥, ⊤]`. With `-seed` the root is one delimiter page that emits no rows and tiles the scope
    at its common prefixes `(⊥,p1], …, (pk, ⊤]`:
    - unusable start-after values are dropped;
    - there are at most `min(1000, 4×workers)` cuts, evenly subsampled;
    - Swath's closing sentinel `(pk, prefixCeil(pk)], (prefixCeil(pk), ⊤]` applies when the page was
      complete and ended with a prefix.
  - **Splits are owner-side.** `order` makes a range's children claimable only when its `Expand` returns,
    so a thief could not run a child of a still-running range. Instead, after each full page the owner
    checks demand under the provider mutex: `hunger = workers − active − queued`. If `hunger > 0` and the
    range ranks among the `hunger` largest eligible ranges, it emits `Child((c, m])` and
    `Child((m, hi])` and returns. Ranking puts the open frontier first, then orders by keys listed, then by
    age. `c` is the last key emitted. Every split is therefore a steal for an idle worker, and no LIST is
    repeated. `(lo, hi] = (lo, c] ∪ (c, m] ∪ (m, hi]`, disjoint and in order, which the `Emitter` contract
    allows.
  - **Guard**, checked before any child and counted per clause:
    - `done`: never after the page that ended the range;
    - `unsafe_cursor`: `c` becomes a start-after;
    - `cursor`: `m > c`;
    - `bound`: `m < hi`, or below `prefixCeil(scope)` on the frontier;
    - `no_pivot`: the owner then finishes the range.
  - **Pivot.** Swath's `ByteMidpoint` is ported: safe scalars only, the bounded midpoint, `ForwardReflect`
    for the frontier, and the 1024-byte fallback. **Deviation from the spec:** the first choice is density
    extrapolation from the last page:
    - keys are read as numbers in the alphabet seen on that page;
    - the target is `cursor + page span × max(4, hunger)`, at most halfway to `hi`;
    - it is cut after its first character that differs from the cursor.

    The midpoint and reflection remain the fallback when that does not yield a safe key. The reason is the
    spec's synthetic balance check T6 (`p/%07d`, 200k keys, 20 ms per page, 16 workers). With midpoint and
    reflection only, most lower children are empty: the cursor and `hi` diverge at adjacent scalars
    (`'/'`, `'0'`), or reflection from ⊥ moves only the last character. That run took 2.58 s; with
    extrapolation it took 1.15 s (both `max_active = 16`).
  - **No probes.** A split that lands in empty space costs one empty-child LIST, counted as
    `child_empty.{lower,upper}`.
  - **Retries.** The SDK's standard retryer, 9 attempts, backoff to 5 s, counted.
  - **The sequential continuation-token listing** stays as the reference oracle (`-sequential`). Its
    output, and the Parquet built from it, is byte-identical to the parallel core's.

  **What the full engine adds** (left out here):
  - durability: checkpoint, resume and `durable_cursor`;
  - thief-side narrowing, with the per-key `hi` re-read and the commit re-trim it requires;
  - the structure probes and `max_keys=1` probes;
  - the owner gate chain, pacing and futility backoff;
  - mass-aware and multi-level seeding;
  - the rate-anchored size estimator;
  - AIMD concurrency;
  - versions mode.
- **Checkpoint (not implemented).** The order stage's frontier is a key cursor: every key below the last
  emitted key is in a finished part or in the open one. A checkpoint is (last published part, its max key, the
  set of ranges not fully drained, each with its own resume cursor). Resume re-expands those ranges from their
  cursors; parts are published only when complete, so nothing is emitted twice.
- **GCS** lists in byte order like S3 and plugs in the same way. **Google Drive** has no byte-sorted listing
  API: its provider reads a folder fully, sorts it with the same key rule, and emits it as the filesystem
  provider does (a folder is a range).
- **Azure Blob** lists in UTF-8 binary order per container: like S3.

## Ordering (`internal/order`)

`order.Run(ctx, provider, sink, Options)` keeps a tree of nodes. Workers take queued ranges from a LIFO stack
(children are pushed in reverse order, so the next-needed range is on top) and call `Expand`; `Rows` and
`Child` append to the node's item list. One drainer walks the tree depth-first and hands contiguous row
segments to the sink in batches of at least 4096 rows. It streams the frontier: rows of the range it is
draining are consumed as they arrive, not when the range completes.

When the drainer expands the range it is draining itself (no worker had claimed it), rows up to that range's
first child are next in global order and go straight to the sink instead of the buffer; without this a
bucket-wide S3 range expanded by the drainer was buffered whole.

Memory: buffered rows are bounded by `Options.Budget` (rows). A worker blocks before claiming a new range,
and when publishing rows, while the buffer is full, unless the drainer is waiting for that very range or
expanding it itself; the drainer expands a queued range itself when it needs it and no worker has it, so a
full buffer cannot deadlock the walk. A long streaming range on the drain path (an S3 prefix) therefore also
waits while the sink is behind; an earlier rule that exempted the whole drain path let a bucket-wide range
buffer every row (found with the S3 provider: 971,927 rows buffered). On top of the budget the walk holds the
rows still pending on the root-to-frontier path (at most one range per level, the O(depth × fan-out) of any
depth-first walk) and one publish batch per worker. Emitters collect a range's items locally and publish them
under the lock at once when the drainer is on that range, or every 1024 rows, or when the range ends: one
lock round trip per directory, while long streaming ranges stay visible as they grow.

## Schema (`internal/schema`)

A schema is data: `Schema{Name, ID, SortOrder, Columns}`; each column has a name, a type (`String`, `Int32`,
`Int64`, `TimestampMicros`, `Bool`), required/optional and an accessor on `Row`. `Message()` renders the
parquet-mr message string stored in the manifest; the Parquet node is built from the same list, so there is
one source of truth. `Local` is `swath-local/v0` (15 columns; `key`, `size`, `last_modified` identical to
Swath). `SwathS3` describes Swath's canonical 13-column S3 schema as column specs only (no accessors): it
exists to show the same model expresses it; no S3 column is added to the local schema.

## Parquet (`internal/parquet`)

- **Writer**: one file; parquet-go column-values path (no reflection per row); served geometry (1024-row
  pages, 8 MiB row groups, 8 KiB dictionary limit, 1024-byte column-index bounds, v2 pages, page statistics
  off); delta key encoding with an ascending single-page column index; bare INT32/INT64 adapters; pure-Go
  zstd (klauspost, SpeedDefault, all-literal entropy). A page's columns may be encoded by several goroutines;
  the bytes are identical to serial encoding (tested).
- **Router** (the `order` sink): owns part boundaries. A row's logical size is its uncompressed field bytes.
  Part 0 is cut at `PartSize / 0.1` logical bytes (0.1 is the assumed compression ratio, close to the 0.11
  measured on the realistic tree); once part 0 is closed its real ratio is frozen and every later part is cut
  at `PartSize / ratio`. Nothing is re-estimated, so boundaries depend only on the rows. Ordinals are dense;
  the router knows the last part and sets `swath.sort.file_final`.
- **Encoders**: each part has its own goroutine, serial writer and zstd encoder, fed through a bounded
  channel (one message per order batch) while the part is open; at most `Encoders` parts are in flight. When fewer parts than encoders are
  active, a part encodes its page columns in parallel, so the single-part case uses all cores.
- **Gate** before publication: dense ordinals, strictly ascending disjoint key ranges, rows written ==
  rows routed.

## Dataset (`internal/dataset`)

Layout `out/data/part-NNNNN.parquet`, `manifest.json`, `symlink.txt`, `_swath_local_summary.json`,
`_SUCCESS` last. Parts are written as `.tmp`, fsynced, renamed in ordinal order; each JSON file is written
`.tmp`, fsynced, renamed, then the directory is fsynced. `Prepare` validates `out` against the root (never
the root or an ancestor, by path and by dev/inode; overwrite only a previous swath-local dataset, never
inside the root, removing only its own entries); `LocateInside` finds an output inside the root so the
provider skips it. Manifest fields mirror Swath's `Manifest.java`, plus `schemaId`, `sourceKind`,
`sourceRoot`, `totalRows`.

## Choices and limits

- Engines: `getdents` on Linux (raw `getdents64`, names parsed into a reused buffer, keys in one arena per
  directory, entries sorted before stat so each stat lands in its final slot), `portable` elsewhere
  (`os.File.ReadDir` + `Lstat`, re-checking that a child directory was not swapped for a symlink). Tiers:
  `names` (no stat), `stat` (`fstatat`), `statx` (full mask, adds `birth_time`).
- Memory: one `--memory` budget; the reorder bound is `memory / 2 KiB` rows (about 130k rows at the 256 MiB
  default, near the measured optimum of 100k); encoder channels hold at most `Encoders × 16` segments.
- Not done: mount-point and cycle detection (`--one-file-system`), resume, non-UTF-8 names (counted and
  skipped), keys over 1024 bytes are not rejected.
