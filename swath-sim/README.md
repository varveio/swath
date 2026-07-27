# swath-sim

`swath-sim` runs swath's **real listing policies against a modelled store in virtual time**;
its sibling [`swath-replay-server`](../swath-replay-server) does the opposite — it runs the
**real engine against a fake S3 endpoint in real time**. Neither is a shipped artifact: both
are development/analysis tools that live in this repository only.

That one-line split is the whole reason the two modules coexist. The replay server exists to
prove the shipped engine behaves correctly end-to-end over HTTP, so it pays real socket, real
Parquet and real clock costs per page. A simulator exists to answer *"what would a different
split/steal policy have done on this bucket?"* thousands of times, so it must pay none of
them — but it must still answer every list request exactly as the real thing would, or its
answers are fiction. This module is the part that guarantees that: the **ground-truth store**.

## The seam

`swath-sim` does not define a second listing protocol. It plugs into the two layers the replay
server already owns and reuses both verbatim:

| Layer | Owns | Lives in |
|---|---|---|
| `ListObjectsV2Pager` | *All* S3 `ListObjectsV2` semantics — max-keys accounting, truncation, delimiter rollup, common-prefix resume, prefix/`start-after` interplay, continuation tokens. | `swath-replay-server` |
| `ListingStore` | Ordered **range reads** only: the first `limit` rows in `[from, toExclusive)`, ascending unsigned key order. Plus the one optional `delimitedRollup` fast path. | `swath-replay-server` |

A simulator backend is therefore *only* a `ListingStore`. If a change here starts re-deriving
when a page is truncated or where a continuation token resumes, it is in the wrong layer.

## Backends

Backends are selected explicitly through `SimStoreFactory.open(fixturePath, backend, config)`
(`SimStoreBackend`), following the replay server's `--serving-mode` / `ReplayServingFactory`
precedent. Explicit selection is the point: an automatic choice would only ever exercise one
backend per fixture, so a conformance or differential run could never compare them.

| Backend | What it is | Metadata |
|---|---|---|
| `ARENA` | Keys-only in-memory arena: segmented key-byte blocks plus long offsets, built once from any other `ListingStore` and then answered entirely from memory. For a fixture whose whole key set fits the configured budget. | **Stubbed** (sim-mode projection, below) |
| `STREAMING` | Keys-only **decode-once** tier: each row group of a sorted fixture is decoded to off-heap keys the first time a cursor reaches it, then served from memory, and evicted behind the cursors. For fixtures too large for the arena. | **Stubbed** |
| `WINDOWED` | The replay module's sequential-window prefetch decorator (`WindowedListingStore`) wrapping its sorted-Parquet store, in process. **Forced-only** — see below. | Full |
| `PARQUET` | The replay module's Parquet-backed store, unchanged. The differential reference. | Full |
| `AUTO` | `ARENA` when the fixture's encoded key bytes fit the configured budget; else `STREAMING` when the fixture is sorted-eligible; else `PARQUET`. | Depends on the resolution |

Which backend actually served a fixture is recorded, not just returned: every resolution bumps
`swath.sim.store.backend{backend}`, an `AUTO` resolution that declines the arena bumps
`swath.sim.store.arena.decline{reason}`, and one that goes on to decline the streaming tier bumps
`swath.sim.store.streaming.decline{reason}` — each logs why. A sweep's results therefore carry a
record of what produced them, so a threshold regression cannot masquerade as a throughput
regression.

`STREAMING` and `WINDOWED` both require a **sorted-eligible** fixture: a stamped, `mode=objects`,
strictly-sorted, pure-`OBJECT` capture, the same eligibility the replay server's own
`--serving-mode sorted` checks (`SortedEligibility`, shared code). A fixture that is not
sorted-eligible fails fast under either forced request, and `AUTO` falls back to `PARQUET`.

**Why `WINDOWED` is forced-only.** It decodes Parquet *inside* the serving loop: every window
refill is a bounded range query, and because the `key` column is a `BLOB` with no usable zonemaps
that query scans the whole key column — a cost proportional to the fixture, not to the window.
Amortising it over a window divides that cost by a constant and leaves the fixture-size term
intact, so on a giant fixture it is orders of magnitude off a simulated run's budget. It stays as
the memory-bounded path that carries **full metadata** and as the conformance comparison for
`STREAMING`, chosen deliberately rather than by default. Its `enabled`/`window-rows`/`max-windows`
come from the replay server's `swath.replay.prefetch.*` system properties — one tuning surface for
both callers, not a second sim-only knob for the same thing — so
`swath.replay.prefetch.enabled=false` serves the bare sorted-Parquet store here too, exactly as it
does for `--serving-mode sorted`.

### The sim-mode projection

The two keys-only tiers (`ARENA`, `STREAMING`) load **keys only**. Every metadata column they
return is a stub: size `0`, epoch-`0` last-modified, and `null` etag / storage class / owner /
checksum fields (`SimModeRows`).

This is deliberately **not** `Projection.KEYS_ONLY`, which only drops the two owner fields —
under `KEYS_ONLY` a store still materialises size, etag and dates because the replay server's
XML renderer needs them. A simulator never renders XML, and metadata is dead weight it would
pay for on every key of every fixture, so these tiers skip loading it altogether. The tradeoff
is explicit: **their responses are ground truth for keys, pagination and truncation only.**
Anything that needs real metadata must use a `PARQUET`-backed backend.

### The streaming tier: decode once, serve from memory, evict behind the cursors

A simulated run issues on the order of 150,000 store calls, so anything the serving loop does per
call is multiplied by 150,000. The streaming tier's whole design is to make *decode* not one of
those things.

- **A segment is one row group**, decoded keys-only the first time a read reaches it. A fixture's
  row groups hold hundreds of thousands of rows, so one decode is amortised over hundreds of
  1000-row pages; served calls after that are a binary search and a walk, the same shape (and the
  same cost class) as the arena tier.
- **Mid-keyspace starts seek, they do not scan.** A steal or a split starts a fresh cursor at an
  arbitrary key. The derived row-group index (`SortedFixtures`/`SortedEligibility`, shared with the
  replay server's sorted-serving path) locates the one row group containing that key by binary
  search, and decode begins there — nothing earlier in the file is touched. The
  `swath.sim.store.streaming.segment.fault{kind}` counter separates these (`seek`) from a cursor
  walking off the end of its current segment (`forward`), so a workload's access shape is readable
  from the metrics alone.
- **Memory is bounded by configuration, not by fixture size.** Decoded segments live in an
  access-ordered map bounded by `swath.sim.streaming.max-resident-bytes`; the least recently used is
  released once the budget is exceeded, which for N mostly-sequential cursors is exactly "evict
  behind the cursors". A segment costs its row group's key bytes plus 8 bytes per key: for a fixture
  whose groups hold ~300K keys of ~100 bytes, ~33 MB each, so the 1 GiB default holds ~30 of them —
  headroom for ~30 concurrent cursors on a fixture of any size. A fault transiently exceeds the
  budget by the block it is decoding (built before the eviction it triggers frees anything), so size
  a run for the budget plus one row group.

#### Why the decoded segments are off-heap, and why the layout is a file format

Segments are `java.lang.foreign.MemorySegment`s allocated from an `Arena`, not `byte[]` blocks.
Three reasons, in order of weight:

1. **`long` indexing.** The arena tier splits its key bytes across fixed-size blocks purely because
   a single Java `byte[]` caps at 2 GiB. A `MemorySegment` has no such cap, so the streaming tier
   has no block bookkeeping, no per-block addressing, and no key that can straddle a boundary.
2. **Deterministic release.** A tier whose claim is a bounded working set wants memory freed when it
   evicts, not when a collector next runs. Closing a segment's `Arena` releases its bytes at the
   moment the residency budget says they are gone — and keeps a multi-hundred-megabyte working set
   out of the heap the rest of the JVM is using.
3. **The layout is dumpable.** A segment is exactly two buffers: an `int64` offset per key, and one
   contiguous run of key bytes — Arrow's `LargeBinary` shape. Nothing in it is an in-process
   pointer, an object reference, or any other state that would not survive a round trip through a
   file. That is deliberate and is the one forward-looking constraint in the tier: decoding a
   fixture *once ever* into a sidecar and memory-mapping it on later runs
   (`FileChannel.map(..., Arena)` returns a `MemorySegment` over the same two buffers) would turn
   the per-run decode into a per-fixture one. **That sidecar is not built here** — it pays for
   itself only across many runs over the same fixture, which is a different decision than this
   tier's own single-run budget. Keeping the layout mappable costs nothing now and is what makes it
   an addition later rather than a redesign.

`KeyArena` (the arena tier) and `KeyBlock` (the streaming tier) are therefore separate on purpose:
one on-heap whole-fixture column built once at load, one off-heap per-row-group block faulted and
dropped as cursors move. They share a shape — sorted keys plus an offset table, binary-searched —
but not a lifecycle, an allocation strategy, or an addressing scheme.

### Arena sizing, and why the tier threshold is in bytes

A single Java `byte[]` cannot exceed 2 GiB, so arena key bytes are held in fixed-size
**segments** addressed by `long` offsets; a key may straddle a segment boundary and is
reassembled on read. Capacity is therefore governed by **estimated encoded bytes**
(`key bytes + (n + 1) × 8` for the offset table), never by a key count: keys are legal up to
1024 bytes each, so two fixtures with identical key counts can differ by three orders of
magnitude in footprint. `SimStoreConfig.arenaMaxEncodedBytes` is the configurable threshold;
`SimStoreFactory.open(fixturePath, backend)` reads it from the `swath.sim.arena.max-encoded-bytes`
system property, so an operator can retune the tier without a code change, while the
config-taking overload lets a caller pin it outright.

Keys are stored raw. Front-coding a sorted key set often buys 3–5×, but that is a *measured*
optimisation to make later against a real fixture, not an assumption to build in now.

## Fixtures

A fixture is a **local path** — a swath Parquet capture file, or a directory of them — supplied
by the caller (config or CLI argument). Nothing in this module hardcodes, or is allowed to
hardcode, a remote object-store location; fetching a fixture to local disk is the caller's job,
outside this module.

## Building and testing

`swath-sim` is part of the ordinary build:

```shell
./gradlew :swath-sim:test
./gradlew :swath-sim:test --tests 'io.varve.swath.sim.store.SimStoreDifferentialTest'
```
