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
| `ARENA` | Keys-only in-memory arena: segmented key-byte blocks plus long offsets, built once from any other `ListingStore` and then answered entirely from memory. | **Stubbed** (sim-mode projection, below) |
| `WINDOWED` | The replay module's sequential-window prefetch decorator (`WindowedListingStore`) wrapping its sorted-Parquet store, in process. For a fixture too large for the arena but still sorted-eligible. | Full |
| `PARQUET` | The replay module's Parquet-backed store, unchanged. The differential reference. | Full |
| `AUTO` | `ARENA` when the fixture's encoded key bytes fit the configured budget; else `WINDOWED` when the fixture is sorted-eligible; else `PARQUET`. | Depends on the resolution |

Which backend actually served a fixture is recorded, not just returned: every resolution bumps
`swath.sim.store.backend{backend}`, an `AUTO` resolution that declines the arena bumps
`swath.sim.store.arena.decline{reason}`, and one that goes on to decline the windowed tier bumps
`swath.sim.store.windowed.decline{reason}` — each logs why. A sweep's results therefore carry a
record of what produced them, so a threshold regression cannot masquerade as a throughput
regression.

`WINDOWED` requires a **sorted-eligible** fixture: a stamped, `mode=objects`, strictly-sorted,
pure-`OBJECT` capture, the same eligibility the replay server's own `--serving-mode sorted` checks
(`SortedEligibility`, shared code). A fixture that is not sorted-eligible fails fast under a
forced `WINDOWED` request, and falls back to `PARQUET` under `AUTO`. The decorator's own
`enabled`/`window-rows`/`max-windows` come from the replay server's `swath.replay.prefetch.*`
system properties — one tuning surface for both callers, not a second sim-only knob for the same
thing — so `swath.replay.prefetch.enabled=false` serves the bare sorted-Parquet store here too,
exactly as it does for `--serving-mode sorted`.

### The sim-mode projection

The arena loads **keys only**. Every metadata column it returns is a stub: size `0`, epoch-`0`
last-modified, and `null` etag / storage class / owner / checksum fields.

This is deliberately **not** `Projection.KEYS_ONLY`, which only drops the two owner fields —
under `KEYS_ONLY` a store still materialises size, etag and dates because the replay server's
XML renderer needs them. A simulator never renders XML, and metadata is dead weight it would
pay for on every key of every fixture, so the arena skips loading it altogether. The tradeoff
is explicit: **arena responses are ground truth for keys, pagination and truncation only.**
Anything that needs real metadata must use a `PARQUET`-backed backend.

### Sizing, and why the tier threshold is in bytes

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

```
./gradlew :swath-sim:test
./gradlew :swath-sim:test --tests 'io.varve.swath.sim.store.ArenaDifferentialTest'
```
