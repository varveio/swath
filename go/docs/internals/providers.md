# Providers

## The contract (`internal/source`)

```go
type Emitter[R any] interface {
    Rows(rows []schema.Row) error // next rows of this range, ascending; the slice now belongs to the caller
    Child(r R) error              // a child range whose whole subtree sorts here
}
type Provider[R any] interface {
    Root() R
    Expand(ctx context.Context, r R, e Emitter[R]) error
}
```

A range is a provider-defined value. `Expand` emits its content in ascending key order: every key of a child's
subtree sorts after everything emitted before the child and before everything emitted after it. `Expand` may
call `Rows` many times (streaming) and runs concurrently for different ranges. Per-entry problems (permission
denied, a vanished entry) are counted by the provider; `Expand` returns an error only when the listing must
stop, including any error returned by the emitter (cancellation or a failed sink).

## Filesystem (`internal/source/fs`)

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCAL_ENGINE=portable`): `os.File.ReadDir` + `Lstat`, re-checking that an opened child directory is
  the one that was listed.
- **Tiers** (`-fields`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `birth_time`).
- Symlinks are never followed (`O_NOFOLLOW`). Names that are not valid UTF-8 are counted and skipped (a
  directory's whole subtree). Mount points are crossed; there is no `-one-file-system` yet.

## S3 (`internal/source/s3`)

**Client.** The AWS SDK for Go v2 behind the `API` interface (one method, `ListObjectsV2`): default
credential chain, anonymous when it finds none, standard retryer (9 attempts, backoff up to 5 s), counted.

**Paging.** A range is `(lo, hi]`: `lo = ""` is ⊥, the open frontier has `hi = ⊤`. The first page sends
`StartAfter = lo`, later pages the continuation token. Keys past `hi` are trimmed (`overfetch_keys`); a page
whose last key equals `hi` ends the range without another request; `IsTruncated = false` ends it. A truncated
page without a token, a truncated page with no key in range, or a key not after its predecessor is an error.

**Root and seed.** The root is `(⊥, ⊤]`. With `-seed` the root is one delimiter page that emits no rows and
tiles the scope at its common prefixes `(⊥,p1], …, (pk, ⊤]`: unusable start-after values are dropped; at most
`min(1000, 4 × workers)` cuts, evenly subsampled; and swath's closing sentinel
`(pk, prefixCeil(pk)], (prefixCeil(pk), ⊤]` when the page was complete and ended with a prefix.

**Splits are owner-side.** A range's children become claimable only when its `Expand` returns, so there are
no thieves. After each full page the owner takes the provider mutex and computes demand,
`hunger = workers − active − queued`. If `hunger > 0` and the range ranks among the `hunger` largest eligible
ranges (open frontier first, then keys listed, then age), it emits `Child((c, m])` and `Child((m, hi])` and
returns, where `c` is the last key emitted. `(lo, hi] = (lo, c] ∪ (c, m] ∪ (m, hi]`, disjoint and in order.
Every split feeds an idle worker and no LIST is repeated.

**Guards**, checked before any child and counted per clause: `done` (never after the page that ended the
range), `unsafe_cursor` (`c` must be a usable start-after), `cursor` (`m > c`), `bound` (`m < hi`, or below
`prefixCeil(scope)` on the frontier), `no_pivot` (the owner keeps listing).

**Pivots.** First choice: density extrapolation from the last page. Keys are read as numbers in the alphabet
seen on that page; the target is `c + page span × max(4, hunger)`, at most halfway to `hi`, cut after its first
character that differs from `c`. Fallback: swath's `ByteMidpoint` (bounded midpoint; `ForwardReflect` on the
frontier; the 1024-byte cap). Every pivot is safe (I12). Extrapolation is a deliberate deviation from the
Java spec: on the synthetic T6 keyspace midpoints alone leave most lower children empty.

**Counters** (in the run summary): objects, bytes, pages, requests, retries, `overfetch_keys`; seed
(prefixes, cuts, unsafe, subsampled, sentinel and why declined); splits (considered, extrapolated, accepted
bounded and frontier, rejected per guard clause, skipped for no demand, outranked or not continuing);
`child_empty.{lower,upper}` (the cost of not probing); `max_active`, `max_queued`, `mean_active`; busy and
sink-wait time.

**Sequential oracle.** `-sequential` lists the scope as one continuation-token chain; its dataset is
byte-identical to the parallel core's. It is the only mode where `-max-keys` stops early (the dataset is
then marked incomplete).

**What the full engine adds** (later steps, each by measured need): durable worklist and resume (step 2);
structure and `max_keys=1` probes; density-aware pivots from probes; the owner gate chain, pacing and futility
backoff; AIMD concurrency; mass-aware and multi-level seeding; the rate-anchored size estimator; versions mode.

## Next providers

GCS lists in unsigned UTF-8 byte order with continuation tokens: S3-shaped. Azure Blob lists in UTF-16
code-unit order, so pivots must stay out of the zone where it differs from byte order and ranges are sorted
internally. Google Drive is unsorted: a folder is a range, read fully and sorted by the filesystem key rule.
