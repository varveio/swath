# Direction note: the replay server as a generic S3-listing query server

**Status:** direction note (owner-originated, 2026-07-04). Not a plan, not a commitment —
a documented framing so future work recognizes what it's actually building toward.
**Relates to:** `docs/proposals/replay-serving-index-at-scale.md` (role tiers),
`docs/sorted-serving-performance.md` (measured cost model),
`notes/2026-07-04-parquet-expert-perf-ideas.md` (the perf ladder + experiment verdicts).

## The reframe

`s3-listing-replay-server` was built to replay captured listings byte-identically for
swath testing. But its serving core — **globally sorted, stamped Parquet + a derived
sparse routing index + bounded range reads + windowed prefetch** — is not replay-specific.
It is a general answer to: *"serve any S3-listing-shaped query over a large key-sorted
corpus, fast, from a cheap immutable file."*

The owner's articulated future use case (varve): browsing/querying **raw S3 inventory**
corpora that have nothing to do with swath testing. Same data shape (key-sorted object
metadata), same query shapes (prefix/range pages, delimiter rollups, point lookups), same
constraints (files cheap to produce and store; server memory bounded, not f(N)).

Naming follows the framing: if this direction is pursued, the component's identity is a
**listing server** (working name to be chosen — "replay" would become one *mode* or one
client of it, not the name).

## What generalizes as-is (already built, measured)

- Sorted stamped Parquet as the serving artifact (self-describing via `swath.sort.*`
  footer KVs, multi-file range-partitioned roll supported, eligibility fail-fasts).
- Startup-derived in-memory routing index (`(file, row-group, first-key, row-count)`),
  1.0s at 18.3M rows; SSTable-family sparse-index design (see
  `docs/sorted-serving-performance.md`).
- Flat-in-N bounded serving: 45–55ms/page at 2M→78M via DuckDB (role 2).
- Windowed sequential prefetch (2026-07-04): amortizes the per-query floor ~11× end-to-end
  naive (7.65ms/page at K=50 measured in isolation), byte-identical by construction,
  memory f(config) not f(N).

## The cold-path frontier (what a generic service still needs)

Measured verdicts that shape this (2026-07-04 experiments):
- The DuckDB `read_parquet` cold floor (~27ms triage box) is **fixed per-query scan
  machinery, not decode volume** — the row-group sweep shows a ~6k-row window costs the
  same as a ~1M-row window. Nothing routed through DuckDB gets cold reads much below that
  floor on the current engine version (1.5.4); DuckDB provably ignores the Parquet page
  index for this query shape (P1).
- Therefore the cold-read lever is **not** SQL-side. **P4 (measured 2026-07-04) validates
  it**: parquet-java ColumnIndex-filtered page reads (`readNextFilteredRowGroup` +
  row-range filter) over **unmodified production files** (8MB row groups, 1MB pages)
  serve a cold ~1000-row lookup in **5.3ms key-only / 12.1ms full projection** (reader
  open included — 0.3–0.7ms, so even open-per-lookup works). Binary filter comparison
  verified unsigned-lexicographic end-to-end (0xFF-key probe) — no §0.3/I10 hazard on
  parquet-java 1.15.1.
- **The page-size tension resolved itself:** 64KB pages are worse for BOTH engines —
  DuckDB never consumes the page index (P1: +3.8% size, +35–40% wall, pure loss), and
  even the filtered custom reader is 9–10× slower on them (P4: index-parse/RowRanges
  cost scales with page count and swamps the decode-volume win). The production format
  is already optimal for both; **no format change is needed for the ~5ms cold tier.**
- **Embedded footer routing blob** (planned follow-up) stays load-bearing in this
  framing: a generic service opens *arbitrary* fixture files ad-hoc; the routing table in
  the footer makes cold-open a footer read instead of a derive pass.
- Production caveat for the reader (JDK 25): parquet-java's Hadoop-path constructors
  (the footer-reuse variant) throw via `UserGroupInformation` (JEP 486 removed
  `Subject.getSubject`); a real reader stays on the `LocalInputFile` constructor path.

## Architecture sketch (if pursued)

```text
sorted stamped parquet (+ footer routing blob)
        │
        ├── point/range/page serving: routing index → page-filtered custom reader (cold ~ms)
        │        └── windowed prefetch on top for sequential walks (sub-ms amortized)
        ├── arbitrary SQL / analytics: DuckDB read_parquet over the same files
        └── S3-protocol facade (today's ListObjectsV2 replay surface) as ONE client/mode
```

## Future research questions (owner, 2026-07-04 — parked, not scoped)

**1. S3-resident fixtures — serve straight from object storage?** Platform framing:
thousands of buckets' listings, infrequent reads, ~100ms latency tolerance. The design is
already shaped for it: every query reads only footer-metadata + a ~1–2-unit window, which
maps to ranged GETs — with per-bucket routing metadata cached (footer blob ≈ KBs/bucket ⇒
thousands of buckets ≈ MBs of cache), a page read ≈ ONE ranged GET + ms of decode ≈ S3
TTFB (~20–60ms same-region) + ε, comfortably inside a 100ms budget with S3 as the only
storage tier. Cold-open of an uncached bucket adds 1–2 metadata GETs (the embedded footer
routing blob becomes load-bearing: cold-open = 1 RTT, no derive). The P4 page-filtered
reader is the natural executor (already byte-range-oriented via OffsetIndex). Honest
corollary: at a 100ms tolerance the sweet spot may even re-admit DuckDB-over-httpfs (its
~27ms floor stops mattering when RTT dominates) — the engine choice is a function of
(latency tolerance × read frequency × corpus count), i.e. the role-tier idea generalizes
to a remote tier.

**2. Iceberg?** Likely management plane, not data plane. Buys: catalog, snapshots /
time-travel (genuinely attractive: time-travel across inventory snapshots of a bucket),
incremental appends/compaction, ecosystem queryability; can wrap our sorted parquet
(Iceberg has sort-order metadata). Does NOT buy read latency: its metadata chain
(catalog → metadata.json → manifest list → manifest → file) adds RTTs, and below
file-level min/max there is no sparse key index — within-file serving is parquet-level
machinery either way. Research question: does Iceberg's stack earn its complexity at
thousands-of-buckets scale vs a trivial manifest-of-fixtures, given the fast path stays
ours regardless?

## Explicitly out of scope of this note

Naming decision, module split, protocol surface beyond S3-replay, multi-tenant/service
concerns, and any commitment of the current campaign to this direction. This note exists
so the framing and its measured foundations are not lost.
