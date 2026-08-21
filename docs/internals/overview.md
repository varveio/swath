# Internals overview

For an accessible, visual explanation of the problem and algorithm, read the
[visual field guide](https://swath.varve.io/field-guide/). This page is the technical
bridge from that explanation to the repository's implementation.

Read [architecture](architecture.md) next for the code map and run lifecycle, then use
[contracts](contracts.md) and [algorithms](algorithms.md) when changing the engine.

## The problem

An S3 bucket is one globally ordered sequence of byte keys. `ListObjectsV2` can start
after a key, but it cannot ask the service to divide an unknown bucket into balanced
partitions. A single pagination chain is therefore correct but serial. Static prefix
partitioning can be parallel, but only if the bucket happens to use the chosen prefixes
evenly; it performs badly on flat, skewed, or deeply nested layouts.

swath treats the ordered keyspace as adjacent half-open ranges `(A, B]`. A worker asks
for keys after `A`, emits through `B`, and stops. Ranges can be split as the run learns
where keys actually exist, so idle workers take part of a busy worker's remaining span.

The name describes the invariant: adjacent swaths cover the field without gaps or
overlap.

## Scope

The shipped product is a Java 25 CLI for general-purpose S3 buckets. It supports:

- recursive current-object listing;
- current objects; the output schema can also represent delete markers and versioned rows;
- JSONL, TSV, table, and managed Parquet output;
- filtering by key, size, modification time, and storage class;
- checkpoint/resume for managed Parquet datasets (the CLI's directory output);
- opt-in globally sorted Parquet output;
- OTLP metrics, JSON run reports, and decision traces.

The range engine assumes bytewise global ordering and a `StartAfter`-equivalent exclusive
lower bound. Internal store interfaces record that requirement, but they are not a stable
third-party SPI. Only S3 ships today. Directory buckets, versioned listing, remote output,
and sorted text output remain outside the supported path; see the [roadmap](../../ROADMAP.md).

## Design laws

### Preserve the keyspace partition

The live and completed ranges must always be pairwise disjoint and together cover the
requested scope. In `(A, B]`, the boundary `B` belongs to the left range; the next range
starts *after* `B`. A split changes one range `(A, H]` into `(A, M]` and `(M, H]` in a
single guarded transaction.

This convention, raw-byte comparison, and the split CAS are correctness rules rather
than implementation details. Their exact statements are invariants I1–I12 in
[contracts §0](contracts.md#0-load-bearing-invariants).

### Learn from the listing instead of classifying the bucket up front

Bucket shape is not a durable category. A run can encounter flat names, deep prefixes,
large empty lexical gaps, and a dense tail in one listing. swath therefore combines two
sources of parallelism:

1. A shallow seed pass discovers useful prefix boundaries before workers start.
2. Demand-driven stealing splits whichever live range currently holds the most estimated
   remaining work.

The engine uses observations already paid for by listing—page density, key bytes,
structure probes, and progress—to choose pivots. It does not lock the whole run into a
single guessed bucket type. Every decision path emits a bounded engagement reason so a
report can later show which mechanisms actually ran.

## The seed-and-steal model

```text
ordered keys
    │
    ├─ shallow delimiter probes ──► initial disjoint ranges
    │                                      │
    │                                workers paginate
    │                                      │
    └──────── idle worker ◄── split busy remaining range
```

The seed step is deliberately bounded. Common prefixes become cut points, capped relative
to worker count. A flat bucket may still begin as one range; stealing then creates
parallelism from observed keys. On a hierarchical bucket, seeding avoids a near-serial
warm-up. On a skewed bucket, stealing corrects the seed's imbalance.

An idle worker first chooses the victim with the largest estimated remainder. It proposes
a byte-safe pivot, verifies that useful keys exist above it, and may fall back through
bounded structure and bisection probes. The victim continues below the pivot while the new
child owns the upper range. Owners can also carve work proactively when the demand and
density gates say a useful child is available.

Probe budgets and terminal cases matter: an empty lexical region must not become an
unbounded API search, and a genuinely adjacent cursor/end pair is unsplittable. The exact
pivot ladder and budgets live in [algorithms §3](algorithms.md#3-stealing--demand-driven-rebalancing)
and [probe budgets](probe-budgets.md).

## Durability model

The checkpoint table is also the worklist. Nodes move from `PENDING` to `IN_PROGRESS` to
`COMPLETED`; one SQLite writer serializes mutations in WAL mode. A page cursor commits
before its rows enter the output pipeline. This keeps the range partition crash-safe but
means a one-shot stdout/text process can stop after commit and before emission, so those
sinks are intentionally not resumable.

Managed Parquet has a second cursor. `cursor` tracks listing progress; `durable_cursor`
advances only after a part footer is written and fsynced. Resume discards an unfinished
part and re-lists from the durable cursor, producing an exactly-once published dataset.
Sorted output stages bounded runs and publishes only after the merge succeeds.

The per-sink guarantees and schemas are canonical in
[contracts §§3–6](contracts.md#3-sqlite-checkpoint-schema).

## Output and backpressure

Listing pages cross a bounded channel into the selected sink. Text output formats and
writes on the consumer stage. Direct Parquet uses a small fixed writer pool and size-based
part rotation. `--sort` packs bounded staging segments, then performs an external merge
into globally ordered Parquet parts.

Backpressure is intentional: a slow filesystem or full writer queue eventually blocks
the producer rather than growing memory with object count. Active row, page, and merge
buffers are bounded by configuration. Finalized-part and staging-segment metadata scale
with the number of files/segments and are accounted separately.

## Prior art

The range-partition idea is related to parallel ordered scans and adaptive work stealing.
s5cmd and rclone demonstrate the value of high-concurrency object-store tooling; S3P
explores overlapping parallel listing. swath's distinguishing constraint is a disjoint,
crash-safe range partition with adaptive splits, so correctness does not depend on
deduplicating overlapping scans.

All names and implementations in this repository remain subject to the repository's
license; references to other projects describe ideas, not copied code.
