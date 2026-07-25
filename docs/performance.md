# swath performance

This page defines the measurements required before swath makes quantitative
performance or scale claims: scaling, memory, resume cost, throughput, and
known slow paths. There is no published release-candidate measurement bundle
yet, so the sections below are test plans and design targets, not results.

For a head-to-head against other S3 listing tools, see the
[S3-listing comparison study](https://github.com/varveio/s3-listing-study),
which is built for exactly that and commits to what it measures before it
measures it. Its comparative runs have not started yet, so it carries a
[methodology](https://github.com/varveio/s3-listing-study/blob/main/docs/methodology.md)
and a tool roster but no results so far.

Every figure below is pending a release-candidate measurement pass; the numbers
are not filled in yet.

## Scaling behavior

How listing time and LIST-call count grow as object count rises across bucket
shapes (deep prefix trees, flat key spaces, skewed distributions).

> _Numbers pending RC measurement._

## Memory behavior and current evidence

The implementation bounds active page, queue, writer, and merge buffers with
configuration. That is not a constant-memory claim for the complete process:
unsorted Parquet retains `O(parts)` finalized-part metadata, rewrites an
`O(parts)` manifest on each finalize (cumulative `O(parts²)` serialization),
and sorted output retains `O(segments)` staging metadata. Larger parts and sort
segments reduce those counts.

The current public PERF-2 gate covers 100,000 keys and asserts peak heap below
1 GB for default Parquet settings. It does not establish the same peak at
million- or billion-object scale, and there is not yet a documented, enforced
maximum part/segment-count envelope. Publish a stamped larger-scale measurement
and an operating envelope before making a stronger bounded-memory claim.

> _Numbers pending RC measurement._

## Resume cost

The overhead of resuming an interrupted run: time to reopen the checkpoint and
the LIST calls a resume spends versus a run that completes in one pass.

> _Numbers pending RC measurement._

## Throughput

Sustained keys listed per second, and the LIST-request rate that throughput
implies, under representative concurrency settings.

> _Numbers pending RC measurement._

## Where swath is slow (honest limits)

The bucket shapes and workloads where swath does not shine — where throttling,
probe overhead, or an adversarial distribution costs more than the ideal.

> _Numbers pending RC measurement._

## Methodology

The evidence bar for every figure on this page: each number is stamped with the
swath version, the measurement date, the machine it ran on, and the bucket it
listed. Measurements come from publicly available tooling only. We publish no
leaderboard and make no self-favorable comparative framing — the numbers
describe swath, not rivals.

> _Numbers pending RC measurement._
