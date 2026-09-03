# swath 0.3.2

## User-facing changes

- **The listing pipeline's shared channel no longer wakes every parked fetch worker per page.**
  All fetch workers hand their pages to the single output stage through one bounded channel.
  Releasing a page used to broadcast to every sender parked on the budget, so with 2,048 workers
  behind the default 50,000-entry budget the consumer paid more per page for the resulting wakeup
  storm than for the sink write itself, and every dataset, stdout and `--sort` run was capped at
  roughly 2,600 pages per second whatever the writer count (#206). The channel now relays one
  wakeup per released page and an admitted sender relays onward while budget remains; the 50 ms
  bounded wait stays as the lost-wakeup backstop. The `--object-listing-queue-size` contract,
  including admission of one over-budget page into an empty channel, is unchanged (#209).
- **Per-page row tallies move off the consumer thread.** Object, common-prefix, delete-marker and
  object-byte counts are now computed on the fetch worker that built the page and carried on the
  page; the dataset, sort and discard stages merge four numbers per page instead of walking every
  entry on the one consumer thread. The stdout formatter keeps its per-entry tally, whose
  broken-pipe truncation semantics depend on it.
- **`client_cost[]` gains a `channel_receive` span** (`swath.channel.receive.latency`): the
  consumer stage's own wait to take each page off the shared channel, the complement of `emit` on
  its timeline. `docs/performance.md` says how to read the two together.
- **Measured effect, one public bucket.** sentinel-cogs (1.069 billion objects, `us-west-2`),
  compressed TSV dataset output, `--concurrency 2048`, run from GCP `us-east1` one day apart on
  0.3.1 and on this change: listing phase 5 m 12 s → 4 m 25 s at 32 vCPU with 16 writers, and
  5 m 32 s → 3 m 11 s at 64 vCPU with 32 writers, where 0.3.1 had gained nothing from the extra
  cores. Steady-state page rates rose from about 3.4 million keys per second to 4.5 and 6.8
  million respectively.
- **Explainer and site.** The explainer report renders run provenance as a collapsed run record
  (#199); the website source now lives in this repository and deploys from CI (#196); runnable
  documentation examples are pinned to a release (#201).

## Evidence

- PR #209 carries the change with `ChannelRelaySignalTest` (senders straddling the budget all
  complete and none waits beyond the backstop; the receiver-drop and interrupt exits relay their
  wakeup), `PageTallyTest` and `PageBatchTest` (a tally whose row count disagrees with its payload
  is rejected), and the existing weighted-bound and pipeline-shutdown tests unchanged. The full
  `build` gate, including the LocalStack integration tier, ran green on the merged head.
- A standalone channel harness in the shape of the failing runs (2,048 virtual-thread senders,
  1,000-entry pages, a 50,000-entry budget, one receiver, 20 s on an 8-CPU host) moved from 9,513
  to 237,870 items per second, the receiver's per-item cost from 105 µs to 4 µs, and the worst
  single send wait from 19.9 s to 1.0 s.
- The run reports of the sentinel-cogs pairs above (kept as `_swath_summary.json` by the
  benchmark harness) show the consumer's `channel_receive` and `emit` medians at about 1 µs per
  page on this release, the AIMD permit wait falling from 531 to 26 workers on average, and the
  remaining wait moving to the dataset writer lanes.

## Limits and known issues

- The throughput figures are single runs on one live public bucket from one placement, one day
  apart; live buckets grow and S3 latency varies between runs. They are an implementation-path
  check, not a portable throughput claim.
- With the channel out of the way, the dataset dispatcher now waits on its sticky writer lane
  while other lanes idle (`head_of_line_blocked_ms` in the run report): lanes were about 60 %
  busy at 32 vCPU and 42 % at 64 vCPU. A routing change that removes that wait was measured
  slower at `--concurrency 2048` because the freed workers cost more client CPU per key than
  they return; it is not in this release. The engine's latency-inflation freeze (#202) is the
  next limiter at that ceiling.
- Everything in the 0.3.1 notes' limits — resume scope, sorted staging portability, the report
  `sort`-block carve-out, supported backends, and live-listing consistency — is unchanged.
