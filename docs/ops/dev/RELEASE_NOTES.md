# swath 0.2.3

## User-facing changes

- Sorted listings large enough to engage the parallel merge no longer abort with
  `error_class=stuck` (exit `75`) partway through their merge phase. Two scan phases that
  run only on the parallel path — the boundary-sampling pass over every staged segment,
  and each range's prefix walk to its first overlapping page — performed real work without
  advancing the liveness progress signal, so the watchdog's total-freeze tripwire (default
  `--idle-timeout`, 120s) halted healthy runs. Both phases now advance the signal per page.
  No output, ordering, or completeness behaviour changes; runs that previously completed
  are unaffected.
- A parallel range merge that is cancelled — by the liveness watchdog's cooperative rung or
  by a sibling range failing — now stops promptly instead of running its prefix walk to
  completion first. Two independent causes: the watchdog's interrupt filter did not match
  the merge's own `swath-sort-range-*` threads, and the prefix walk polled no cancellation
  point. A consequence is that a genuine, classified merge failure is now reported as
  itself rather than being overtaken by the stall tripwire and re-reported as
  `stuck_unknown`.

## Evidence

- The halt is measured, not inferred. Two ~1.9-billion-object public buckets that failed on
  every prior attempt under 0.2.2 (`its-live-data`, `usgs-lidar-public`) both completed on
  the fix, sorted, at 16 vCPU with 8 merge ranges. Their `sort.merge_boundaries_ms` — the
  previously silent boundary-sampling phase — was **136,483 ms and 157,039 ms**, against a
  120,000 ms stall window. That is the failure, quantified: the phase crossed the window
  and reported nothing while it did. It also explains the observed size threshold, since a
  1.04-billion-object bucket's shorter sampling pass fit inside the window and completed
  under 0.2.2.
- The phase is fast, not slow: ~2.5 minutes at 1.9 billion objects over 45-54 GiB of
  staging. This release restores visibility to a healthy phase rather than accelerating a
  pathological one.
- `RunSortMetricsTest` pins the wiring the defect actually hid in. `SortMetrics` is a
  `@FunctionalInterface` and the pipeline bound it with a method reference, which can only
  ever supply one method and silently inherits the no-op default for any other — so a newly
  added hook reaches nothing, with no compile error. The named bridge is now asserted to
  forward every hook, and the assertions were each verified to fail when the delegate is
  broken.
- `LivenessWatchdogTest` pins the interrupt filter against the exact thread-name shape
  `ParallelRangeMerge` produces.
- CI's fast, Docker and CodeQL tiers were green on the merged SHA, alongside independent
  review of the cancellation semantics — specifically that throwing from the frontier
  constructor's prefix walk cannot leak a stream or expose a half-constructed frontier, and
  that interrupting a range mid-write can damage only an unpublished `.tmp` part, since
  parts are renamed into the output directory only after every range completes and closes.

## Limits and known issues

- The live validation exercised the progress-tick fix. The cancellation changes are covered
  by unit tests and review, not by a billion-object run — they govern how a trip unwinds,
  not whether one occurs.
- The parallel merge holds every output part open for the duration of the merge (a
  deliberate deferred-footer design), while the file-descriptor reservation accounts for
  the range count rather than the eventual part count. A merge producing many parts can
  therefore exhaust the descriptor allowance that the pre-merge clamp declared safe, well
  into a long run. Not addressed in this release: sizing it correctly means predicting part
  count from staged bytes before the merge begins, where an over-estimate needlessly clamps
  parallelism and an under-estimate strands the merge.
- `--idle-timeout` remains 120s by default. Operators running billion-scale sorted listings
  should still size it against their own staging, since any single unticked stride — a
  large fsync, a slow finalize — can still cross it.
