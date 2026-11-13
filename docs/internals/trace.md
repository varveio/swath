# Run trace

`swath diag list --trace FILE` (and `swath diag count --trace FILE`) writes the range engine's run trace: every
range the engine made, claimed, paged, split and ended, one JSON object per line. The counters
([counters.md](counters.md)) say how often each path engaged; the trace says where in the keyspace and when.
[`tools/explainer`](../../tools/explainer/README.md) turns a trace into a report and a video.

It covers the sources on the range engine ([range-engine.md](range-engine.md)): S3, GCS and Azure Blob. A local
tree lists through its own engines ([providers/local.md](providers/local.md)), which keep no ranges; `--trace`
with a local source is refused (exit 2). A `--sequential` run lists one continuation-token chain, not ranges:
its trace holds `run_start` and `run_end` only.

## Writing it

The engine encodes each event on the goroutine of the range it describes, outside the decision mutex, and hands
it to the trace's writer goroutine through a buffer of 4,096 events (`internal/runtrace`). When the buffer is
full the event is dropped and counted, never waited for: a slow disk can lose events, it cannot slow the
listing. A run without `--trace` pays a nil check at each event site. The writer flushes whenever no event
waits, so a killed run leaves the trace up to its last quiet moment, at most with a torn last line.

The drops are in the trace's `run_end` and in the run report's source counters, `trace.events` (handed to the
writer) and `trace.dropped` (feature `trace`, off without `--trace`). A trace with drops is incomplete: its
ranges' lifecycles need not balance.

FILE is checked as `--report` is, before the source is opened ([side paths](run-lifecycle.md#side-paths)), and,
like a profile, is never written through a symbolic link. It is replaced, not appended to. `swath resume` of a
traced run traces again, to the recorded FILE unless `--trace` names another.

A trace holds keys, seed cuts and pivots verbatim: it is as sensitive as the listing itself.

## Schema, version 1

Every line has `event` and `ts_ns`, the nanoseconds since the trace was opened (a monotonic clock). Events of
one range are written in the order they happened; events of concurrent ranges may be a few microseconds out of
`ts_ns` order, so a reader sorts by `ts_ns` (stably: the file order of a range's own events is causal). A reader
ignores event kinds and fields it does not know, and tolerates a torn last line.

**Keys.** Every key and bound (`lo`, `hi`, `after`, `last`, `cursor`, `pivot`, `prefix`, `resume_after`) is a
JSON string when it is valid UTF-8, the key itself; a key that is not UTF-8 (a store may hold one) is an object
`{"hex": "…"}` holding its bytes in hexadecimal, so every byte survives. Keys compare as bytes (UTF-8 for a
string). An absent `lo` is never written: `lo` `""` is ⊥, the start of the scope.

**Ranges.** A range is `(lo, hi]`, keys above `lo` up to and including `hi`; `open: true` in place of `hi` is
the frontier, `(lo, ⊤]`. `after`, on a resume, is the resume key: keys up to it are already published and
dropped. `id` names a range for the run (the run's root, listed without a seed, is `0`). `worker` is the lane a
range ran in: the lowest index no other range in flight held when it was claimed (the engine does not see the
order stage's goroutines; at most `--workers` plus one, the drainer, listing a range itself).

| Event | When | Fields |
|---|---|---|
| `run_start` | the trace opens, before the first request | `trace_version` (1), `tool`, `version`, `revision` (absent when unknown), `time` (RFC 3339, UTC, the wall clock at `ts_ns` 0), `prefix` (the scope), `workers`, `page_size`, `sequential`, `seed` (the delimiter seed), `prior` (`--seed-from`), `structure` (`auto`, `on` or `off`), `slow_split`, `versions` (`--all-versions`), `resume_after` (on a resume) |
| `seed` | the delimiter seed has tiled the scope | `dur_ns`, `requests`, `round_trips`, `levels` (descended below the top), `cuts`, `stop` (why it stopped: the `seed.stop.*` counters' names, [counters.md](counters.md)), `keys` (rows its first data page emitted when that page held the whole scope, `stop` `one_page`: then no range follows) |
| `seeded` | a range made by a tiling: the seed's tiles, `--seed-from`'s, a resume's, or the root | `id`, `lo`, `hi` or `open`, `after`; `expected` (a counted prior's keys in it); `reused: true` for a resume's reused span, which the order stage replays and no worker claims |
| `claimed` | a worker starts listing a range | `id`, `worker`, `lo`, `hi` or `open`, `after`, `depth` (splits since its tile), `born` (`root`, `seed`, `lower` or `upper`) |
| `page` | a listing page of a range is handed on | `id`, `worker`, `after` (the range's cursor before the page: its `lo`, then the previous page's `last`), `last` (the cursor after it), `rows` (rows emitted; with versions, rows rather than keys), `dur_ns` (the request, retries, requeues and empty continuing pages included), `done` (the page ended the range) |
| `split` | a range split at its cursor | `id`, `worker`, `cursor`, `pivot`, `hi` or `open` (the range's), `lower` and `upper` (the children's ids: `(cursor, pivot]` and `(pivot, hi]`), `mechanism` (`reflect`, `midpoint`, `density` or `structure`: [range-engine.md](range-engine.md)), `reason` (`demand`: idle workers; `slow`: a latency outlier, `--slow-split`), `hunger` (idle workers, the demand), `rows` (the range listed), `depth` (the children's), `gen` (the range's gallop generation); `remainder` (the estimate of `(cursor, hi]`, when the page gave one), `est_lower`, `est_upper` (the children's estimates), `held` (they share a prior's count), `persistent: true` (admitted after refused admissions) |
| `completed` | a range listed through its bound | `id`, `worker`, `rows` |
| `failed` | a range stopped by an error, cancellation included | `id`, `worker`, `rows`, `error` |
| `run_end` | the trace closes, after the run | `status` (the report's `outcome.status`), `events` (written before it), `dropped` |

A split ends its range: the range listed `(lo, cursor]`, and its children list the rest. So in a complete trace
of a run that ended well (`run_end` with `dropped` 0), every range made (`seeded` and not `reused`, or a split's
`lower` and `upper`) is claimed once and ends once, by `completed` or `split`, and

```text
seeded + 2 × split = claimed = completed + split
```

A range's pages chain: the first starts after its `lo`, each after the previous one's `last`, and a split's
`cursor` is its last page's `last`. The intervals the ranges listed, `(lo, hi]` for a completed range and
`(lo, cursor]` for a split one (with a `seed` of `stop` `one_page`: the whole scope), tile the scope with no gap
and no overlap, and the pages' `rows` sum to the rows listed. `TestTraceInvariants`
(`internal/source/keyrange`) checks all of this from the trace alone, over the fixture keyspaces, seeded and
not, under the demand gate and under random splits.

A trace with drops, or of a run that failed or was stopped, may hold ranges that were made and never claimed,
claimed and never ended, and pages whose range's claim was dropped.

A change that removes or renames an event or a field, or changes a meaning, raises `trace_version`; adding one
does not.
