# Decision-trace goldens — the policy-seam safety net

This is the operational doc for the decision-trace golden recorder
(`swath-core/src/test/java/io/varve/swath/engine/{GoldenTrace,RecordingTraceSink,
DecisionTraceGoldenTest}.java`, goldens under
`swath-core/src/test/resources/goldens/decision-trace/`). See
[`TESTING.md`](TESTING.md) for the wider test-tiering picture. The seam principle
this recorder exists to check — decision logic is a deterministic state machine over
observed events, and execution is everything touching time, threads, sockets, and
SQLite — is stated in [`contracts.md`](../../internals/contracts.md) §2.1.

## What it is

All four decision sites — `Thief`, `OwnerSelfSplit`, `WorkerState`/`IdleStealBackoff`
(pacing), and `SeedStep` — have now landed their policy-logic extraction into
`io.varve.swath.engine.policy` (contracts.md §2.1). This recorder is the mechanical
proof that each extraction was "behavior-preserving", not merely an assertion of it: it
drives each of those four **decision sites** with a deterministic, single-thief/
single-victim harness (never a storm — see `docs/ops/dev/TESTING.md`'s tag-convention
section and issue #18 for why hard engagement asserts on schedule-dependent storms
are banned in this repo) and records one JSON object per decision:

- **`thief.steal`** — the pool of candidate victims (the *view*), the ordered probe
  request/response log the attempt issued, the `RunMetrics` engagement-counter deltas
  (which named branch of the pivot cascade fired — plain midpoint, far-ahead,
  step-back, structure probe, reflect, bisect, flat-leaf), any `TraceSink` events, and
  the final outcome (the *decision*: `CHILD_CREATED`/`RETRY`/`UNSPLITTABLE`/`NO_VICTIM`,
  plus the committed pivot bytes when one exists).
- **`owner_self_split`** — the victim's density view, the same counter-delta/trace
  capture, and whether a carve published (with its pivot bytes) or which gate
  suppressed it.
- **`pacing.steal_paced`** — the per-victim futility cooldown, driven directly as a pure
  state machine (no I/O). This fixture drives `WorkerState.stealPaced()` — which is
  **not** the live pair (an independent review's finding, issue #26's widening lives on
  this exact surface): production reads `WorkerState.pacingSkipAvailable()` at
  view-construction time and applies `consumePacingSkip()` afterward, split across the
  policy-executor seam (contracts.md §2.1); `stealPaced()` itself has **zero**
  production callers (verified: only its declaration and javadoc remain in `src/main`)
  and is kept only because this fixture drives it — see its own corrected javadoc.
  There is currently **no golden on the live `pacingSkipAvailable()`/
  `consumePacingSkip()` pair**, and **no golden at all** on the fleet-wide
  `IdleStealPacingPolicy` (the other half of pacing, driving `IdleStealBackoff`) — see
  the known gaps below; the "four decision sites" framing above should not be read as
  "pacing is covered".
- **`seed.seed_specs`** — the probe log, the per-probed-level classification trace
  (`RunSummary.SeedSummary.decisions()` — narrow/flat_wide/partition/explosion/
  heavy-cut-banded), and the final tiled range set.

Every fixture is a JSONL golden file: one file per bucket-shape/edge-case scenario,
one line per decision recorded during that scenario's replay.

## Zero production behavior change

The recorder adds **no production hook**. Every seam it uses already exists for
tests:

- `MockPageFetcher`'s `PageInterceptor` (already used by dozens of tests) — logs every
  probe request/response a decision-site call issues, in call order.
- `RunMetrics#diagnostics`/`RunMetrics#summary` — reads back the *already-instrumented*
  `recordStealReason`/`recordSeedSummary` engagement counters (AGENTS.md's "instrument
  every new algo path" law), before/after a call, as a category/reason delta map.
  Nothing new is instrumented; the recorder only reads what production code already
  emits for post-hoc analysis.
- `io.varve.swath.observability.TraceSink` — a production interface (the `--trace`
  flight recorder seam) with a `NONE` no-op default. `RecordingTraceSink` is an
  ordinary **test double** implementing it (the same shape as any other test's stub),
  not a new hook: it defaults `enabled()` to `true` so a caller that gates its return
  value on the sink being enabled (`OwnerSelfSplit#maybeOwnerSelfSplit`'s pivot-bytes
  trace object) always gets the payload.

`Thief`, `OwnerSelfSplit`, `WorkerState`, and `SeedStep` are driven exactly as
`ThiefTest`/`OwnerSelfSplitTriggerTest`/`SeedStepTest` already do — direct
construction against `StubCheckpointStore`/`MockPageFetcher`/`WorkerStates`, never the
full engine, never multiple threads.

## Golden JSON format

One JSON object per line (JSONL), Jackson `ObjectNode` with a fixed field order per
event kind (insertion order — never re-sorted). Every byte-valued key field
(`lo`/`cursor`/`hi`/`pivot`/`prefix`/probe `start_after`/…) is **hex-encoded**
(`HexFormat.of().formatHex(...)`), never UTF-8-escaped — unlike the production
`--trace` sink (`JsonlTraceSink`), which escapes for human readability, this golden
format needs byte-exactness a lossy escape can't guarantee (arbitrary/non-UTF-8 keys,
algorithms.md §11 edge case 1). `null` (⊥ / the open frontier) renders as a JSON
`null`.

Every event shares the envelope `{"site", "fixture", "seq", ...}`; the rest of the
object is site-specific (`view`, `probes`, `reason_deltas`, `trace_events`,
`decision`, and for `seed.seed_specs` a `levels` per-probed-level classification
array). See `GoldenTrace.java` and `DecisionTraceGoldenTest.java` for the exact
per-site shapes — they are the source of truth, not this doc.

## Regenerating goldens

After a **deliberate, reviewed** change to one of the four decision sites (including
the sanctioned seam extractions this safety net exists for: the thief-brain, the
owner-split-governor, the pacing, and the seed-planner slices):

```shell
./gradlew :swath-core:test --tests 'io.varve.swath.engine.DecisionTraceGoldenTest' \
    -Dswath.goldens.update=true
```

This rewrites every fixture under `swath-core/src/test/resources/goldens/
decision-trace/`. **Always review the diff before committing** — a drift here means
either the change was intentional (review confirms it, commit the new goldens in the
same commit as the code change) or it caught an unintended behavior change (the whole
point of this safety net).

Mirrors `swath-cli`'s `HelpUsageGoldenTest` update ergonomics
(`-Dswath.goldens.update=true`) — same property name, same pattern, reused rather
than inventing a second convention.

## Determinism requirement

Two consecutive recording runs must produce **byte-identical** files: no wall-clock
reads, no thread ids, no iteration over unordered collections, no hash-order-dependent
JSON key ordering, no unseeded randomness. This is enforced by construction, not by a
test:

- Single-threaded drivers only (no `WorkStealingScan`, no virtual-thread pool).
- `RecordingTraceSink`/`GoldenTrace.ProbeLog` are plain `ArrayList`s appended to in
  call order — no concurrent structures, no need for one.
- `GoldenTrace.reasonDeltas` returns a `TreeMap` (key-sorted), never the raw
  `Map` `RunMetrics#diagnostics` hands back (whose iteration order is
  implementation-defined).
- Jackson `ObjectNode` preserves child-insertion order on serialization (the same
  property `JsonlTraceSink` already relies on), and every event builder here inserts
  fields in a fixed, hand-written order.
- `Keyspaces.*` fixture generators are seeded (`java.util.Random(seed)`).

Verified: two back-to-back `-Dswath.goldens.update=true` runs produce byte-identical
output (`diff -r` empty) — re-run this check after any change to the recorder itself.

## Verifier

`DecisionTraceGoldenTest` (no `-Dswath.goldens.update`) replays every fixture and
`assertThat(actual).containsExactlyElementsOf(golden)` — AssertJ's list diff reports
exactly which line(s) drifted and how, not just "not equal". Runs as part of the
default `:swath-core:test` tier (no `@Tag`) — every commit.

## Known gaps

- **No real-bucket replay fixtures.** The fixture matrix is entirely synthetic
  (`Keyspaces.*` generators, plus a few hand-built keyspaces in
  `DecisionTraceGoldenTest` itself). Real-bucket fixtures served through
  `:swath-replay-server` would be a strong addition, but `swath-replay-server`
  *depends on* `swath-core` — reaching it from `swath-core`'s own test source set
  would need a dependency cycle, so it cannot be wired in from here as this module
  is laid out today. If this gap is worth closing, the goldens (or a
  replay-server-backed variant of them) belong in a module that can depend on
  both, or the replay-server fixture data would need to move somewhere
  `swath-core` can reach without inverting the dependency graph. This is a
  standing limitation, not a deferral with a planned follow-up.
- **Synthetic generators cannot reproduce real irregular key distributions.**
  Even setting the module-cycle problem aside, `Keyspaces.*`'s generators are
  parametric (uniform-within-shape, seeded PRNGs) — they cannot reproduce the
  genuinely irregular, heavy-tailed, structurally inconsistent layouts real
  buckets exhibit (mixed conventions from different upload tools, partially
  migrated layouts, one-off directories). The synthetic fixtures here catch
  extraction bugs in the *named* branches and edge cases; they cannot catch a
  bug that only a real bucket's specific irregularity would trigger. Real
  production traffic (the public-bucket differential at the release gates,
  and any `--trace` captures from real runs) remains the backstop this golden
  suite does not replace.
- **The `flat-wide` fixture no longer pins the long-zero-padded-id flat shape.**
  It moved from `Keyspaces.singlePrefixFlat`'s 8-digit zero-padded counter to a
  local 3-digit one proportionate to its 400-key scale (see `flatWideKeys`'s
  javadoc): at 400 keys the 8-digit format left the flat-leaf density
  reflection extrapolating over a window ~250,000× larger than the real data,
  so every round landed within one code point of the cursor — nominal, not
  real, cascade coverage. The 3-digit swap was the right call for THIS
  fixture's purpose (`Keyspaces.singlePrefixFlat`'s 8-digit padding exists for
  a different, deliberately adversarial test elsewhere), but it means the
  corpus no longer has any golden pinning a "long zero-padded id, sparse
  relative to its own digit width" flat shape — a real pattern in migrated or
  legacy-tool-generated datasets — and nothing else in this fixture matrix
  covers it. Disclosed here rather than silently dropped; not planned to be
  closed as part of this safety net.
- **`OWNER_SPLIT.self_aborted` is out of this recorder's reach.** Every scenario's
  `StubCheckpointStore` always accepts the split, so the abort path never triggers
  here — it is covered instead by `OwnerSelfSplitContractTest`'s dedicated
  abort-path test (T2).
- **Issue #25 is resolved: the thief `poolView` now records the full `StealAttemptView`
  shape.** An independent review had found `poolView` recorded only the pool-selection
  fields (`node_id`/`lo`/`cursor`/`hi`/`unsplittable`, later widened to also carry
  `keysEmitted`/`pacingSkipAvailable`), while `StealAttemptView` additionally carries
  per-victim state the cascade genuinely branches on — `densityFraction`, the
  alphabet-digest state, the `unchangedSinceNonProductiveSteal` flag, and both
  structure-probe streaks. Issue #25 offered two ways to settle this permanently
  (record the full view, or document the subset as a deliberate scope boundary); the
  owner decision (2026-07-26) took option 1. `poolView` now computes every one of
  those fields for **every** candidate (not just the one `selectVictim` eventually
  picks — which candidate wins is itself part of the decision under test, so recording
  only the winner's per-attempt state would make the view's own shape depend on the
  decision it exists to verify), and `AlphabetDigest.Snapshot` gained a small,
  package-private, test-only `base()`/`cleanBits()`/`wordsHex()` accessor set (hex,
  never handing out its backing array) so its state can be serialized at all. A
  `thief.steal` golden's decision is now verifiable from its own recorded view alone —
  regenerated with zero decision-byte drift (46 events across the 6 `thief.steal`
  fixtures gained the new fields; 0 decisions changed anywhere in the corpus).
- **The seed slice's per-branch discipline was never applied (this campaign's own
  finding, converged on after being conflated three times).** `HybridSeedPlanner`
  fires **22** distinct `SEED.*` marks (21 via its own `mark()` calls, recounted
  directly from `HybridSeedPlanner.java`'s source — not from any prior prose in this
  file or `DecisionTraceGoldenTest`'s javadoc — plus the `radix_bands` magnitude
  counter `RunMetrics` records separately). The 4 seed fixtures' goldens pin only
  **6** of those 22, recounted from the committed JSONL:
  `delimiter_seeded`/`descent_cuts_subsampled`/`frontier_level_ordered`/
  `frontier_reordered`/`mass_weighted_subsample`/`top_complete`. The other ~16
  (`banding_deferred_to_fanout`, `dense_root_radix_banded`, `explosion_confirmed`,
  `fanout_tiled`, `flat_trivial`, `frontier_continued_past_explosion`,
  `heavy_cut_banded`, `heavy_cut_descended`, `heavy_prior_applied`,
  `heavy_prior_banded`, `heavy_prior_left_whole`, `radix_bands_toggle_disabled`,
  `top_probe_paginated`, `top_truncated`, and `radix_bands` itself) are unpinned by
  any golden — they are exercised instead by `SeedStepTest`/`SeedMassAwareDescentTest`
  and this package's other dedicated seed tests (`SeedStepFanoutTilingContractTest`,
  `SeedSampleBudgetExhaustionPriorTest`, `SeedDescentTwoHeavySiblingsBothSurviveTest`,
  and others), never by a decision-trace golden. This file previously recorded only a
  *fixture-file* count (`seed.seed_specs`: 4 fixtures) for seed coverage, which said
  nothing about per-branch mark coverage — exactly the same distinct-count-vs-mark-count
  conflation this campaign found and corrected on the thief/owner-split sides earlier;
  it had simply never been checked on the seed side until this pass.
- **`pacing.steal_paced` pins a dead method, not the live pacing pair.** See the
  `pacing.steal_paced` bullet above: the fixture drives `WorkerState.stealPaced()`,
  which has zero production callers; there is no golden on the live
  `pacingSkipAvailable()`/`consumePacingSkip()` pair issue #26's widening actually
  lives on.
- **`IdleStealPacingPolicy` (the fleet-wide half of pacing) has no golden at all.**
  Only the per-victim futility cooldown (`WorkerState`, via the dead `stealPaced()`
  method above) has any fixture; `IdleStealBackoff`'s fleet-wide pacing decision —
  the OTHER extracted policy this recorder's own "four decision sites" list implies
  is covered — has no golden anywhere in this matrix. A mutant in
  `IdleStealPacingPolicy.decide`/`onNonProductive`/`parkNanos` would evade this entire
  safety net.

## Coverage matrix

Every decision site appears in at least 3 fixtures (see `DecisionTraceGoldenTest`'s
class-level Javadoc for the exact, current matrix — kept there so it can't drift from
the code that produces it).
