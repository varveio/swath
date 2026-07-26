# Decision-trace goldens — the policy-seam safety net

This is the operational doc for the decision-trace golden recorder
(`swath-core/src/test/java/io/varve/swath/engine/{GoldenTrace,RecordingTraceSink,
DecisionTraceGoldenTest}.java`, goldens under
`swath-core/src/test/resources/goldens/decision-trace/`). See
[`TESTING.md`](TESTING.md) for the wider test-tiering picture; the seam principle
this recorder exists to check ("decision logic = deterministic state machines over
observed events; engine and simulator share the former") lives in the `swath-notes`
policy-seam campaign, not in this repo.

## What it is

Before any policy logic is extracted out of `Thief`, `OwnerSelfSplit`, `WorkerState`
(pacing), or `SeedStep` into a standalone, simulator-shareable form, we need a
mechanical way to prove "behavior-preserving" instead of asserting it. The recorder
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
- **`pacing.steal_paced`** — `WorkerState.stealPaced()`'s per-victim cooldown state
  machine, driven directly (a pure state machine, no I/O).
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

```
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

## Coverage matrix

Every decision site appears in at least 3 fixtures (see `DecisionTraceGoldenTest`'s
class-level Javadoc for the exact, current matrix — kept there so it can't drift from
the code that produces it).
