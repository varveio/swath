# Range engine (`internal/source/keyrange`)

The range model, seed, owner split, safe pivots, guards, demand, the probes, retry-and-requeue and their
counters, shared by the providers whose listing returns keys in unsigned byte order and can start after any
key (`Capabilities.RangeEngine()`). `Engine[P]` implements `source.Provider[Range, P]`; a provider supplies:
- a page fetch, `Fetch[P]`: one request (`Query`: prefix, delimiter, `After`, `Through`/`Bounded`, token,
  page size, `Probe` for requests whose objects are not emitted) to one `Page[P]` (rows in listing order,
  common prefixes, truncated, token). A continuation request repeats every field of its chain's first
  request and adds the token; the provider maps them onto its wire (S3 sends start-after on the first
  request only);
- a `Description[P]`:
  - *bound inclusivity:* the engine's ranges are `(lo, hi]`. `After` means keys strictly above it; a
    provider whose start bound is inclusive sends it as is and sets `StartInclusive`, and the engine drops
    a key equal to `After` from the first page of a request chain, and with a delimiter every common prefix
    at or below `After`: GCS returns `a/` for `startOffset` `a/` or `a/b`, S3's `start-after` returns
    neither (both measured on the live services; `TestInclusiveBoundDropsCoveredPrefixes`). `Through` is the inclusive end; a
    provider may ignore it (the engine trims) or bound the request with `ExclusiveEnd(Through, accept)`,
    the immediate successor (`Through` + NUL), when its own predicate accepts that string (a service may
    refuse NUL, or 1,025 bytes); otherwise it sends no end bound and the engine trims;
  - *bound admissibility:* the engine needs only valid UTF-8 bounds (it compares bytes); which strings a
    provider accepts as a start bound is its `AcceptBound` predicate, by default the pivot alphabet
    (`Safe`: valid UTF-8 without C0, DEL, C1, `%`, surrogates or non-characters). S3 declares `SafeKey`
    (`s3.AcceptBound`): the pivot alphabet plus `%`. `%` is out of the alphabet because an SDK that decodes a
    verbatim-echo endpoint's `StartAfter` crashes on it (I12); this client percent-encodes the query and parses
    no echo, so a real key holding `%` (a cursor, a folder) is a usable `start-after`. Under `Safe` a bucket
    whose every key holds `%` (URL-encoded names) had no usable cursor and listed on one worker: 107.9 s
    against 8.6 s on 2M keys at 50 ms ([measurements](../measurements.md#red-team-fixes-2026-11-07)).
    Synthesized scalars always come from the pivot alphabet: a pivot is a prefix copied from one of its input
    keys (cursor, bound or anchor), up to the scalar it changes or appends, plus safe scalars, so it never
    introduces a `%` its inputs do not hold at that position (`FuzzPropose` checks it). The
    predicate guards every bound the engine sends: pivots (guard clause `unsafe_pivot`), cursors
    (`unsafe_cursor`), seed and structure boundaries and a resume's start (K's longest accepted prefix);
  - `Requeue` (which failures are asked again after a backoff instead of failing the run), `InvalidPage`
    (the error contract breaks wrap), `Size` (a row's bytes) and the provider's own `Counters`, reported
    after the engine's.

The engine's options are the scope prefix, `Sequential`, `Seed`, `Prior` (set by `SeedFrom`, below), `Structure` (`StructureOff`, `StructureAuto`, `StructureOn`),
`Floors`, `Workers`, `MaxKeys`, `Resume` and `PageSize`, the nominal page size (default 1,000): data and
seed requests ask for it, structure probes for no more, and the floors count pages of it. The
engine sorts and deduplicates each page's common prefixes (`prefixes_normalized`), so a provider need not.
A continuing structure probe (a provider may cut pages short) is a partial
sample whatever its length (its furthest folder is the pivot). Its tests (`internal/source/keyrange`) run on a fake listing
API over a sorted key slice (`keyrange/keyrangetest`, with the fixture and hostile keyspaces).

**Rules** (the generic contracts are in [contracts.md](contracts.md)): a range `(a, b]`
lists `a < k ≤ b`, and its right neighbor starts after `b` (the boundary belongs to the left); a synthesized
pivot is valid UTF-8, strictly between its bounds and adds no code point from the unsafe set (safe pivots);
synthesized pivots are at most 1,024 bytes. Row key limits belong to the bound schema
(`schema.ErrKeyTooLong`), not the engine; Azure permits longer names.

**Paging.** A range is `(lo, hi]`: `lo = ""` is ⊥, the open frontier has `hi = ⊤`. The first page starts
after `lo`, later pages follow the continuation token. Keys past `hi` are trimmed (`overfetch_keys`); a page
whose last key equals `hi` ends an ungrouped range without another request; a page that is not truncated
ends it. A truncated page without a token or a descending/repeated key is an error (group mode's repeated
keys are described below). Empty continuing pages follow their tokens under the bounded cycle and
progress checks in [contracts.md](contracts.md#execution-and-recovery).

**What is trusted.** swath takes a page that is not truncated as the true end of its range: every key the
service holds after the page's start, up to the range's end, was on that page. Nothing in one reply can show
otherwise. An empty page marked not truncated is exactly what an empty range returns, and swath never sees a
key that the service leaves out. Only a witness outside that reply can expose the omission:

- an independent sequential audit of the same scope (one continuation-token chain from the start);
- a key known to exist past the page's start, inside the range, that the listing lacks.

The durable spill campaign found such a service: MinIO answering a `start-after` inside one prefix with an
empty page ([measurements.md](../measurements.md#durable-spill), `bench/minio_start_after.sh`). Its key-file
oracle is the first kind of witness.

**Root and seed.** The root is `(⊥, ⊤]`. With the seed (on by default in the command, `swath diag list --seed=false` turns it
off; never with `--sequential`) the root is a seed that emits no rows and tiles the scope at cut points
`(⊥,c1], …, (ck, ⊤]`, all real folders found by delimiter listings (`/`, 1,000 entries a page,
`Probe` set):
- *The first data page.* The first wave also carries the scope's first data page (no delimiter, from the
  start a first tile would use; `Options.SeedFirstPage`, which the command's policy sets with the seed). When it
  is not truncated the scope fits in one page: the seed emits it, the delimiter requests still in flight are
  canceled, and the seed stops there with no tile (`seed.stop.one_page`, `seed.first_page_keys`), one round
  trip in all, as `--seed=false` (red team: 100 and 1,000 keys took 4x the `--seed=false` wall, 70 requests per
  1,000 keys on 100). A truncated page is discarded (`seed.first_page_discarded`, one request more) and the
  tiles are the seed's alone, from the scope's start. Emitting it and starting the first tile after it left
  the 12.6M-key megascenes list with a serial tail (12 s to 20 s, mean active 57 to 35, for the same requests
  and splits; `TestSeedFirstPageLargeFirstTile` is its shape).
- *Pagination.* The top level follows its continuation token up to 4 pages (`seed.pages`), so a top level of
  up to about 4,000 folders is tiled whole; past that it is `seed.truncated`. Four pages are already four
  times the most cuts a seed keeps. A chain whose last page held no folder (objects only) is not followed
  (`seed.flat_chains`; the level then stops as `seed.stop.flat` if it found no folder): each further page of
  a flat level is a round trip that finds no cut (the red team's flat leaves spent 4-6 waves for none). The
  cost: a level with more than a page of objects before its first folder is not tiled past them, and the
  tiles split there as any range does.
- *Descent.* While a level holds fewer folders than `--workers`, the next level is listed under every one of
  them, all at once: one *wave* of independent requests, through the provider's client and admission like
  any request. Each folder's chain is paginated the same way, one wave per page. The descent stops
  (`seed.stop.<reason>`) when a level holds at least `--workers` folders (`wide`) or none (`leaf`), or at a
  budget: 4 levels below the top (`levels`; the single-root date layout
  `warehouse/events/year=Y/month=M/day=D/` needs four to reach its day folders), 64 requests (`requests`; a
  level is listed whole or not at all) or 6 round trips (`round_trips`; each wave is one request latency on the
  run's critical path before any range lists). An empty continuing page that `list` follows by its token is a
  request and, on its chain, a round trip: the seed charges each one before it is sent (a wave costs its
  longest chain), and when one would pass a budget the chain stops there, truncated, with that budget's stop
  reason; without that charge, a delimiter chain behind empty pages would run unbudgeted inside one seed page.
  Java's serial 256-probe descent spent 13.5 s of a 39 s run on its chain; here a wave sends its requests at once, so the cost is the number of waves: 1 for a wide top level, 2
  for a narrow one, 5 for the date layout.
- *Cuts.* Every folder found at every level is a cut (a descended folder stays one, and its children lie
  inside its tile), sorted, filtered by the provider's bound predicate (`seed.unsafe`) and evenly subsampled to
  at most `min(1000, 4 × workers)` (`seed.subsampled`). `seed.level.<n>.cuts` counts the folders found at each
  level, `seed.requests` and `seed.round_trips` what the seed spent, `seed.levels` how deep it went. They are
  published on every exit: a seed whose listing fails or is canceled counts `seed.stop.failed` and reports
  what it spent until then, which on a resume can be most of the run.
- *Sentinel.* When the top level was listed whole and its last page ends with a folder `pk`, the last tiles are
  `(…, prefixCeil(pk)], (prefixCeil(pk), ⊤]`, so the folder holding the end of the top level is bounded and
  not the open frontier (a page ending with an object declines it, `seed.sentinel_declined.*`). With the
  descent, a bucket whose mass sits under its last top-level folder is tiled inside that folder too.
- *A resumed scope.* After `Restart(K)` the seed tiles `(K, ⊤]` instead: every folder above `K` inside the scope
  is listed at the top, after `K`'s longest accepted prefix (each lists the siblings after `K`'s branch at its
  level, so a resume inside one day folder still tiles the rest of the month and year), and only cuts strictly
  above `K` are kept: a cut at or below `K` would list published keys again, and the drop of keys up to `K`
  covers only the first tile, `(K, c1]`. An inclusive-start provider returns the folder at or below the bound,
  which the engine drops (`bound_drops`). `seed.resumed` marks it and `resumed_ranges` counts the tiles.

**Warm start** (`--seed-from PATH`, `prior.go`; off by default). A previous listing is a cut-point set with a density
prior, so the engine can start from it instead of the seed: the root tiles the scope at the prior's cuts and lists
nothing itself, no delimiter request is sent (`seed.requests` 0, the `seed` feature off, `seed_from` on). The prior
(`source.Prior`, in `internal/source` so the pipeline fills it without importing a provider package; `source.Seeded` takes it) is source-neutral: ordered byte-exact cut keys, each with an optional count of the keys expected
in the interval it closes, and the scope they were taken under. Two sources fill it (`internal/listing/prior.go`):
- *a committed dataset directory* (SeekParquet): the manifest's parts give exact bounds and row counts; the key
  column's page index (column index maxima and offset index row counts, 1,024-row pages; `parquetout.KeyPages`
  reads the footer and those two index sections, never a data page, through `seekparquet.InspectKeyPages`, which
  checks its footer and stamps first) gives cuts a thousand times finer. The
  reader offers 4,096 cuts of about equal rows (four candidates per tile at the most tiles a run starts with),
  reading the index only of parts a cut falls in; a part whose index cannot be read (another writer, a footer or
  stamp the module refuses) is cut at its own last key (`seed.from.index_failed`). The dataset's last key closes
  the last cut;
- *a cut file*: one key per line, ascending bytewise without repeats, optionally a tab and the expected count;
  every line has a count or none does (uncounted cuts weigh one each). Its cuts are held for the run and
  merged into at most 1,000 tiles, so the file is bounded: more than 262,144 cuts or 16 MiB (counted while reading, so a
  pipe or a growing file is bounded too) is refused before any request (`ErrPrior`; `maxCutFileCuts`, `maxCutFileBytes`, about 30 MiB held at most). Thin a larger one,
  every nth line with its count the sum of the lines it replaces (`TestSeedFromCutFileBounded`).

Identity, checked before any request (exit 2, class `refused`, `ErrPrior`): a dataset's manifest must name the
run's source kind and container (the bucket), and its scope must be the run's prefix exactly: a dataset of
`s3://b/logs/` does not seed `s3://b` nor `s3://b/logs/2026/` (a prefix mismatch is refused, not clipped). The
manifest records no endpoint or region, so those are not checked: a dataset of another store's bucket of the same
name is accepted, which can make the run slower, never wrong. A cut file records no scope: every cut must carry the
run's prefix. Either way the cuts must ascend, be valid UTF-8 and at most 1,024 bytes. The prior is not the output
directory; the sequential listing and sources off the range engine (the filesystem) refuse it.

Tiling (`tilePrior`): cuts at or below a resume key are dropped (a resume tiles `(K, ⊤]`, its first tile from
K's longest accepted prefix, as the seed's); a cut the provider does not accept as a bound is replaced by its
longest accepted prefix when that still sorts after the previous cut and inside the scope, else merged into the
next (`seed.from.unsafe`); with more cuts than `min(1000, 4 × workers) − 1`, a cut is kept where the running mass
first reaches the next multiple of total/(tiles − 1), the last always. The tiles are `(⊥, c1], …, (ck−1, ck]` and
the open frontier `(ck, ⊤]` for keys the prior did not know (`seed.from.tiles` counts all). Each bounded tile
carries its expected keys as its estimate (`Range.est`), the rank's remainder until its first page.

The prior sets the initial tiles and that first estimate only; the adaptive engine is the same for every source
(owner splits on demand, the gauge, the admission gates, the override, structure probes). How a stale prior is
corrected: a tile's first page replaces the prior's estimate with its gauge's remainder, so a grown segment is
under-ranked only until its first page; from then the rank, by estimated remainder, sees it as large, and once
workers go idle (hunger) it splits inside like any range, the gates reading the page, not the prior. A gauge that
cannot read the bound (+∞) ranks it above every finite estimate; where the gauge errs, the admission override (4
refusals in a row) and hunger cover the rest. An emptied segment costs one request for its tile; a new folder
between cuts lies inside one tile and is split there. The report shows it: `seed.from.error.within_2x`, `.grown`,
`.shrunk` and `.max_grown` compare every prior tile's listed keys (its descendants included, exact once the run
ends) with the prior's count; `split.pivot.*`, `split.max_depth` and `child_empty.*` show the splits it took, as on
any run; `seed.from.expected_keys` against `objects` is the scope's growth. The run report's `invocation.prior`
names the path, the source (`dataset` or `cuts`), the cuts offered and whether they were counted. On a resume the
prior is read again; one that is gone or no longer matches is a warning, and the resume lists without it.

There are no finer cuts than the page index: the index gives every 1,024th key, more than the 1,000 tiles a run
starts with need, and a tile's own pages measure the rest.

The seed is a serial pre-phase of 1 to 6 waves; on a bucket of one page the first wave is the whole listing.
The default is measured against `swath diag list --seed=false` on production key lists of 1M objects and more
([measurements](../measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)), and on
100 to 10,000 keys ([measurements](../measurements.md#red-team-fixes-2026-11-07)).

The seed only paginates and descends narrow levels, in parallel waves, under fixed budgets. Java swath's hybrid
seed descent (`HybridSeedPlanner`: a walk of the folder hierarchy that classifies truncated levels, samples
ambiguous folders and cuts dense flat folders into radix bands) has no counterpart: measured on the replayed
recordings it gave no gain on any of them and 30-60 % more requests on the shallow ones
([measurements](../measurements.md#policy-switches)).

**Splits are owner-side.** A range's children become claimable only when its `Expand` returns, so there are
no thieves. After each full page the owner takes the provider mutex and computes demand,
`hunger = workers − active − queued`. If `hunger > 0`, the range ranks among the `hunger` largest eligible
ranges, the guards allow the pivot and admission does, it emits `Child((c, m])` and `Child((m, hi])` and
returns, where `c` is the last key emitted. `(lo, hi] = (lo, c] ∪ (c, m] ∪ (m, hi]`, disjoint and in order.
Every split feeds an idle worker and no LIST is repeated.

**Admission** (`admission.go`): demand is the only trigger (no rate limit, no feedback loop); what admission
adds is whether a split is worth its requests. Each child pays a first request, and on S3, which has no end
bound, a child's last page overfetches up to a page past its `hi`; the split saves at most the page latencies
the owner would spend on the rest alone. Without the gates a small list splits about once per page and half
the children come back empty (measured without them on os-climate-physical-risk: 674 splits, 426 empty
children, 1.82 requests per 1,000 keys). One estimate serves both the rank and the gates:
- *The gauge.* The keys in an interval `(x, y]` are estimated from the range's last page, from its run of keys
  under the prefix `x` and `y` share (a page that crosses folders would otherwise measure the density of
  folder names). Keys are read as numbers in the run's own alphabet: the distinct bytes past the run's shared
  prefix, a byte outside it rounding up to the next one in it, the end of a key below them all. The run's
  keys per unit scale the interval. The page's alphabet, not base 256, keeps decimal counters in proportion
  (base 256 reads a remainder one decimal digit above the page 25 times too long), and a synthesized pivot
  past the run's digits (`logs/p/00001l` after `logs/p/000019999`) reads as the end of them. Where the run
  cannot read the interval the estimate is +∞ and no gate engages: no run of two keys under the prefix, or a
  difference above the run's varying digits at a position whose byte in the run is not one of its digits (a
  bound in another folder). A folder gap still reads as keys (a page cannot know that a folder ended), so on
  hierarchical keys the estimate errs long, where the gates stay out of the way.
- *The gates,* on bounded ranges only, after the guards, in pages of the range's own page length (a
  provider may return fewer keys than asked): no split when the remainder `(c, hi]` is under 2 pages (the
  owner lists it in at most two more requests; `split.skipped.remainder`), or when either child, `(c, m]` or
  `(m, hi]`, is under 1 page (the owner's next request covers a small lower child anyway; a small upper child
  leaves the lower one nearly the whole remainder, and an empty one overfetches a full page;
  `split.skipped.lower_tiny`, `split.skipped.upper_tiny`). The frontier's remainder is unbounded, and a
  frontier refused for a near pivot would keep its anchor at ⊥. A refusal declines that page's split only:
  the range decides again after its next page (`TestAdmissionRedecides`), and after 4 refusals in a row, each
  with demand, the next split is taken (`split.admission.overridden`, `TestAdmissionOverride`). The policy can
  propose one refused pivot page after page: on e11bio-prism the midpoint `…/0/1>` lies in the gap past
  `…/0/19…` below `hi` `…/0/2`, its upper child is truly empty, and its lower child holds a million keys;
  refused for the range's life, those went to one worker and the run fell to 4–13 wide (66 s against 7 s on a
  6M-key slice). A page of fewer than two keys gives no estimate (`split.estimate_none`).
- *The rank* is by estimated remainder: the open frontier first, then the larger remainder `(c, hi]` by the
  range's last page, ranges whose page could not read their bound (+∞, a bound in another folder) above every
  finite estimate, and age within a class and on ties: a strict total order (`TestRank`). A child starts with its share of the parent's estimate, `(c, m]` or
  `(m, hi]`, until its own first page; without either, the keys it has listed. The keys listed alone reset in
  every child, so a rank by them is a rank by age, blind to an upper child holding most of a remainder.

The pivot is not touched by admission; the gates refuse, they do not move a pivot, and they apply to a
structure boundary as to the policy's pivot. On the 4.1M-key noaa-nws-fourcastnetgfs-pds list and synthetic
1-2M-key lists the gates cut requests per 1,000 keys by 2-15 % with no wall increase
([measurements](../measurements.md#split-admission-on-small-lists)); a flat million-key list ends in about 250
ranges where it ends in 630-810 without them (`TestAdmissionFlat`: 1,000 pages at 64 workers, 100,000 keys in
pages of 100 by default, the million-key list in pages of 1,000 with `SWATH_PERF=1`). What remains are empty upper children of pivots placed in a folder
gap (a date's `day=32`-`day=99`, a letter after a counter's digits), which only a structure probe can see.

**Slow-range splitting** (off by default, `swath diag list --slow-split`; `Options.SlowSplit`). Demand is idle
workers and the rank counts keys, so a range whose pages are slow (one folder at 2 s a page among 200 ms pages,
the red team's `hotcold.narrow.d200`) stays on one worker until the rest drains (`split.skipped.no_demand`).
With the switch each range keeps its page latency (an EWMA, weight 0.5 on the newest page; children start from
their parent's), the rank reads time left (keys left × page latency) in place of keys left, and a bounded range
splits without an idle worker (`split.reason.slow_range`) when its page latency is at least 4 times the run's
mean (a latency outlier: neither jitter of 20-500 ms nor a slow half of the keyspace reaches it) and its time
left is at least twice the running ranges' mean. Keys left are the gauge's remainder or, where that is +∞ (a
seed tile's bound is the next folder, and a page cannot see where its own folder ends), the keys the range has
listed; a slow split's child that is still an outlier halves again without that test. Bounded: one slow split's
children wait at a time (they are queued, not run, until a worker frees up), and the admission gates apply.
Counters `split.slow.max_range_latency_ns`, `split.slow.run_latency_ns`. Measured: on a scaled slow folder
(`TestSlowRangeSplits`) 9 % off the wall; on the suite ([measurements](../measurements.md#red-team-fixes-2026-11-07))
too little to adopt: the slow folder still peels a few pages per split, as the pivot policy hands out what an
idle worker can take, not half a folder whose end no page shows.

**Floors** (Java swath's owner-split gate chain; off by default, `swath diag list --floors` turns them on;
[measurements](../measurements.md#policy-switches)).
Two gates on bounded ranges, read from a position estimate rather than a request (Java swath's default
rate-anchored sensor, `RateAnchoredEstimator`): the keys left are `max(keys listed, 1 page) × g`, where `g`,
clamped to [1/4, 16], is the remaining span over the consumed span read as base-256 fractions (12 bytes) past
the byte where the cursor diverges from `lo`; where the cursor shares more with `lo` than `hi` does, the window
is that shared subtree, with ceiling 1. A cursor at `hi` leaves 0. So a range clears the remaining floor after
at most 16 pages listed, however deep its keys' shared prefix.
- *Remaining floor:* a range with at most 4 pages (4,000 keys) left is not split. Counted per full page,
  before demand (`split.skipped.remaining_floor`).
- *Child-tail floor:* the child past the pivot must realize more than 2 pages. It is credited with
  `remaining × max(1/16, min(1, ρ) − f)`, where `f` is the pivot's fraction of the remaining span (same frame) and `ρ`
  the ratio of the trailing page density (EWMA, weight 0.3 on the newest page) to the range's average; a
  thinning keyspace (`ρ < 1`) shrinks the credited tail (`split.skipped.tail_floor`).
The open frontier's remainder is unbounded, so neither gate applies to it.

**Structure probes** (on their evidence by default; `swath diag list --structure=on` probes every range in a
folder, `--structure=off`, or `=false`, none; a library caller's zero `Options` has none). A page's density is
leaf-local and the anchor reflection overshoots into gaps between sparse folder names, so neither can place a
pivot on a distant folder boundary; a delimiter listing can. A probe is one delimiter request per folder level
(`max-keys` 32, start-after the cursor), all levels at once, alongside the range's next page, only while a
worker is idle and with at most one probe in flight per range; it takes its admission permit from the same pool
as pages. Who probes, and why (`structure.sent.*`):
- *the open frontier* (`frontier`), while its cursor has a folder below the scope: the scope root and the two
  folders above the cursor's own (its own folder lists the cursor's siblings, rarely a folder). The furthest
  boundary of the coarsest level that has any is the frontier's pivot when it lies further than the policy's
  pivot (`structure.frontier_pivots`); the lower child inherits the other boundaries and halves on them. One
  probe moves a date-partitioned frontier from `month=01/day=02` to `month=12/`;
- *a bounded range on evidence of a stall* (`stall`): the split that made it kept its pivot inside the cursor's
  own folder while `hi` lies beyond that folder, the shape where a chain peels one folder per round trip. Its
  levels are the folder where the cursor and `hi` diverge, then the next folder below it on the cursor's path.
  A flat keyspace inside one folder never shows that evidence, since its `hi` shares the cursor's folder;
- *every bounded range in a folder* with `--structure=on` (`forced`), a diagnostic.
The coarsest level with a folder inside `(c, hi)` gives the range its boundaries (`structure.boundaries`). At a
later split decision, if the result has arrived, a bounded range pivots on the median boundary inside
`(c, hi)`, or the furthest when that level's page was full; the policy's pivot is used otherwise. A split never
waits for a probe (`structure.late` counts decisions taken while one was in flight). A probe still in flight
when its range's `Expand` returns is canceled. Children inherit the boundaries inside their bounds, so one
probe serves several generations of splits, and they inherit the count of consecutive probes that found no
folder at any level: eight in a row along a range's ancestry stop probing there and below
(`structure.suppressed`; `structure.suppressed_inherited` when the eight were all an ancestor's), so a flat
keyspace pays at most eight probes along any chain.

**Guards**, checked before any child and counted per clause (a page that ended the range never reaches
them: the loop ends before the decision): `no_pivot` (the owner keeps listing), `unsafe_cursor` (`c` must be a usable start-after), `cursor`
(`m > c`), `unsafe_pivot` (the provider's `AcceptBound` vetoes `m`), `bound` (`m < hi`, or below
`prefixCeil(scope)` on the frontier). Each declines only that page's
split, and the next continuing page is evaluated afresh, except `no_pivot` after a safe cursor: a later cursor only
narrows `(c, hi]`, so the range stops considering splits (`TestSingleUnsafeCursorDoesNotDisableLaterSplits`: one
unsafe or vetoed page-ending key among 100,000 still leaves 98 splits). That `no_pivot` is reached only when the
midpoint `between(c, ceiling)` finds neither a safe scalar nor a sliver, so it is a fact about the interval, never
a synthesis step that could not run on its inputs (`TestInvalidScopeCeiling`).

**Pivots.** A split waits for a page, so a chain of `d` dependent splits costs at least `d` page latencies:
the pivot policy is judged by the depth of the split tree it builds (`split.max_depth`), which a balanced tree
keeps near `log2(pages)`. A pivot that peels a few pages off the cursor and leaves the remainder in the upper
child makes the depth linear in the range's size. So the pivot is the further of two steps (`propose`):
1. **The anchor reflection.** Each chain of upper children carries an anchor: on the open frontier a seed or
   resume tile's `lo`, or, for a chain that started at ⊥, its first pivot (⊥ itself measures nothing); on a
   bounded chain the first key its first range listed. The cursor is reflected from the anchor at the scalar
   where they diverge, by `idx(c[i]) − idx(anchor[i])` (`forwardReflect`). The covered distance then doubles
   per split (δ, 3δ, 7δ, …) and climbs folder levels by itself: from `day=01` the frontier reaches `day=03`,
   `05`, `09`, then `day=A`, which sorts after `day=31`, and the next month; reflecting from the previous
   pivot instead would cover one page per split, since the upper child's cursor is one page past it. A
   reflection at or past the range's ceiling (`hi`, or `prefixCeil(scope)` on the frontier) falls back to the
   safe midpoint `between(c, ceiling)`, and the upper child starts a new chain measured from its own keys
   (`split.reflect.ceiling`): the remainder is then shorter than the distance already covered. A ceiling
   that is not UTF-8 (`prefixCeil("ÿ")` is `c3 c0`) is replaced by the least valid string at or above it
   (`Ā`), which bounds exactly the same valid keys, so a scope ending in a multi-byte scalar still has a
   midpoint. An anchor that is not UTF-8 (a first key) measures nothing, and the reflection is taken from ⊥.
2. **The galloped density step.** Keys are read as numbers in the alphabet seen on the last page (safe ASCII
   and `%`; the target's first new character is synthesized, so a `%` there is snapped up to `&`); the target is `c + page span × (max(4, hunger) << g)`, at most halfway to `hi`, cut after its first
   character that differs from `c`. `g`, the gallop generation, counts the consecutive upper children along the
   chain (a lower child is 0, at most 16), so a remainder that keeps splitting hands out twice as much per
   round trip. A target past the representable keys, with a hole, or outside `(c, hi)` is halved within the
   decision, down to 4 page spans (`split.gallop.halvings`). The step is leaf-local: one page's span lives in
   the last folder's digits, so it cannot reach a parent folder; that is the reflection's job. It may move the
   pivot further than the reflection, never nearer (`split.density.shrink_prevented`), on the frontier and in
   bounded ranges alike: a nearer pivot would make the frontier crawl one folder per round trip.
A folder boundary known from a structure probe inside `(c, hi)` takes precedence over both in a bounded range; on
the frontier it is taken when it lies further. A sliver, the
only pivot when nothing safe lies between the cursor and the ceiling and the density step is refused
(`cursor + " "`, or the 1,024-byte cap's bump), leaves the lower child all but certain to be empty: it is
refused (`split.sliver_refused`). The range is not made unsplittable; it keeps listing and decides again after
its next page. The split's demand slot is reserved in the same decision, so concurrent owners do not split for
the same idle worker. Every pivot is safe (I12); `FuzzPropose` checks it over the policy's inputs.

The pivot is placed outside the decision mutex, from the page and a snapshot of the demand taken with the
page's bookkeeping: the density step's big-number arithmetic, up to one evaluation per halving, is the costly
part of a decision. The decision itself (demand, rank, a structure boundary, the guards, the reservation) stays
under the mutex; a page whose snapshot saw no demand is not split even if demand arose since
(`split.skipped.no_demand`). Counters are atomic and are not a reason to take the mutex: a path that only
counts (requests, pages, overfetch, structure probes) does so without it, and `SWATH_SPLIT_TRACE` prints after
the split is committed and the mutex released, so `mutex.wait_ns` measures decisions. Generation and anchor live on the immutable `Range`; nothing persists them (a
resume starts new chains).

There is no 1-key pivot probe (Java's `max-keys=1` request after the pivot, moving an empty upper child's
pivot toward the cursor): on the deep replayed recordings ([measurements](../measurements.md#policy-switches))
it cost more than the empty children it saved, a serial round trip before each split that lowered
concurrency: 27–31 % more requests and 2.2–2.3× the wall.

**Counters** (in the run summary): objects, bytes, pages, requests, retries, `overfetch_keys`; seed
(prefixes, pages, requests, round trips, levels, cuts per level, why it stopped, cuts, unsafe, subsampled,
sentinel and why declined, resumed); warm start (`seed.from.*`: parts, indexed and failed parts, cuts, unsafe
cuts, tiles, expected keys, the prior's error per tile); splits (considered, accepted bounded and
frontier, rejected per guard clause, skipped for no demand, outranked, or by an admission
gate), each accepted
split's pivot source, the density refinement's refusals by reason; `child_empty.{lower,upper}` and by pivot
source and kind of `hi`; small children; depth; unsplittable ranges; the floors' skips; structure probes and
their pivots; `max_active`, `max_queued`, `mean_active`; busy, idle-with-demand and sink-wait time.

**Reading a split chain from the report.** Every accepted split counts one pivot source:
`split.pivot.reflect` (the anchor reflection), `split.pivot.midpoint` (a safe scalar between the cursor and the
ceiling, after a reflection reached it), `split.pivot.density.bounded` and `.frontier` (the galloped density
step lay further), and `split.pivot.structure`. `split.frontier.anchored` counts frontier splits whose chain had
an anchor, `split.gallop.max_generation` the longest run of upper children that split, and
`split.reflect.ceiling`, `split.density.shrink_prevented`, `split.gallop.halvings` and `split.sliver_refused` the
decisions where those rules engaged. When the density step is refused, `split.density.refused.*` says why
(`alphabet` is a page with a non-ASCII or control byte: such keys turn density off, and only the reflection and the
midpoint remain). A child carries its parent's pivot source, so an empty child is attributed
(`child_empty.by.<source>.<lower|upper>`) and classed by its `hi` (`child_empty.hi.pivot`: a pivot; `.bound`: a
seed cut, the scope's end or ⊤). The chain's length is `split.max_depth`, its mean over the leaves
`split.leaf_depth_sum` / `split.leaves`; at one page latency per step, depth times `inflight.p50_ns` is a floor
under the wall time. Children ending under one page (`child_small.*`) and the keys a frontier split handed to
its lower side (`split.frontier_handout_keys` / `split.accepted_frontier`) show the balance of the splits;
`idle_demand_ns` is worker time idle while work was wanted, the cost of a policy that cannot hand work out fast
enough; `split.unsplittable` and `split.unsplittable_keys` are ranges a guard made unsplittable and the keys one
worker then drained alone. `SWATH_SPLIT_TRACE=1` prints each split with its pivot source, depth, gallop
generation and the anchor its upper child carries.

**Sequential oracle.** `swath diag list --sequential` lists the scope as one continuation-token chain; its dataset is
byte-identical to the parallel core's. It is the only mode where `-max-keys` stops early (the dataset is
then marked incomplete).

## Resume

`func (*Engine[P]) Restart(after string) error` sets the engine's `Options.Resume` and returns `nil`.
Call it before execution (`Root` or `Expand`). S3, GCS and Azure promote this method; they keep no separate
resume state in provider options. The registered source exposes it through `keyrange.Registered`.

`Restart(K)` makes the scope `(K, ⊤]`: with a prior or the seed it is tiled as above, without them the root
is one range `(K, ⊤]`, split as on a fresh run. The engine chooses K's longest accepted start bound and drops
rows through K. Group mode drops every version of K, across pages. `source` `TestRegisteredRestart` checks
remaining keys and complete version groups through registry opening; engine resume and crash tests cover
range tiling and recovery ([durability.md](durability.md)).

## Group mode (`groups.go`)

A versions listing (`Description.Groups`, which must agree with the bound schema's `Info.Groups`, and
`Description.Detach`, the schema's) returns a key once per version, consecutive, and a page may end inside a
key's rows. Every consumer treats the last key it saw as finished (a split's lower child starts after it, the
journal records it as published, a resume lists after it), so within one chain (an `expandRange` call or the
sequential chain) the engine emits a row only once its key's rows are proven complete:

- *Hold-back.* A continuing page's trailing key group is held and prepended to the next page; a later key or
  the chain's end (EOF, or a row above `hi`) completes it. A page that is all one new key is held whole; a page
  that repeats the held or emitted key cannot be held (a key can span any number of pages), so it is emitted as
  it arrives and the chain's group is *open*. Held rows come from at most one page; the emitted and the held
  sides of a partial page are both copied out of the page (`Detach`), so a few rows never pin
  a page the order stage did not charge.
- *Splits.* A range splits only at its last emitted key, which is complete: never while its group is open, and
  not before it has emitted a key (a split at `lo` would hand the whole range on again). The veto is in
  `wanted`, in `decide` (before the coin, structure boundaries and the gates) and asserted before `commit`;
  such a range is left out of the split rank. Both children are ordinary fresh ranges; held rows are dropped
  and listed again by whichever child owns their key (`groups.split_relisted_rows`). Nothing crosses an
  `Expand` call: a retry, a requeue or the drainer's expansion starts fresh.
- *Bounds.* A bounded range lists until a key above `hi` or EOF, not until `c == hi` (hi's rows may continue).
  A resume drops every row at or below its key, across pages, without moving the cursor. The inclusive-start
  drop removes the bound key's whole group across pages.
- *Order and progress.* Rows are nondecreasing; a row equal to the previous page's last key is allowed only
  when that key's group may continue. A continuing page that shows no key above the chain's last must carry a
  token outside the window of such pages' tokens (1,024), or it is an invalid page; ever-fresh tokens over one
  key earn watchdog credit only within the first 1,024 pages of a no-advance run. Raw keys are
  validated before bound and resume suppression; suppressed rows cannot reset that window.
- *Units.* `st.keys` counts rows (work); density, the gauge and the pivot take distinct keys.
- *Counters:* `groups.open_pages`, `groups.held_rows`, `groups.split_relisted_rows`, `groups.max_rows`, `groups.overfetch_rows`,
  `split.skipped.group_open`.

Group page fragments are detached on both sides before emission or retention: their row arrays and
string arenas cannot retain excluded rows outside the order budget. Native chains validate raw keys
before inclusive or resume suppression. Only a strictly greater observed key resets the token window;
same-key pages earn watchdog credit for at most the first 1,024 pages of a run. GCS also validates
strictly ascending generation ordinals across page seams, including empty and suppressed pages,
with state owned by each chain. `groups.max_rows` scans every group, including interior groups,
sequential listings and seed EOF. `groups.overfetch_rows` counts only rows above a range's upper bound
read to close its last group; resume suppression and split relisting have separate accounting.
