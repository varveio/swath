# Contracts

What the Go tool promises, by responsibility: what a dataset guarantees to its readers, what each producer
means by its rows, and how a run executes and recovers. Each rule names its mechanism and the test that holds
it ([../testing.md](../testing.md)). Provider-specific rules (S3's `(lo, hi]` ranges, safe pivots, the key
limit) live with the provider ([providers.md](providers.md)); the correspondence with Java's I1–I12 is
migration history, at the end.

## Dataset guarantees (readers rely on these)

| Rule | Mechanism | Test |
|---|---|---|
| **The manifest is the commit.** A directory without `manifest.json` holds no dataset; one with it holds exactly the parts it lists, in key order, under `data/`. | `dataset.Publish` installs `manifest.json` last by an atomic rename and a directory sync, after every part is fsynced and renamed. A failed or cancelled run leaves no manifest. | `TestDatasetContract`, `TestPublishFailureLeavesNoManifest`, crash matrix |
| **Coverage is stated.** The manifest says what was listed (`source`, `scope`) and how much of it (`coverage`: complete, stop reason, skipped entries, exclusions, filters, `snapshot: false`). A reader that needs the whole source fails closed on anything less. | Coverage comes from the provider's report; the run report is never part of acceptance. | `TestS3Dataset`, Varve probe |
| **Keys are bytes in unsigned order, unique, strictly ascending within and across parts.** Parts are key-disjoint, so they concatenate without a merge. | Every stage checks strict ascent: the provider, the order stage's stream-wide guard on every row (the resumed seam included), the router on each part against the one before it and the journal on each record, both before the part is journaled, the writer, and the publication gate (`*dataset.InvariantError`). | `TestDatasetContract`, `TestOrderGuard`, `TestBoundaryDefectNeverReachesTheJournal`, `verify.py`, `diffds.py` |
| **Part facts are exact.** Each part's `rows`, `bytes`, `md5` and `min_key_base64`/`max_key_base64` (the first and last key's exact bytes) are true; the parts hold every row listed. | The writer measures them; the gate checks counts and order; the row counts are reconciled at the provider, the order stage (received and handed on), the router and the writer before the manifest. | `TestDatasetContract` |
| **The page-index profile.** Footers carry exactly `swath.profile`, `swath.profile_version`, `swath.schema_id`, `swath.index_bound_bytes` and standard `sorting_columns`; 1,024-row pages with column and offset indexes; index bounds over 1,024 bytes are truncated conservatively. | `parquetout` writer constants ([parquet-output.md](parquet-output.md)). | `TestDatasetContract`, `TestKeyLimits`, `verify.py` |
| **Parts are immutable.** A published part is never rewritten; a resume verifies retained parts by size and MD5 before continuing. | `recoverParts` (`ErrPartAltered`). | crash matrix, `TestAlteredPartIsRefused` |

## Producer semantics (what a row means)

| Rule | Mechanism | Test |
|---|---|---|
| **One row per listed entry, once.** The ranges of a run partition the scope: every key in it is listed by exactly one range, and no key outside it. | Providers split ranges into disjoint children; the order stage drains them depth-first. | S3 T1 random-split tiling, `TestListingEqualsSortedOracle` (filesystem) |
| **S3: every row is a current object** (`swath/s3-objects/v1`); a key ending in `/` is an object; delimiter prefixes are derived, never stored; owner columns are null unless asked for at run start. | `s3.Schema`. | `TestS3Dataset`, `diffds.py` against Java |
| **GCS: every row is a live object** (`swath/gcs-objects/v1`; its current generation, listed by `objects.list` without `versions`); a name ending in `/` is an object; delimiter prefixes are derived, never stored; checksums are base64 as listed and absent fields are null. | `gcs.Schema`, the adapter's row mapping. | `TestRowsCarryEveryField`, `TestClientPagesMatchGoogleClient`, the replay identity with the S3 route (REPORT) |
| **Filesystem: one row per entry below the root**, directories included with a named `kind`; names are bytes, never skipped or normalised; symlinks are not followed. | `localfs.Schema`; `getdents64` and portable engines. | `TestListingEqualsSortedOracle`, `TestKeysAreBytes` |
| **A listing is not a snapshot.** Entries that change while it runs may or may not be in it. | `coverage.snapshot` is always false. | – |

## Execution and recovery

| Rule | Mechanism | Test |
|---|---|---|
| **Bounded memory.** Rows in flight are bounded by `--memory`, not by object count, apart from the awaited range and the root-to-frontier path; spill takes ranges ahead of the frontier to disk within its disk budget. | `order.Options.Budget` shared with the encoders' queues; spill ([ordering.md](ordering.md)). | `TestSpillKeepsWorkersBusy`, `TestSpillDiskBudget` |
| **No wait on child work; no deadlock on a full buffer.** | Children become claimable; the drainer expands what it awaits itself. | order tests |
| **No infinite loop.** A truncated page needs a continuation token. A continuing page with no entry is followed by its token (object stores may return them), and every token of a run of them must be new, or the page is an error; a nonempty page must hold keys strictly ascending past the cursor. | `keyrange.Engine.list` for every request path (ranges, the sequential chain, the probes); the order checks in each path. | `TestEmptyContinuingPages`, `TestInclusiveOneKeyProbe`, `TestT4Paging` |
| **Deterministic output within a build.** The same listing produces byte-identical parts for any worker or encoder count. | Part boundaries depend only on rows; column-parallel encoding equals serial. | `TestRouterIsDeterministicAcrossEncoders`, gates (same-build MD5s) |
| **Commit before emit.** A part is renamed into `data/` only after its journal record (its `PartInfo` and the writer state) is fsynced. | The router's in-order `Publish` hook, the one ordered committer. | crash matrix, `TestSIGKILLAtEveryPoint` |
| **Durable-prefix recovery.** A resume never emits a published key again and never skips one: it lists after the last recorded part's max key K, or from K's longest prefix the provider accepts as a bound, re-fetching and dropping the keys up to K. On a static source the result is byte-identical to an uninterrupted run; on a live one, durable output is exactly-once. | `swath resume`, `source.Resumable.Restart(K)`; no split or cursor is journaled ([durability.md](durability.md)). | crash matrix, `TestResumeAfterKey`, `TestBoundVetoOnEveryRequest`, `TestSIGKILLResume`, `TestAmbiguousTailRefusesOnChangedSource` |
| **One owner per output.** Nothing in the output is read, removed or renamed before the process holds its lock. | `journal.TakeLock` before preparation or recovery. | `TestCompetingRunsAreRefused` |
| **A frozen identity.** A resume refuses changed schema, writer recipe, part size, key cap or source identity (`*journal.MismatchError`) and a journal of another version (`*journal.VersionError`). | `journal.Check`, a pure function. | `TestCheck`, `TestResumeChecksIdentity` |
| **Shutdown order.** On cancellation the drainer stops, wakes all waiters, waits for the workers, then the router closes and waits for its encoders; an interrupted run (a signal, `--max-duration`, `--stall-timeout`) gets 15 s, then the process exits with its code. | `order.Run`, `cmd/swath`. | cancellation tests |

## Migration history: Java's I1–I12

The rules above were first stated as Java swath's contracts (`docs/internals/contracts.md` §0 in the Java
tree). Correspondence: I1 commit before emit; I2 one row per entry (no gap, no overlap); I3 the boundary
belongs to the left (`(a, b]`, an S3 rule now in providers.md); I4 a split is atomic with one owner (owner-only
splits, never journaled); I5 and I6 durable-prefix recovery; I7 no wait on child work; I8 shutdown order; I9 no
infinite loop; I10 unsigned byte order; I11 bounded memory; I12 safe pivots (an S3 rule). Java's §4.1
publication order (`_SUCCESS` last) became "the manifest is the commit". Retired as Go requirements: the
`_SUCCESS` ordering, the compound version/row-type sort stamps, the 13-column canonical schema and Java's
schema-string spelling ([../upgrade.md](../upgrade.md)).
