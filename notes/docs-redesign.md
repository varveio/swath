# Newcomer-first documentation redesign

## Problem

swath's documentation is thorough but does not disclose complexity progressively. The README is
reasonable on its own, yet the documentation set is about 119,000 words. Installation, flags,
checkpointing, metrics, defaults, and the listing algorithm are each explained in several places.
That makes a first successful run harder to find and gives maintainers too many copies of facts
that can drift.

The rewrite must serve three readers without making any one of them read material intended for
the others:

1. a newcomer deciding whether swath fits and trying one listing;
2. an operator choosing outputs, resume behavior, credentials, and tuning; and
3. a contributor verifying the split, checkpoint, schema, and instrumentation contracts.

## Prior art

Current official documentation from eight technically comparable projects was reviewed in August
2026. These links are examples of information architecture, not sources for swath's wording.

| Project | Useful pattern |
| --- | --- |
| [rclone](https://rclone.org/install/) | Installation begins with a short procedure, then routes to task and backend references. |
| [restic](https://restic.readthedocs.io/en/stable/010_introduction.html) | A complete workflow comes before the separately versioned [design reference](https://restic.readthedocs.io/en/v0.18.1/design.html). |
| [s5cmd](https://github.com/peak/s5cmd) | Strong positioning and concrete examples; its very long README is a useful boundary not to cross. |
| [DuckDB](https://duckdb.org/docs/stable/clients/cli/overview) | The CLI path is procedural, while the [internals overview](https://duckdb.org/docs/current/internals/overview) starts from a processing model. |
| [MinIO `mc`](https://github.com/minio/mc) | The README explains the idea and first connection; the command reference owns exhaustive detail. |
| [ClickHouse](https://clickhouse.com/docs) | User navigation follows jobs; architecture lives under contributor resources. |
| [ripgrep](https://github.com/BurntSushi/ripgrep/blob/master/README.md) | The README explains defaults and surprising behavior; the [guide](https://github.com/BurntSushi/ripgrep/blob/master/GUIDE.md) teaches progressively and generated help owns flags. |
| [Borg](https://borgbackup.readthedocs.io/en/stable/quickstart.html) | The quickstart introduces only the concepts and warnings required for the workflow; [internals](https://borgbackup.readthedocs.io/en/stable/internals.html) are separate. |

The common shape is: state the promise, complete one realistic workflow, organize user material by
task, and give technical readers a distinct mental-model-first entrance. Exhaustive flags belong to
CLI help or a single reference, not repeated narrative.

## Proposed information architecture

Stable paths referenced by code and tests remain stable. The hierarchy changes through ownership
and navigation rather than a disruptive directory rename.

| Layer | Canonical owner |
| --- | --- |
| Product promise, fit, one command | `README.md` |
| First public/private listing, inspect output, stop/resume | `docs/getting-started.md` |
| Release artifacts and source installation | `docs/install.md` |
| Common workflows, output forms, public Parquet schema, resume, exit codes | `docs/usage.md` |
| Configuration sources, precedence, environment, `--tune`, diagnostic toggles, and JVM properties | `docs/configuration.md`; exact visible flags remain in `swath list --help` |
| Credentials, IAM, endpoints, and request cost | `docs/operating.md` |
| Performance interpretation and tuning | `docs/performance.md` |
| Operator interpretation of progress, summaries, and exported metrics | `docs/metrics-and-observability.md` |
| Plain-language engine mental model | `docs/internals/overview.md` |
| Component ownership and run flow | `docs/internals/architecture.md` |
| Normative engine mechanisms and correctness argument | `docs/internals/algorithms.md` |
| Load-bearing invariants, persistence schema, sort format, and delivery guarantees | `docs/internals/contracts.md` |
| Instrument identities, engagement registry, and trace schema required by CI | `docs/internals/metrics-internals.md` |
| Release history | `docs/ops/dev/RELEASE_NOTES.md`, retained as an authoritative release input |
| Dated raw investigations | `docs/ops/dev/field-investigations.md`, retained as supporting evidence rather than product semantics |

Every other page may summarize a canonical fact in one sentence, then links to its owner. It must
not carry a second detailed table or explanation.

### Page disposition

| Page | Disposition |
| --- | --- |
| `README.md` | Rewrite as promise → idea → quickstart → fit/limits → routes. |
| `docs/README.md` | Rewrite as the task-oriented Start / Operate / Understand / Contribute index. |
| `docs/getting-started.md` | Add the single five-minute public/private Parquet workflow. |
| `docs/install.md` | Keep and shorten to installation and verification only. |
| `docs/usage.md` | Keep; consolidate common workflows, output semantics, public schema, resume, and exit codes. Remove installation, metrics, algorithm, and diagnostic-toggle essays. |
| `docs/configuration.md` | Keep; own `--tune`, diagnostic toggles, environment variables, precedence, and JVM properties. Move the machine-checked tune table and its test/build input here. |
| `docs/operating.md` | Keep; own credentials, IAM, endpoints, and cost. Remove summary-UI duplication. |
| `docs/performance.md` | Keep; consolidate current evidence and the operator diagnosis sequence. |
| `docs/faq.md` | Keep as short symptom → action routing; do not duplicate tables. |
| `docs/packaging-and-docker.md` | Keep as release-maintainer and container reference; remove install tutorial duplication. |
| `docs/metrics-and-observability.md` | Keep; rewrite around operator questions and concise field semantics. |
| `docs/replay-troubleshooting.md` | Keep but shorten to the reproduction workflow; link to replay reference for flags. |
| `docs/swath-replay.md` | Keep as the replay toolkit reference; remove repeated rationale and tuning narrative. |
| `docs/internals/overview.md` | Rewrite as the accessible technical bridge and routing page. |
| `docs/internals/architecture.md` | Keep and consolidate to the component map and run flow. |
| `docs/internals/algorithms.md` | Keep and consolidate current normative mechanisms; remove rollout/review chronology. |
| `docs/internals/contracts.md` | Keep I1–I12, current types/schemas/protocols/defaults; remove extraction audits and deferred pseudo-APIs. |
| `docs/internals/walkthroughs.md` | Keep; edit for standalone readability after the overview. |
| `docs/internals/metrics-internals.md` | Keep CI markers/registry and trace schema; collapse essays duplicated in public metrics or algorithms. |
| `docs/internals/probe-budgets.md` | Keep as a short focused call-class contract; move dated incident narration to field evidence. |
| `docs/internals/s3-implementation-compatibility.md` | Keep as the focused endpoint-compatibility reference. |
| `docs/internals/build-and-modules.md` | Keep as contributor build/module reference. |
| `docs/ops/dev/TESTING.md` | Keep as the canonical testing workflow. |
| `docs/ops/dev/decision-trace-goldens.md` | Keep and consolidate to current use/regeneration/coverage. |
| `docs/ops/dev/field-investigations.md` | Keep as dated supporting evidence; label it non-normative and shorten repeated mechanism prose. |
| `docs/ops/dev/PARALLEL_MERGE_DEFAULT_GATE.md` | Delete: rollout is complete, the result is already recorded in current performance evidence, and Git history preserves the runbook. |
| `docs/ops/dev/RELEASE_NOTES.md` | Keep content unchanged as the release workflow's live input; repair links if their targets move. |
| `swath-sim/README.md`, `swath-sim/docs/executor-ordering.md`, `tools/explainer/README.md` | Keep their specialized references; repair links and remove duplication only where the main-doc rewrite exposes it. |

## Content rules

- Put a copy-pasteable successful listing before architecture, tuning, benchmarks, or every install
  variant.
- Explain the work-stealing idea once in plain language, with a small range-based example. Deep pages may
  use the exact `(A, B]` model after linking back to that explanation.
- Keep decision-critical warnings next to the action they affect: LIST cost before a large run,
  sorted-output disk requirements beside `--sort`, and resume limitations beside output choice.
- Preserve I1–I12, schemas, checkpoint ordering, byte-exact key handling, split CAS, bounded-buffer
  claims, and the CI-enforced steal-reason registry. These are not simplification targets.
- Remove review narratives, rejected alternatives, stale rollout plans, and duplicated option or
  metric prose. Retain durable rationale where it explains a current choice.
- Treat CLI help and code constants as the source for visible flags and defaults. Documentation
  teaches choices and exceptional constraints.
- Preserve referenced headings/anchors and literal CI markers. A renamed section is allowed only
  when every source, test, script, and doc caller is migrated in the same change. In particular,
  keep the metrics registry's `<!-- ci:steal-reason-table:* -->` markers and the semantic identities
  currently referenced as algorithms/contracts § sections and metrics §5/§5a/§7.
- Keep deep technical pages discoverable from both `docs/README.md` and the plain-language internals
  overview; do not place them in the newcomer sequence.

## Acceptance criteria

- A reader can go from the README to a successful public-bucket listing in at most one link and
  five minutes of reading.
- The getting-started path demonstrates public access, private credentials, Parquet output, a
  DuckDB query, interruption, and resume without requiring an internals page.
- The README is 800–1,300 words and has one primary quickstart.
- `docs/README.md` has visible Start, Operate, Understand, and Contribute paths.
- Each facet listed in the information-architecture table has exactly one canonical detailed
  owner; other pages only summarize and link.
- Exact I1–I12 wording and all CI-parsed documentation markers remain present unless the associated
  tests are deliberately updated to an equally strong machine check.
- All repository-local Markdown links resolve under a script that parses inline Markdown targets,
  resolves relative paths, and validates explicit anchors against headings/HTML anchors.
- `HeadlineDocsCommandSmokeTest` parses the README and getting-started headline commands;
  `TuneOptionsTest` checks the canonical table at its new owner.
- `./scripts/ci/check-instrumentation-drift.sh` and `./gradlew build` pass.
- The baseline active product documentation is 119,991 words (`README.md` plus `docs/**/*.md`,
  excluding the release-workflow-owned `docs/ops/dev/RELEASE_NOTES.md`). It falls by at least 35%
  without moving prose to `notes/`. The baseline all first-party Markdown is 139,277 words
  (excluding `THIRD_PARTY_NOTICES.md` and build/cache trees); report its final count separately so
  archival movement cannot masquerade as consolidation.
- Dated evidence is clearly separated from current user and contributor guidance.
- Existing source/test/doc path and section references remain resolvable and semantically accurate.
- The newcomer path states prerequisites, shows expected artifacts, and gives the next command for
  authentication errors, interruption, and a completed run.

## Deliberate non-goals

- Building a documentation website or introducing a docs generator.
- Redesigning the CLI or changing runtime behavior.
- Hiding advanced tuning and observability from operators who need it.
- Removing detailed correctness material merely to reach a word-count target.

## Open questions

None block the first pass. A future change may generate a checked-in CLI reference from picocli,
but this rewrite can rely on `swath list --help` and the existing help golden without adding build
machinery.

## Result

The completed first pass has 77,549 active product-documentation words, a 35.4% reduction
from the 119,991-word baseline. All first-party Markdown is 97,470 words, down 30.0% from
139,277. No prose was moved into `notes/` or an archive to reach either result.
