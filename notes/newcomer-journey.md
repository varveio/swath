# Newcomer journey across Swath surfaces

Status: approved for prototype

## Problem

The Swath homepage, field guide, and README are each technically credible, but
all three begin near the same mechanism-heavy depth. A visitor arriving from
Varve has to understand keyspace partitioning before getting a small, safe
first success. The quickstart uses a placeholder bucket, important “use
Inventory instead” guidance is buried, and the field guide has no prominent
path back to installing the tool.

The goal is progressive disclosure without flattening Swath's technical depth.

## Research summary

Current pages were audited at desktop and mobile widths. Prior art was sampled
from the official sites and documentation for restic, rclone, uv, Litestream,
and DuckDB.

| Project | Observed newcomer pattern |
| --- | --- |
| [restic](https://restic.net/) | Leads with the familiar backup job and benefits, then routes to use and internals. |
| [rclone](https://rclone.org/) | Explains itself through familiar user jobs and gives a short, numbered first run before exhaustive reference. |
| [uv](https://docs.astral.sh/uv/getting-started/) | Separates guides, concepts, and reference; tutorials show a concrete result before explaining artifacts. |
| [Litestream](https://litestream.io/getting-started/) | Creates a working first success, then routes separately to deployment, mechanism, caveats, and troubleshooting. |
| [DuckDB CLI](https://duckdb.org/docs/current/clients/cli/overview) | Gives the exact first command and expected prompt, then explains choices and expands into reference. |

The convergence is: familiar problem, concrete outcome, who it is for, one
bounded action, then deeper mechanism. Limitations work best as decision
support (“use this when; use that when”), not as a status disclaimer.

## Decision

Give each surface one job:

- **Homepage: front door and router.** Establish relevance in 30 seconds, give
  one verified first run, then route to GitHub, the full quickstart, the field
  guide, and recorded evidence.
- **README: practitioner entry point.** Establish fit, get a user running, show
  the result, then explain mechanism, scope, and references.
- **Field guide: mechanism and evidence.** Preserve its depth and measured-run
  opening; add a clear exit to using the tool.

Do not repeat the full explanation on every surface. The deliberately repeated
artifact is one canonical public-demo target.

## Canonical public demo

Target:

```text
s3://noaa-gestofs-pds/estofs.20210101/
```

Verified 2026-08-13:

- AWS region: `us-east-1`
- anonymous `ListObjectsV2`: allowed
- objects under prefix: 5,196
- raw pagination: six pages
- the parent bucket is the same bucket used by the recorded 39.7M-object run

Canonical first run (Docker required; no Swath or JDK installation):

```bash
docker run --rm ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/estofs.20210101/ \
  --region us-east-1 --no-sign-request --format tsv > /dev/null
```

Supporting copy:

> One day of NOAA's global surge model output on AWS Open Data: about 5,200
> objects, listed anonymously in a few seconds. No AWS account or credentials
> are needed; these anonymous LIST requests are not associated with your AWS
> account. The object rows are discarded in this installation and connectivity
> check, while Swath's summary reports the objects, time, API calls, and
> estimated request cost. The next command keeps the result as Parquet. This
> small slice validates the workflow, not Swath's scaling claims; the recorded
> full-bucket run is the scale and mechanism evidence.

Do not publish an exact Swath request count because probes and splits add to the
six raw pagination requests. Re-verify the prefix whenever a release changes
these public surfaces; a dated operational prefix can eventually be archived.

## Homepage changes

Keep the current identity, tagline, two destination cards, video, and restrained
visual system.

Replace the opening mechanism paragraph with:

> For when you need every key in a bucket too big to list one page at a time —
> and there's no fresh S3 Inventory to fall back on. Swath lists; it never reads
> object contents.
>
> It divides the unknown keyspace into guessed ranges, **corrects the guesses
> while it lists**, and writes JSONL, TSV, or crash-resumable Parquet.

Add a **First run · a real public bucket** block after the primary actions. It
contains the canonical command, supporting copy, and a link to the maintained
install and quickstart guide at
`https://github.com/varveio/swath/blob/main/docs/install.md`. Keep the code block
selectable and focusable; no copy-button JavaScript.

Rename the recorded-run card heading to **A recorded 39.7M-object listing** so
the heading names the destination rather than presenting bare metrics. State
that it is the same NOAA bucket as the demo slice.

The maintained quickstart link points to `docs/install.md`, not a README anchor.

Replace the footer version-status paragraph with factual shipped/roadmap copy:

> Swath ships `list` and `resume` — crash-safe checkpoint/resume and optional
> globally sorted Parquet included. On the roadmap: `inspect`, `diff`,
> versioned-bucket listing, and object stores beyond S3.

Accessibility changes:

- Add semantic `header`, `main`, and existing `footer` landmarks.
- Add explicit `:focus-visible` styles for links and the scrollable code block.
- Give the video an accessible label and fallback text/link to the trace report;
  its figcaption and trace-report link contain all essential information, so
  the muted terminal footage is explicitly supplementary.
- Adjust light-theme `--ink-3` from `#7C8781` to `#59635D`, and light-theme
  `--signal` from `#9A6710` to `#875806`. These replacements clear 4.5:1 on
  `--paper`, `--paper-2`, and the darker `--paper-3` hover surface (minimum
  measured ratios 4.66:1 and 4.57:1). Adjust dark-theme `--ink-3` from
  `#74807A` to `#829089` so it also clears every dark surface (minimum 4.60:1
  on dark `--paper-3`); dark `--signal` already clears all three.

## README changes

Order the first read as:

1. badge, name, one-sentence definition;
2. concise “when to use it / use Inventory instead” paragraph;
3. Quickstart using the canonical public demo;
4. Parquet-output variant with the documented container-user fix;
5. resume command and expected output shape;
6. demo GIF as evidence;
7. two-paragraph mechanism explanation and field-guide link;
8. factual shipped/roadmap statement;
9. detailed behaviour, replay server, docs, and internals.

The Parquet variant must avoid the known Linux permissions trap:

```bash
mkdir -p out
docker run --rm -t --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/estofs.20210101/ \
  --region us-east-1 --no-sign-request \
  --format parquet -o /out/data
```

Clarify two current inconsistencies:

- Replace “No prefix hints, no pre-pass” with “No user-supplied prefix hints
  and no full listing pre-pass”; Swath does use one bounded delimiter probe.
- Replace “published scale evidence is still thin” with the precise limitation:
  one published large recorded run is mechanism evidence, not a comparative
  benchmark or broad performance characterization.

Do not duplicate the full field guide in the README.

## Field guide changes

Preserve the measured-run question, metrics, technical narrative, provenance,
collapsible contents, glossary, and candid limits.

In the existing “what Swath is” callout, add a direct route to the install and
quickstart guide. Add **Use it — install & quickstart** to the hero action row.
Remove version-status framing from the footer and use the same factual
shipped/roadmap sentence as the homepage.

Wrap the technical sections in a `main` landmark and add
`summary:focus-visible` styling. Do not turn the field guide into another
installation page.

## Varve handoff

The Varve homepage continues to link to the Swath homepage, not directly to the
field guide or recorded run. Once the Swath homepage gains the verified first
run and clearer routing, it serves technical leaders and practitioners without
duplicating those choices on Varve.

## Acceptance criteria

- A first-time visitor can state what Swath does, when to use it, and when to use
  Inventory instead from the Swath homepage or README before learning its
  partitioning algorithm.
- The canonical public-demo command is identical on the homepage and README.
- The demo target, region, anonymous access, approximate object count, and
  relationship to the recorded run are factual and re-verified at implementation.
- The first command requires Docker but no AWS credentials, local Swath/JDK
  installation, or writable bind mount; it supplies the bucket region
  explicitly and does not flood the terminal with object rows.
- The Parquet Docker command runs as the host user and writes inside the mounted
  directory.
- Homepage, README, and field guide contain no public “pre-1.0” framing.
- Roadmap copy does not imply unshipped commands are available.
- The homepage has clear semantic landmarks and visible keyboard focus.
- The homepage remains usable without JavaScript; core content and the command
  are plain HTML.
- The field guide preserves its full technical content and adds no more than a
  small use-path callout/action plus footer/accessibility edits.
- The existing mobile layouts do not acquire page-level horizontal overflow;
  intentionally wide field-guide figures remain locally scrollable.
- Links between Varve, Swath, quickstart, field guide, and recorded run form a
  complete path with no dead end above the fold.
- All new external links resolve, and the recorded-run report retains a
  discoverable route back to the Swath homepage or field guide.

## MVP and deferrals

MVP edits the Varve prototype, source README and install guide, public homepage,
and public field guide. The public HTML pages are prototyped from the `gh-pages`
branch without publishing them.

Defer a field-guide three-minute reading path, regenerated social-card art, and
captions/transcript work unless review finds the muted terminal video contains
unique essential information not covered by its caption and trace report.
