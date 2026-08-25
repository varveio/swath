# Documentation style and terminology

This guide keeps swath's public writing consistent without flattening the technical
detail that makes the project useful.

## Names and voice

- Write the project name as **swath** in lowercase everywhere, including in headings and
  at the beginning of a sentence.
- Use code formatting only for literal executables, commands, flags, paths, package names,
  and artifact identifiers: `swath`, `swath list`, `--sort`, and
  `ghcr.io/varveio/swath`.
- Use plain swath when referring to the project or tool in ordinary prose.
- Use US English in first-party documentation: `behavior`, `color`, `analyze`.
- Prefer direct verbs—list, write, resume, query, sort, inspect—over abstract nouns such
  as surface, disposition, or seam in user-facing pages.
- State the action and consequence before implementation rationale.

Contributor specifications can use exact internal terminology when the term has a defined
contract. User guides should introduce an internal term only when the user must recognize
it in a command, file, report, or error message.

## Core terms

### Object, key, and prefix

Use **object** for an S3 object and **key** for its byte-exact name. Do not use **file** as
a synonym in technical claims. A broad introduction may say “objects—the files stored in
the bucket” once.

Use **prefix** for a key prefix. Directory or folder language is acceptable only when
describing a user interface or a dataset's physical output directory.

### Live listing, result, and snapshot

swath performs a **live listing** and publishes a **listing result** or **inventory**.

Do not call the result a point-in-time snapshot. Use this standard qualification when
consistency matters:

> swath publishes the complete result of the live listing it performed. It is not a
> point-in-time snapshot of a bucket that changes during the run.

`_SUCCESS` means the result was completely published; it does not add transactional
snapshot semantics.

### Managed Parquet dataset

On first use in a beginner or workflow page, define it:

> A managed Parquet dataset is a directory of Parquet parts plus swath's manifest,
> completion marker, run report, and temporary resume state.

After that, **managed dataset** is enough when the format is clear.

### Supported and experimental

Use these labels consistently:

- **Supported:** expected to work within the documented scope and covered by the normal
  compatibility gates.
- **Experimental:** implemented but not covered to the same stability or conformance
  level; behavior may change.
- **Diagnostic:** intended for controlled measurement or investigation, not ordinary
  configuration.
- **Planned:** not implemented. Planned controls must not be presented as selectable
  current behavior.

S3 general-purpose buckets are supported. GCS through its XML API is experimental
S3-compatible access, not a native GCS backend.

## Evidence and performance claims

Every prominent measured run should identify as much of the following as is available:

- swath version and commit;
- date;
- target bucket or fixture;
- client machine and region;
- object-store region;
- full command;
- output mode;
- clock being reported; and
- machine-readable report or trace.

Label an observed number as an observation, not a portable limit or promise. Prefer:

> This run reached 655,346 keys/s on the stated machine and bucket.

over:

> swath lists at 655,346 keys/s.

When provenance is incomplete, say so rather than inferring it.

## Information ownership

Give each detailed fact one canonical owner. Other pages may summarize it in one or two
sentences and link to that owner.

| Fact | Owner |
| --- | --- |
| Product promise and fit | root `README.md` |
| First successful workflow | `docs/getting-started.md` |
| Output and resume choices | `docs/usage.md` |
| Credentials, IAM, endpoints, and cost | `docs/operating.md` |
| Visible options and defaults | installed CLI help |
| Expert and diagnostic controls | `docs/configuration.md` |
| Performance interpretation | `docs/performance.md` |
| Metrics and report fields | `docs/metrics-and-observability.md` |
| Current engine behavior | `docs/internals/algorithms.md` |
| Invariants and schemas | `docs/internals/contracts.md` |
| Dated raw evidence | `docs/ops/dev/field-investigations.md` |

Do not copy a detailed table into several pages. Summarize and link.

## Editing discipline

- Put a successful copy-pasteable command before architecture, tuning, or benchmarks.
- Keep a decision-critical warning beside the action it affects.
- Remove stale rollout history, review narration, and rejected alternatives from current
  guides; preserve durable rationale in an ADR, issue, or dated investigation.
- Avoid repeated declarations that a page is exact, canonical, authoritative, honest, or
  deliberate. Demonstrate rigor with clear ownership, tests, evidence, and current links.
- Prefer one short caveat over several overlapping qualifications.
- When a concept needs several paragraphs merely to explain a surprising CLI behavior,
  consider changing the interface before adding more prose.
