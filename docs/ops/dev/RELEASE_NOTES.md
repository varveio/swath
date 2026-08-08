# swath 0.2.2

## User-facing changes

- Sorted listings now merge staged page-runs in parallel by default once staged output
  passes 256 MiB, using a processor-derived fan-in (`max(1, min(8, processors / 2))`)
  instead of the prior serial-only final merge. Output ordering, file indexes, and
  completeness stamps are unchanged; runs still fall back to serial automatically when
  staged size, fan-in, heap budget, file descriptors, or keyspace shape can't safely carry
  parallel work.
- Output failures caused by a full disk now exit with a dedicated code, `74`
  (`EX_IOERR`), instead of the generic `1`, and the terminal error line now includes the
  full cause chain (e.g. `swath: parquet writer failed: No space left on device`) instead
  of a bare stage name. Every other output failure keeps exit code `1` and its prior
  message shape.
- Added a trace explainer (`tools/explainer/`) that turns a run's `--trace` event log into
  a self-contained HTML report — replay, keyspace map, seed distribution, mechanism
  ledgers — plus an optional narrated video cut. Published output is served from
  `swath.varve.io`.
- The README now demos a real 39.6-million-object interrupt/resume/DuckDB-query session
  with an autoplaying GIF alongside the existing MP4.

## Evidence

- Parallel merge default-on: a focused live-S3 gate on `pds-css-archive` found zero
  full-row mismatches across a bidirectional `EXCEPT ALL`, zero physical-ordering
  regressions, and 8 effective merge ranges with no clamp, cascade, cancellation, or FD
  signal. Merge time fell from a 139.5s serial mean to 38.4s (3.63x); full session wall
  from 300.5s to 194.9s. A corroborating scale sweep (GEFS/PDS/MRMS, 2-128 segments) showed
  2.81x-3.97x merge speedups. `./gradlew build` (3,681 tests), the integration tier (15
  tests), and the deep tier (45 tests) were all green on the tested SHA.
- Disk-full classification: a 12-case test suite covers the exit code end-to-end through
  the CLI entry point (direct cause, wrapped cause chains, negative cases) and the message
  path (cause carried through, single-line output, causeless errors unchanged,
  duplicate-message dedupe, cyclic-chain termination).
- Trace explainer: a 29-check self-test passes with 0 failures. Building it surfaced and
  fixed two real bugs: an unescaped-JSON-key script-injection hole in the embedded report
  model, and an incorrect split-topology replay that had misattributed ancestry for 229 of
  2,399 split children in the reference trace.
- All four changes passed CI's fast, integration, deep, Docker, and CodeQL tiers, plus
  independent review, before merging to `main`.

## Limits and known issues

- The parallel-merge default-on decision rests on a focused gate against one workload
  family (PDS) plus a corroborating scale sweep; it is not an exhaustive matrix across
  bucket shapes, and MRMS physical ordering specifically was not evaluated as part of the
  focused gate.
- The focused merge gate ran on a single Hyperdisk Balanced persistent disk rather than
  local SSD; speedup ratios are meaningful, but absolute times carry that storage caveat.
- The explainer's `--video-style map` cut still uses an earlier dark palette and needs
  re-lighting before it's used for anything published.
