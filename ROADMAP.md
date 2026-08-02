# Roadmap

Deliberately deferred work, in rough priority order. Items here are intents, not
commitments; issues track the ones that are actionable today.

## Planned features

- **`swath inspect`.** Probe and report a bucket's shape/strategy (dense vs.
  sparse, prefix structure, estimated scale) without listing it — a
  cheap-first-look companion to `swath list`. Not yet implemented.
- **Versioned listing (`--all-versions`).** Emit every object version, not
  just the current one: `version_id`, `is_latest`, and `is_delete_marker` /
  `row_type=DELETE_MARKER` rows are reserved columns in the Parquet schema
  today but are not populated in v1.0.
- **`hints` seed mode.** `--tune seed.mode=hints` is reserved in the CLI but
  not yet implemented; it would let a caller pre-supply keyspace structure
  instead of paying for the `shallow` discovery pass.
- **Diff two buckets or prefixes.** `swath diff s3://A/prefix s3://B/prefix`
  streams both sides in byte order — one engine per side yields a globally
  sorted stream — and runs a two-pointer merge-join that emits
  `LEFT_ONLY / RIGHT_ONLY / mismatched`. The subtlety is producing each side's
  *global* order out of its parallel work-stealing ranges: a side emits its
  completed ranges in `range_start` order behind a bounded per-side reorder
  buffer, so a stalled lower range applies backpressure rather than letting a
  higher range emit out of order. Memory stays `O(buffer + active_ranges)`. Not yet
  implemented; it is planned as restartable only — `swath resume` is out of scope
  for it in v1.
- **Express One Zone (directory-bucket) listing.** Directory buckets (name suffix
  `--x-s3`) do not expose a single global lexicographic order, so range-splitting
  is unsafe. Current releases fail closed on that suffix before checkpoint creation or the
  first LIST request. The planned treatment is an opaque-continuation-token
  sequential path (one `(⊥, null]` worker, no stealing); the engine-side design
  is in [`docs/internals/algorithms.md`](docs/internals/algorithms.md) §10.

## Post-v1 candidates

- **Null-safety annotations (JSpecify + NullAway).** Adopt a checked nullness
  story across the modules. Deferred so the v1 restructuring could land without
  fighting an annotation migration at the same time.
- **Embeddable library entry point.** v1 is CLI-only: `swath-core` has no
  supported public API surface beyond what the CLI exercises. A curated library
  facade (stable entry points, semver discipline for the core types) is a
  follow-up once the internal shape has settled in the open.
- **Parallel sort-merge promotion.** The range-partitioned parallel merge
  (`merge-parallelism > 1`) ships off-by-default. Promoting it involves closing
  the completeness-stamp gap on its output (the design options are still open)
  and a broader throughput/memory characterization.
- **Resume retargeting.** `swath resume` deliberately does not allow changing
  the output destination (destination is identity). A retarget-that-rewrites-
  identity flow may return alongside a remote-output regime.
- **Additional backends and a server mode.** The module layout reserves
  function-keyed names (`swath-gcs`, `swath-server`) for a GCS driver and a
  long-running service. The current Java seams are internal and unsupported;
  range-engine reuse additionally requires global lexical ordering and a
  client-chosen lower bound equivalent to `StartAfter`.
