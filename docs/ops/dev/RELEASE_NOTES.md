# swath 0.2.1

## User-facing changes

- `swath --version` now identifies the build, source revision, Java runtime, and Varve;
  root help carries the same unobtrusive Varve attribution.
- Dataset startup and resume now refuse pre-existing symlinks in managed output,
  checkpoint, marker, and sorted-staging paths before swath can truncate or overwrite an
  external target. Existing valid datasets need no migration.
- Release downloads and images are now bound to the exact tagged source. The publication
  path runs fresh fast, integration, deep, kill-9 resume, licence, and instrumentation
  gates; stages an exact asset set; and verifies checksums, signatures, attestations, and
  published assets before making the GitHub release public.
- Installation and packaging documentation now describe the released artifacts and
  container images directly instead of the retired pre-release workflow.

## Evidence

- Ten adversarial CLI tests cover dataset-root, checkpoint, SQLite sidecar, marker-temp,
  markerless part-file, explicit-restore, and deterministic sort-segment symlinks while
  asserting that external targets remain unchanged.
- The merged change passed the full local `./gradlew build` gate, independent CodeRabbit
  and Opus reviews, and `main` CI's fast, integration, deep, Docker, and CodeQL checks.
- The release workflow independently rebuilds and retests the immutable tag before any
  publication step, then self-verifies the resulting downloads and container digest.

## Limits and known issues

- Filesystem protection covers links that exist when a command validates its dataset; it
  does not claim to defend a writable output directory against hostile concurrent
  mutation during the run.
- This is the first release through the newly hardened publication workflow.
