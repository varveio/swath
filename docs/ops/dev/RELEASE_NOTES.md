# swath 0.3.1

## User-facing changes

- **`swath-replay` answers wide `delimiter=/` rollups through the Parquet page index.** A
  common-prefix successor hop that stayed inside one physical row group used to keep its
  forward key cursor, so the server decoded every key in the subtree it was meant to skip;
  wide directory probes over a skewed fixture became CPU-bound inside boundary row groups.
  The skip-scan now reopens the cursor through the Parquet page index when the successor
  provably lies beyond the current data page, and keeps the forward cursor when the page's
  own maximum says the target can still occur on it — so a huge skipped subtree costs one
  page landing while dense, tiny directories are not re-decoded page by page. Responses are
  byte-identical to the previous walk; only the work changes. In the recorded local
  isolation run over a 143,008,674-row fixture, the widest directory probe moved from
  392.7 ms to 22.4 ms mean and its continuation request from 63.6 ms to 7.0 ms.
- **New replay meters expose the skip-scan's work.**
  `swath.replay.delimiter.skipscan.decoded_key_rows` and `.page_reseeks` record, per native
  rollup, the key rows its cursors decoded and the same-row-group page reseeks it took —
  distinguishing page-bounded landing work from a sequential subtree walk.
  `swath.replay.delimiter.reader_pool.readers_opened` records how many reader handles a
  file's lazy delimiter pool opened on first touch, isolating cold pool construction from
  query time. `swath.replay.delimiter.skipscan.row_group_opens` now counts cursor opens as
  operations: after a reseek the same physical row group can be opened more than once.
- **Sorted-serving order checking has its scope stated where it is enforced.** Startup
  eligibility proves the ascent of row-group first keys; request-time cursors additionally
  check every row they decode and refuse with
  `swath.replay.serving.refused{reason=row_group_disorder}` on an inversion. Page-index and
  routing-index shortcuts deliberately leave other rows unread, so that refusal is a
  fail-stop guard on the request path, not an exhaustive validation of a malformed fixture.
  This was already the behavior; the reader javadoc, `docs/swath-replay.md`, and the
  metrics reference now say it plainly, and tests pin the contract from both sides.
- **The `swath` CLI is unchanged.** The only change under `swath-core` is a passive
  decoded-row counter in the shared sorted-Parquet key cursor; listing, output, resume, and
  sorted finalization behavior are identical to 0.3.0.

## Evidence

- PR #194 carries the change, a one-row-group adversarial regression (16 prefixes ×
  1,000 keys must answer from page landings, not subtree decodes), a dense-singleton-prefix
  test proving the hybrid keeps its forward cursor on a shared page with zero reseeks, a
  successor/resume-boundary parity test against an independent in-memory rollup — including
  a bare object exactly equal to `successor(P)` — and a disorder-contract pair: an
  inversion wholly inside a pruned page is skipped with a correct answer and no refusal,
  while an inversion on a decoded landing page still refuses with `row_group_disorder`.
- The recorded local isolation run (4-core/8-thread host, `-Xmx4g`, warm filesystem cache,
  eight readers, eight requests in flight) over a four-part, 143,008,674-row,
  2.96 GiB AWS Public Blockchain fixture measured 14 request shapes; every baseline and
  candidate response was byte-identical by SHA-256, the three high-subtree shapes improved
  7.1–21.8× in mean latency, and the full method and table are retained in
  `docs/ops/dev/field-investigations.md` (2026-09-01 entry).
- The full sorted-vs-DuckDB differential and conformance suites passed on the change, and
  the repository `build -PnoIntegration` gate was run on the merged head.

## Limits and known issues

- The recorded latencies are a local warm-cache isolation run at one sample per
  configuration on one host and one fixture — an implementation-path check, not a
  production S3 latency model or a portable throughput claim. Small already-cheap shapes
  moved by single-digit milliseconds in both directions at that sample size; the
  dense-small-directory claim is carried by unit coverage rather than that run.
- Sorted serving continues to require a correctly stamped, sorted fixture. Order checking
  covers decoded rows only; a malformed fixture whose disorder sits entirely inside skipped
  pages can serve answers those page bounds make correct rather than being detected.
  Re-sort a suspect capture or serve it with DuckDB mode.
- Cold first-request cost on a large fixture still includes synchronously opening the
  lazy per-file delimiter reader pool and first-use page/index warming; this release does
  not redesign pool initialization.
- Everything in the 0.3.0 notes' limits — resume scope, sorted staging portability, the
  report `sort`-block carve-out, supported backends, and live-listing consistency — is
  unchanged.
