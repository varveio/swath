# swath 0.4.0-rc.1

<!-- Links are absolute and pinned to the tag: the release body does not resolve repository-relative links. -->

## User-facing changes

This is the first release candidate of 0.4.0, the first release of the Go implementation. Go replaces
the Java tool: v0.3.2 was the last Java release, and no later release ships Java.

- **Sources.** Local and mounted filesystems, Amazon S3 general-purpose buckets, Google Cloud Storage
  (native `gs://` through the JSON API) and Azure Blob Storage flat namespaces (`az://`). Each source's
  limits are in the [README](https://github.com/varveio/swath/blob/v0.4.0-rc.1/README.md#supported-sources).
- **Commands.** `swath list` writes text rows or a sorted Parquet dataset, `swath du` totals a source per
  directory or prefix, and `swath resume` continues an eligible object-store dataset run. `swath-replay`,
  a contributor tool for testing listers, serves a recorded listing back through the S3, GCS and Azure
  listing APIs. The
  [Go guide](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/guide.md) is the starting point.
- **Sorted output while listing.** Every dataset is globally key-sorted, with no merge pass at the end:
  rows listed ahead of the output wait in memory and then in spill files under the output directory.
- **Downloads.** One archive per target (`linux-amd64`, `linux-arm64`, `darwin-arm64`, `windows-amd64`),
  each holding static `swath` and `swath-replay` binaries with `LICENSE`, `NOTICE` and
  `THIRD_PARTY_NOTICES` (the licenses of every module linked into the binaries); an SPDX SBOM;
  `SHA256SUMS`; a keyless cosign signature for each; and build provenance attestations.
- **Images.** `ghcr.io/varveio/swath:0.4.0-rc.1` and `ghcr.io/varveio/swath-replay:0.4.0-rc.1` for
  linux/amd64 and linux/arm64, carrying the archived linux binaries, signed and attested. A release
  candidate publishes only its own tag: no `latest` and no `0.4`.
- **Verifying.** The checksum, signature and attestation commands are in
  [RELEASING.md](https://github.com/varveio/swath/blob/v0.4.0-rc.1/RELEASING.md#verifying-a-release); the
  archive and image contents are in
  [operating.md](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/operating.md#release-downloads-and-images).

### Breaking changes from 0.3.2

- **Launcher.** The Java jar (`java -jar swath.jar`) and its `.zip` and `.tar.gz` distributions are
  replaced by a native binary per platform; no JRE is needed. `--version` prints `swath-go 0.4.0-rc.1`
  and the commit.
- **Command line.** Flags are renamed or removed, without aliases (an unknown flag exits 2). The most
  common: `--concurrency` is `--workers`, `--no-sign-request` is `--anonymous`, `--endpoint-url` is
  `--endpoint`, `--force` is `--overwrite`, `--output` is `-o`, `--parquet-part-size` is `--part-size`,
  `--format parquet --sort -o DIR` is `-o DIR`, and `--format discard` is `swath diag count`. Unsorted
  Parquet, the SQLite checkpoint, OTLP metrics, partitioned text datasets and `--tune` are gone. Filter
  regular expressions are Go's RE2. Text rows use the S3 API's field names (`Key`, `LastModified`, `ETag`).
  Exit codes keep their meaning. Every option, with its counterpart and the reason, is in
  [from-java.md](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/from-java.md) and
  [cli-inventory.md](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/cli-inventory.md).
- **Dataset layout.** A dataset is a swath dataset in the SeekParquet format (`format_version` 0,
  `draft_revision` 1) instead of the Java format 1: `manifest.json`, installed last, is the commit
  (`_SUCCESS` and `symlink.txt` are gone), `run.json` replaces `_swath_summary.json`, `swath/canonical`
  becomes `swath/s3-objects/v1` without the `version_id`, `is_latest`, `is_delete_marker` and `row_type`
  columns, and the local schema's columns are renamed. A Java reader does not read the new format and
  `swath resume` does not read a Java run: finish a Java run with Java or start over. The field-by-field
  mapping is in [upgrade.md](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/upgrade.md).
- **Endpoints.** S3 and GCS endpoints with a query, a fragment or user information are refused (exit 2);
  pass credentials through the provider's credential settings.
- **Images.** The lister image is distroless static instead of a Temurin JRE on Ubuntu: the entrypoint is
  `/opt/swath/swath` (was `java -jar /opt/swath/swath.jar`), and there is no shell or JVM. The replay
  image is `scratch`, with the entrypoint `/opt/swath-replay/swath-replay` (was
  `/opt/swath-replay/bin/swath-replay`). The legal files moved from `/opt/swath/` to `/licenses/`. Both
  still run as `10001:10001`.
- **Builds from `main` before 0.4.** The Go port's own pre-release flag renames are listed under
  [Changes from 0.3 to 0.4](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/upgrade.md#changes-from-03-to-04).

## Evidence

- The tagged commit passed `ci.yml` on its pull request and again on `main`: `go vet`, the race tests in
  shards on amd64 and arm64, the default tier on macOS (on `main`) and Windows, the nested modules,
  SeekParquet conformance, the replay fixture tier, and both images built for two architectures and smoked
  offline
  ([go/docs/testing.md](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/testing.md#continuous-integration)).
- The release workflow then reran `go vet` and the whole race suite unsharded on the tag, built the four
  targets once with the tag's version and commit checked in each linux/amd64 `--version`, and smoked the
  pushed image digests before tagging them: an anonymous listing of a public S3 prefix into a dataset,
  read back with DuckDB from the manifest's part list, resumed as a no-op, and served by the replay image.
  It verified its own signatures and attestations against the published assets before leaving draft.
- Every module linked into the binaries passed the license gate that generated `THIRD_PARTY_NOTICES`
  (permissive licenses plus EPL and CDDL; no GPL, LGPL or AGPL).
- Dated measurements of the Go tool, each with its configuration and harness, are in
  [measurements.md](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/measurements.md). None was
  taken on this tag.

## Limits and known issues

- This is a release candidate: it rehearses the publish path, and it is not promoted. Its binaries report
  `0.4.0-rc.1`; 0.4.0 will be a separate build.
- Windows refuses journaled object-store runs and `swath resume` (directory sync is unavailable there).
  Local scans and text output do not resume on any platform
  ([known limits](https://github.com/varveio/swath/blob/v0.4.0-rc.1/go/docs/operating.md#known-limits)).
- Not listed: Azure hierarchical namespaces, blob versions, snapshots and deleted blobs; S3 directory
  buckets; GCS soft-deleted objects. Mounted shares are read through the operating system, not a native
  NFS or SMB client.
- Archives are built for the four targets above only; images are linux only.
- The dataset format is SeekParquet draft revision 1.
- `swath-replay` reads Java-era fixtures through a transitional reader that will be removed; re-list a
  Java-era fixture you still need.
- swath publishes the complete result of the live listing it performed. It is not a point-in-time
  snapshot of a bucket that changes during the run.
