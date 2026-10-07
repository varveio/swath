# Java conditional-range input spike

This bounded experiment adapts provider SDK range requests to Parquet's `InputFile` and
uses swath's actual sorted row-group reader. It does not implement remote replay routing,
dataset discovery, concurrent planning, remote output, or a supported CLI option.

For a source-hashed final run, use `python3 tools/java-remote-spike/run_probe.py --out /absolute/output/root`; it retains hashes before/after, SDK versions, fixture/jar identity and the exact measured operations.

The default run creates a deterministic 20,000-object local Parquet fixture (13 canonical
fields, nullable ETags/owners, 128-row pages, one row group) and serves its exact bytes
through a fault-injecting JDK HTTP server. Real SDK clients contact that server:

- AWS SDK v2 S3 `2.31.78`, supplied by the existing swath fat jar; HEAD captures length,
  ETag and optional version, and each GET carries `Range` plus `If-Match`.
- Official Google generated JSON Storage API
  `v1-rev20260524-2.0.0`, with Google HTTP client/Apache transport `2.2.0`.
  Object metadata captures length/generation; media GETs carry `Range`,
  `ifGenerationMatch`, and `Accept-Encoding: identity`. This is the official JSON API SDK,
  **not** `google-cloud-storage`'s higher-level `ReadChannel`.

Both sources validate status, exact `Content-Range`, declared length, returned identity,
encoding and actual complete body. A failed fetch never enters the input's window.
The adapter retains one 64-KiB demand window per stream and serializes provider reads.
The caller owns the source/client; Parquet owns the streams it opens. Closing the source
aborts its owned HTTP pool. This is an experiment in ownership, not a recommendation to
create one cloud SDK client for every inventory part in a production service.

## Run

From the swath root, with Java 25 configured:

```sh
./gradlew :swath-cli:shadowJar
./gradlew -p tools/java-remote-spike \
  --project-cache-dir /tmp/java-remote-spike/gradle-cache runProbe
```

Build outputs go under `/tmp/java-remote-spike/build`. Durable default output goes to
`/workspaces/swath-data/java-storage-spike-20261007/remote`, containing the fixture,
`summary.json` and server-side `requests.json`. Register the root in Varve after its first successful run. Override the output using `-PoutputRoot=/absolute/path` when appropriate, then
register a newly meaningful output root under the storage rules.

For an **owned disposable local S3 emulator**, create the server with fake credentials
`swathspike` / `swathspike-secret`, then run:

```sh
./gradlew -p tools/java-remote-spike \
  --project-cache-dir /tmp/java-remote-spike/gradle-cache \
  s3Smoke -Pendpoint=http://127.0.0.1:4569
```

The smoke mutates `bucket/file` in that emulator, verifies an exact 37-byte range, replaces
the object, and requires the previously captured input identity to fail with HTTP 412.
Only loopback endpoints are accepted. Parent-run evidence uses LocalStack `4.8.1`;
MinIO image pulls were denied by registries. Neither run is live S3/GCS authentication,
service availability or cloud-throughput evidence.

## Evidence and limits

The protocol suite compares every field/null of all 20,000 rows through the full 13-field
Parquet decoder. It separately checks swath's existing indexed key cursor and every field
of its nine-field `ObjectRow` projection against local reads, including owner omission,
exclusive/empty bounds and a page-crossing limit. The sorted object projection intentionally
does not expose the four version/row-type fields; full decode covers those physical columns.

Tests reject replacement preconditions, 404, ignored ranges, wrong range offsets, changed
ETag/generation, truncated bodies, wrong declared lengths, encoded bodies, and changed S3
version IDs. 404/412 require exactly one server request. Successful reads after each error
prove no failed buffer survives. Replacement after footer initialization must fail before
an indexed result is returned. EOF, zero-length reads, direct ByteBuffers, backward seeks,
invalid bounds/overflow, and objects shorter than the Parquet footer are covered.

Cancellation stalls both before headers and inside a body, closes the input, joins the
request worker, and asserts completion within two seconds. A successor source then reads
successfully. With the initial Google `NetHttpTransport`, body cancellation took the five-
second socket timeout; switching to an owned `ApacheHttpTransport` and closing its pool
made cancellation immediate in this local test. This does not yet establish request-scoped
cancellation with a shared production SDK pool or reader lease reuse.

In the initial measured fixture, both SDK paths made eight GET ranges for full decode
(395,431 bytes) and eight for a ten-key cursor plus ten owner-inclusive objects
(356,744 bytes), against a 468,551-byte file. These are exact server payload counts, excluding
metadata, headers, authentication and retries. Full decode skips physical bytes it does not
need; repeated/overlapping ranges mean GET bytes are not unique bytes. The bounded query
still fetched roughly 76% of this small file. Dictionaries, indexes and the naive 64-KiB
window are plausible causes; this spike does not attribute fetched bytes to those causes. This proves conditional indexed reading, **not** Varve-level remote
request efficiency. A footer/index planner and measured coalescing remain useful Java work.

The production seam is a reader constructor accepting `InputFile` and a diagnostic `Path`.
That path is a synthetic local label only; converting a real `s3://` or `gs://` key to `Path`
can normalize important repeated slashes. A production input owner needs an exact location
label, shared SDK ownership, authentication, typed storage errors, retry policy, routing,
dataset discovery and request cancellation. Hadoop dependencies are unchanged by this spike.
