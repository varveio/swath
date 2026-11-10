# Dependencies and what is built here

What the Go port takes from modules, what it builds on the standard library instead, and the rule for adding a
module. The rule behind both lists: use a maintained package where it does the job as well; build on the
standard library only where the package is weaker on the property that matters (directory fsync, the gauge
rule, classified retry) or heavier than the code it would replace.

## Direct modules

Linked into `cmd/swath` unless marked tests.

| Module | License | Why |
|---|---|---|
| `github.com/aws/aws-sdk-go-v2` (`aws`, `config`) | Apache-2.0 | The S3 credential chain (environment, profiles, SSO, web identity, roles, IMDS) and SigV4 signing, for the S3 list client ([clients.md](clients.md)). |
| `github.com/aws/aws-sdk-go-v2/service/s3` | Apache-2.0 | The S3 list client's request path: ListObjectsV2 and ListObjectVersions go through the SDK's service client, its retryer off; swath's scanner decodes the pages ([clients.md](clients.md)). Tests also use a stock client as the decoder oracle. |
| `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` (v1.8.3), `.../sdk/azcore` | MIT | The Azure Blob list client's request path: List Blobs and Get Account Information through azblob's service and container clients, Shared Key signing, azcore's pipeline with its retries off ([clients.md](clients.md)). Tests: the paginators also consume the replay's Azure route (`internal/replay/azure`). Every listing asks for the Arrow listing format, then XML; the SDK decodes either ([clients.md](clients.md#azure-decoding-and-requests)). |
| `github.com/apache/arrow-go/v18` | Apache-2.0 | Required by azblob's Arrow listing decoder; swath's own use is its IPC stream reader, with which the Guard checks an Arrow listing's envelope (stream end, schema metadata, row count) before the SDK decodes it ([clients.md](clients.md#azure-decoding-and-requests)). |
| `github.com/Azure/azure-sdk-for-go/sdk/azidentity` (v1.15.0), `.../sdk/azcore` | MIT | The Azure Blob list client's Entra ID credentials: `DefaultAzureCredential` (environment, workload identity, managed identity, the Azure CLI) and the `TokenCredential` type; tokens are fetched by swath before each attempt and sent as a header ([clients.md](clients.md)). |
| `github.com/aws/smithy-go` | Apache-2.0 | The SDK's `APIError`, middleware and logger types: classifying failures, swath's User-Agent token and the minimal envelope the SDK decodes in the scanner's place, and a quiet logger. |
| `cloud.google.com/go/compute/metadata` | Apache-2.0 | The GCE metadata server's typed error, to tell a transient metadata outage from a refusal. |
| `golang.org/x/oauth2` (`google`) | BSD-3 | Google application-default credentials and token refresh for GCS; bearer-token reuse (`internal/source/tokensource`). |
| `golang.org/x/sync` (`singleflight`) | BSD-3 | One token refresh shared by concurrent callers (`tokensource`: S3 bearer tokens, GCS credentials and Azure Entra tokens). |
| `golang.org/x/sys` | BSD-3 | Linux `getdents64` and `statx` for the filesystem lister; Windows `LockFileEx` (the output lock), console VT mode, `GetDiskFreeSpaceEx` (free space for the spill reserve) and the closed-pipe and sharing-violation errors. |
| `github.com/spf13/pflag` | BSD-3 | The CLI's flag parser (`cmd/swath`): GNU-style long options, `--no-NAME` negations, and the flag table that help and `swath completion` are generated from. |
| `golang.org/x/term` | BSD-3 | Whether stderr is a terminal, and its width, for the progress line. |
| `github.com/klauspost/compress` (`zstd`, `gzip`) | BSD-3 | zstd for Parquet pages and spill frames, gzip and zstd for text output; the standard library has no zstd, and the same module's gzip is a drop-in for `compress/gzip`. |
| `github.com/varveio/seekparquet/go` | Apache-2.0 | The SeekParquet format's mechanisms: the part writer (`write`), the manifest's JSON, footer and key-page inspection, and the reader `swath-replay` serves datasets through. Pinned at a pseudo-version of its `main` (bumped deliberately) until it releases. |
| `github.com/varveio/swath/go/reader` | Apache-2.0 | This repository's nested module (`go/reader`, `replace`d to `./reader`): the schema ids and their columns, which the producers' schemas bind to (`schema.Bind`), so writer and readers share one description. |
| `github.com/parquet-go/parquet-go` | Apache-2.0 | The Parquet writer and reader. It links protobuf, geometry, brotli and lz4 code the port never calls; see the triggers below. |
| `google.golang.org/api` (`storage/v1`, `option`) | BSD-3 | The GCS list client's request path and decoder: Google's generated JSON API client, under swath's envelope checks ([clients.md](clients.md)). It links gRPC and OpenTelemetry packages it does not call for listing ([measurements](measurements.md#sdk-backed-clients-2026-11-09)). |
| `github.com/johannesboyne/gofakes3` | MIT | Tests and `bench/s3fake`: an in-process S3 server. |

## Built here, and the package considered

| Component | Where | Considered | Why not adopted |
|---|---|---|---|
| Journal | `internal/journal` | tidwall/wal, bbolt, SQLite (modernc) | None covers the part files, renames, directory syncs, the lock, or telling a torn tail from corruption; they would add a storage engine and keep all of that. |
| Output lock | `internal/journal` (`lock_*.go`) | gofrs/flock | The same syscall (`flock`, `LockFileEx` from `x/sys`) on the file the journal already holds; a package adds a module and no identity check. |
| Atomic publication | `internal/dataset` (`Replace`, `PublishPart`) | google/renameio, natefinch/atomic | Neither syncs the directory after the rename; renameio has no Windows build. |
| Retry and classification | `internal/source/objhttp` | cenkalti/backoff, hashicorp/go-retryablehttp | The substance is the classification and the admission of attempts, not the loop; a second retry loop would change request counts and timing. |
| Adaptive gauge limiter | `objhttp/limiter.go` | `x/sync/semaphore`, generic adaptive limiters | The fraction window and stale-answer rule are listing policies ([request admission](clients.md#request-admission)); a semaphore supplies neither. |
| Fixed request-rate cap | `objhttp/rate.go` | `golang.org/x/time/rate` | Equal code size, and `Wait` fails early on a deadline, which the retry loop would turn into a silently dropped attempt. |
| Spill | `internal/order` | external-sort packages (extsort) | Rows arrive sorted, so there is no merge to buy; the spill shares the memory and disk budgets with the run. |
| Counters | `internal/counters` | `expvar`, VictoriaMetrics metrics, OpenTelemetry | The definition table is the report's schema (units, feature gating, documentation); the packages have neither. |
| Supervisor | `cmd/swath/supervise.go` | `signal.NotifyContext` | The exit-code mapping, the progress watchdog, owned diagnostic writes and the bounded forced exit are the application's; the standard primitives are used underneath. |
| Commands | `cmd/swath` (pflag flag sets) | spf13/cobra | The subcommands, help and shell completion are generated from one flag table over pflag; cobra adds command machinery the CLI does not need. |
| Filesystem lister | `internal/source/localfs` | charlievieth/fastwalk, karrick/godirwalk | They are unsorted, allocate per entry and have no `statx` or arena; the lister sorts before it stats. |
| List clients | `internal/source/s3/client`, `gcs/client`, `azblob/client` | the SDKs' paginators and retryers, `cloud.google.com/go/storage` | Each client uses the SDK's service client; what is built here is the one retry owner and admission, the bounded and validated body (`objhttp.Guard`), S3's page scanner (per page, not per object, and versions in wire order), Azure's merge into wire order and the transport pool ([clients.md](clients.md)). `cloud.google.com/go/storage` adds gRPC transport and retries the port would turn off. |
| Parquet part router | `internal/parquetout` | the seekparquet module's writer alone (`local.WriteDir`) | It is the part-size, interval, resume-cut and encoder-pool policy over the module's part writer (`write.NewPart`), which is the encoder; the journal and publication stay swath's (the module's `local` package has no fault hooks). |

**Triggers to revisit:** a storage need beyond append-and-scan (mutable tables, queries, compaction) or another
change to the journal's recovery format (bbolt); a second subsystem that needs generic retries (a retry
package there); metrics export to an external system (OpenTelemetry); many more commands (cobra); a profile on macOS or Windows where `stat` dominates (a native lister); binary size becoming a gate
(an upstream split or a fork of parquet-go); a Windows crash test (Windows directory durability, then
journaled runs there).

## Adding a module

Before writing a utility, name the standard-library or `golang.org/x` facility considered. A new module's
commit message states:

- its license;
- its maintenance signal (latest release and its date, open issues on the part used);
- its transitive weight (`go list -deps ./cmd/swath | wc -l` before and after);
- the stripped binary's size delta (`./build.sh -o swath` for linux/amd64, before and after).

Removing one records the same counts.
