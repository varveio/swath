# Dependencies and what is built here

What the Go port takes from modules, what it builds on the standard library instead, and the rule for adding a
module. The rule behind both lists: use a maintained package where it does the job as well; build on the
standard library only where the package is weaker on the property that matters (directory fsync, the gauge
rule, classified retry) or heavier than the code it would replace.

## Direct modules

Linked into `cmd/swath` unless marked tests.

| Module | Licence | Why |
|---|---|---|
| `github.com/aws/aws-sdk-go-v2` (`aws`, `config`, `feature/ec2/imds`) | Apache-2.0 | The S3 credential chain (environment, profiles, SSO, web identity, roles, IMDS) and the SigV4 signer; the list requests themselves are ours ([providers.md](providers.md#the-list-clients-hand-rolled-on-the-sdks-auth)). |
| `github.com/aws/aws-sdk-go-v2/service/s3` | Apache-2.0 | Tests: the SDK oracle the S3 list client is compared against (`internal/source/s3/sdkoracle`). |
| `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` (v1.7.0), `.../sdk/azcore` | MIT | Tests: the official Azure SDK's flat and hierarchy paginators consume the replay's Azure route (`internal/replay/azure` `sdk_test.go`, `sdkunit_test.go`); not linked into either binary. v1.7.0 is the newest release that does not require Apache Arrow and newer compression modules than the production build uses. |
| `github.com/aws/smithy-go` | Apache-2.0 | The SDK's `APIError` and logger types, to classify credential failures and keep the SDK's logger quiet; `encoding/httpbinding.EscapePath`, the SigV4 URI encoding of the list query. |
| `cloud.google.com/go/compute/metadata` | Apache-2.0 | The GCE metadata server's typed error, to tell a transient metadata outage from a refusal. |
| `golang.org/x/oauth2` (`google`) | BSD-3 | Google application-default credentials and token refresh for GCS; bearer-token reuse (`internal/source/tokensource`). |
| `golang.org/x/sync` (`singleflight`) | BSD-3 | One token refresh shared by concurrent callers (`tokensource`: S3 bearer tokens and GCS credentials). |
| `golang.org/x/sys` | BSD-3 | Linux `getdents64` and `statx` for the filesystem lister; Windows `LockFileEx` (the output lock), console VT mode, `GetDiskFreeSpaceEx` (free space for the spill reserve) and the closed-pipe and sharing-violation errors. |
| `golang.org/x/term` | BSD-3 | Whether stderr is a terminal, and its width, for the progress line. |
| `github.com/klauspost/compress` (`zstd`, `gzip`) | BSD-3 | zstd for Parquet pages and spill frames, gzip and zstd for text output; the standard library has no zstd, and the same module's gzip is a drop-in for `compress/gzip`. |
| `github.com/varveio/seekparquet/go` | Apache-2.0 | The SeekParquet format's mechanisms: the part writer (`write`), the manifest's JSON, footer and key-page inspection, and the reader `swath-replay` serves datasets through. Pinned at a pseudo-version of its `main` (bumped deliberately) until it releases. |
| `github.com/parquet-go/parquet-go` | Apache-2.0 | The Parquet writer and reader. It links protobuf, geometry, brotli and lz4 code the port never calls; see the triggers below. |
| `google.golang.org/api` | BSD-3 | Tests: Google's generated GCS JSON client, the oracle the GCS list client is compared against. |
| `github.com/johannesboyne/gofakes3` | MIT | Tests and `bench/s3fake`: an in-process S3 server. |

## Built here, and the package considered

| Component | Where | Considered | Why not adopted |
|---|---|---|---|
| Journal | `internal/journal` | tidwall/wal, bbolt, SQLite (modernc) | None covers the part files, renames, directory syncs, the lock, or telling a torn tail from corruption; they would add a storage engine and keep all of that. |
| Output lock | `internal/journal` (`lock_*.go`) | gofrs/flock | The same syscall (`flock`, `LockFileEx` from `x/sys`) on the file the journal already holds; a package adds a module and no identity check. |
| Atomic publication | `internal/dataset` (`Replace`, `PublishPart`) | google/renameio, natefinch/atomic | Neither syncs the directory after the rename; renameio has no Windows build. |
| Retry and classification | `internal/source/objhttp` | cenkalti/backoff, hashicorp/go-retryablehttp | The substance is the classification and the admission of attempts, not the loop; a second retry loop would change request counts and timing. |
| Adaptive gauge limiter | `objhttp/limiter.go` | `x/sync/semaphore`, generic adaptive limiters | It is the Java client's control law; generic limiters implement a different one. |
| Fixed request-rate cap | `objhttp/rate.go` | `golang.org/x/time/rate` | Equal code size, and `Wait` fails early on a deadline, which the retry loop would turn into a silently dropped attempt. |
| Spill | `internal/order` | external-sort packages (extsort) | Rows arrive sorted, so there is no merge to buy; the spill shares the memory and disk budgets with the run. |
| Counters | `internal/counters` | `expvar`, VictoriaMetrics metrics, OpenTelemetry | The definition table is the report's schema (units, feature gating, documentation); the packages have neither. |
| Supervisor | `cmd/swath/supervise.go` | `signal.NotifyContext` | The exit-code mapping, the progress watchdog, owned diagnostic writes and the bounded forced exit are the application's; the standard primitives are used underneath. |
| Flags | `cmd/swath` (standard `flag`) | spf13/cobra and pflag | The subcommands and their Java-compatible spellings fit the standard package; cobra adds command machinery the CLI does not need. |
| Filesystem lister | `internal/source/localfs` | charlievieth/fastwalk, karrick/godirwalk | They are unsorted, allocate per entry and have no `statx` or arena; the lister sorts before it stats. |
| List clients | `internal/source/s3/client`, `gcs/client` | the S3 SDK's paginator, `cloud.google.com/go/storage` | The page scanner allocates around pages and arenas, and the requests go through the port's own retry and admission; the SDKs supply credentials only. |
| Parquet part router | `internal/parquetout` | the seekparquet module's writer alone (`local.WriteDir`) | It is the part-size, interval, resume-cut and encoder-pool policy over the module's part writer (`write.NewPart`), which is the encoder; the journal and publication stay swath's (the module's `local` package has no fault hooks). |

**Triggers to revisit:** a storage need beyond append-and-scan (mutable tables, queries, compaction) or another
change to the journal's recovery format (bbolt); a second subsystem that needs generic retries (a retry
package there); metrics export to an external system (OpenTelemetry); shell completion or many more commands
(cobra); a profile on macOS or Windows where `stat` dominates (a native lister); binary size becoming a gate
(an upstream split or a fork of parquet-go); a Windows crash test (Windows directory durability, then
journaled runs there).

## Adding a module

Before writing a utility, name the standard-library or `golang.org/x` facility considered. A new module's
commit message states:

- its licence;
- its maintenance signal (latest release and its date, open issues on the part used);
- its transitive weight (`go list -deps ./cmd/swath | wc -l` before and after);
- the stripped binary's size delta (`./build.sh -o swath` for linux/amd64, before and after).

Removing one records the same counts.
