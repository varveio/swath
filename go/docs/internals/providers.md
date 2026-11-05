# Providers

## The contract (`internal/source`)

```go
type Emitter[R, P any] interface {
    Rows(rows []schema.Row[P]) error // next rows of this range, ascending; the slice now belongs to the caller
    Child(r R) error                 // a child range whose whole subtree sorts here
}
type Provider[R, P any] interface {
    Root() R
    Expand(ctx context.Context, r R, e Emitter[R, P]) error
}
```

`P` is the provider's row payload (`fs.Entry`, `s3.Object`), bound to columns by its `schema.Schema[P]`, which
lives beside the provider.

A range is a provider-defined value. `Expand` emits its content in ascending key order: every key of a child's
subtree sorts after everything emitted before the child and before everything emitted after it. `Expand` may
call `Rows` many times (streaming) and runs concurrently for different ranges. Per-entry problems (permission
denied, a vanished entry) are counted by the provider; `Expand` returns an error only when the listing must
stop, including any error returned by the emitter (cancellation or a failed sink).

## Filesystem (`internal/source/localfs`)

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCALFS_ENGINE=portable`): `os.File.ReadDir` + `Lstat`, re-checking that an opened child directory is
  the one that was listed.
- **Tiers** (`--metadata`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `birth_time`).
- **Cancellation:** a huge directory stays cancellable. getdents checks the context every 64 reads, the sort
  is bracketed by checks, the portable engine checks every 1,024 entries, and stat checks as before.
- Symlinks are never followed (`O_NOFOLLOW`). Names are bytes: one that is not valid UTF-8 is listed with
  its exact bytes, like any other. Mount points are crossed; there is no `-one-file-system` yet.

## S3 (`internal/source/s3`)

**Client** (`internal/source/s3/client`). The ListObjectsV2 request, the retry policy and the response parser
are this package's. Credentials and SigV4 come from the AWS SDK's auth modules (`aws-sdk-go-v2/config`,
`credentials`, `aws/signer/v4`). The SDK's S3 service client (`service/s3`) is not linked into the binary:
`go version -m swath` lists no `service/s3`.

- *Request:* one signed GET with `list-type=2`, `prefix`, `start-after`, `continuation-token`, `max-keys`,
  `delimiter`, `fetch-owner` and `encoding-type=url`. The query is built sorted and SigV4-encoded, so the URL
  sent is the canonical query that is signed.
- *Endpoints:* the client supports these forms:
  - virtual-hosted `bucket.s3.REGION.amazonaws.com` (the default);
  - path-style (`-path-style`; also used automatically for bucket names with dots and for IP endpoints);
  - FIPS (`-fips`: `s3-fips.REGION…`) and dual-stack (`-dualstack`: `s3.dualstack.REGION…`), alone or
    combined;
  - the China partition (`*.amazonaws.com.cn` for `cn-` regions);
  - custom endpoints (`-endpoint`, S3-compatible stores).

  **Not supported yet:** access points and Multi-Region Access Points (ARNs), S3 Express One Zone directory
  buckets, Outposts, the GovCloud-specific and ISO partitions beyond the plain regional host,
  `use_fips_endpoint` and `use_dualstack_endpoint` read from the shared config (use the flags), and
  automatic region redirects.
- *Signing:* the SDK's SigV4 signer with `UNSIGNED-PAYLOAD` (`x-amz-content-sha256`), plus the session token
  when the credentials have one.
- *Credentials:* `-anonymous` is authoritative: nothing is looked up and one warning is printed if `AWS_*`
  variables are set. Otherwise the SDK's default chain runs, with `-profile` selecting the shared-config
  profile. The chain covers:
  - environment keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`);
  - the shared `config` and `credentials` files and their profiles (`AWS_PROFILE`, `AWS_CONFIG_FILE`,
    `AWS_SHARED_CREDENTIALS_FILE`);
  - IAM Identity Center (SSO) profiles;
  - web identity (`AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`);
  - `credential_process`;
  - `role_arn` with `source_profile` or `credential_source` (AssumeRole through STS);
  - container credentials (ECS, EKS Pod Identity);
  - EC2 instance roles (IMDSv2).

  Credentials are cached and refreshed by the SDK. The first lookup is bounded at 3 s. If the chain finds
  nothing and no profile was named, requests go unsigned; a named profile that fails is an error.
- *Region:* `-region`, else the SDK's resolution (`AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's
  `region`), else `us-east-1`. A 301 or `AuthorizationHeaderMalformed` answer names the bucket's region in
  the error.
- *Retries:* up to 9 attempts with full-jitter exponential backoff (100 ms doubling, capped at 5 s) on 5xx,
  429, `SlowDown` and other throttling codes, `RequestTimeout`, connection errors and cut bodies, and attempts
  past the per-request timeout (`-request-timeout`, default 30 s), and 200 answers whose body does not parse
  (a cut or garbled body is a transport fault: retried, counted as `parse_failures`, never a skipped page).
  Other 4xx answers are typed `*client.Error`
  values (status, code, message, request id) and are not retried. Attempts, retries, throttles and timeouts are
  counted in the summary.
- *Parsing:* a hand-rolled scanner over the whole body (read into a pooled buffer) with encoding/xml's
  semantics for what S3 sends: entities and character references, CDATA, comments, namespaces, `\r\n`
  normalisation, last occurrence wins, unknown elements (`RestoreStatus`, newer fields) skipped. It allocates
  per page, not per object: the page's strings (keys, ETags, owner, checksum fields, token) are views into one
  arena allocated for that page, repeated values share one copy, and keys and common prefixes are URL-decoded
  in place (form semantics, as the AWS SDKs decode). Rows keep pointing into the arena until encoded; the
  writer clones the only keys that outlive a part (its min and max).

**Paging.** A range is `(lo, hi]`: `lo = ""` is ⊥, the open frontier has `hi = ⊤`. The first page sends
`StartAfter = lo`, later pages the continuation token. Keys past `hi` are trimmed (`overfetch_keys`); a page
whose last key equals `hi` ends the range without another request; `IsTruncated = false` ends it. A truncated
page without a token, a truncated page with no key in range, or a key not after its predecessor is an error.

**Root and seed.** The root is `(⊥, ⊤]`. With `-seed` the root is one delimiter page that emits no rows and
tiles the scope at its common prefixes `(⊥,p1], …, (pk, ⊤]`: unusable start-after values are dropped; at most
`min(1000, 4 × workers)` cuts, evenly subsampled; and swath's closing sentinel
`(pk, prefixCeil(pk)], (prefixCeil(pk), ⊤]` when the page was complete and ended with a prefix.

**Splits are owner-side.** A range's children become claimable only when its `Expand` returns, so there are
no thieves. After each full page the owner takes the provider mutex and computes demand,
`hunger = workers − active − queued`. If `hunger > 0` and the range ranks among the `hunger` largest eligible
ranges (open frontier first, then keys listed, then age), it emits `Child((c, m])` and `Child((m, hi])` and
returns, where `c` is the last key emitted. `(lo, hi] = (lo, c] ∪ (c, m] ∪ (m, hi]`, disjoint and in order.
Every split feeds an idle worker and no LIST is repeated.

**Guards**, checked before any child and counted per clause: `done` (never after the page that ended the
range), `unsafe_cursor` (`c` must be a usable start-after), `cursor` (`m > c`), `bound` (`m < hi`, or below
`prefixCeil(scope)` on the frontier), `no_pivot` (the owner keeps listing).

**Pivots.** Three steps:
1. **The spec's pivot is the default:** swath's `ByteMidpoint`, i.e. the safe midpoint of `(c, hi]`, or
   `ForwardReflect` on the open frontier, with the 1,024-byte cap.
2. **A density refinement from the last page.** Keys are read as numbers in the alphabet seen on that page.
   The target is `c + page span × max(4, hunger)`, at most halfway to `hi`, cut after its first character
   that differs from `c`. It is taken only when it lies between the cursor and the spec pivot (nearer the
   cursor), or when the spec pivot is within one page span of the cursor. In that second case the spec
   pivot would leave the lower child empty: reflection from ⊥ moves only the last character.
3. **The 1-key probe (Java's pivot probe; off by default, `-probe` turns it on).** Before the split is published,
   one ListObjectsV2 with `max-keys=1` after the pivot checks that the upper child `(m, hi]` holds a key. If
   it does not, the pivot moves to the safe midpoint of `(c, m]` and is probed again, up to 3 probes. Then
   the split is skipped, and the range does not try again for 2, 4, … 64 pages.

The split's demand slot is reserved while it is probed, so concurrent owners do not split for the same idle
worker. Every pivot is safe (I12). Without `-probe` the provider lists with the kernel alone.

Measured on five replayed recordings (REPORT.md, "Step 5a"), the probe costs more than the empty children
it saves: it is a serial round trip before each split, so it lowers concurrency, and runs were 1.4–2.2× slower
than without it. Without it the refined pivot matches the old one. So the probe is off by default; the code
stays, to be revisited only as an asynchronous or speculative check, never a serial pre-split round trip.

**Why.** Before the probe, the density estimate was the first choice and could reach far past the data on
skewed shapes. On a replay of noaa-nws-fourcastnetgfs-pds, 1,362 of 2,927 splits left an empty upper child,
and the run used about 1 of 64 workers (REPORT.md "Step 5a").

**Counters** (in the run summary): objects, bytes, pages, requests, retries, `overfetch_keys`; seed
(prefixes, cuts, unsafe, subsampled, sentinel and why declined); splits (considered, extrapolated, accepted
bounded and frontier, rejected per guard clause, skipped for no demand, outranked or not continuing);
`child_empty.{lower,upper}`; with `-probe`, `probe.{requests,upper_empty,moved}` and `split.rejected.probe_empty`; `max_active`, `max_queued`, `mean_active`; busy and
sink-wait time.

**Sequential oracle.** `swath diag list --sequential` lists the scope as one continuation-token chain; its dataset is
byte-identical to the parallel core's. It is the only mode where `-max-keys` stops early (the dataset is
then marked incomplete).

**Resume.** The provider is `source.Resumable`: `Restart(K)` makes the root one range `(K, ⊤]`, split as on
a fresh run ([durability.md](durability.md)).

**What the full engine adds** (later steps, each by measured need; the 1-key probe is ported):
structure and `max_keys=1` probes; density-aware pivots from probes; the owner gate chain, pacing and futility
backoff; AIMD concurrency; mass-aware and multi-level seeding; the rate-anchored size estimator; versions mode.

## Registry and optional interfaces

A provider registers a `registry.Factory` from `init`: its URL scheme (`s3`; the empty scheme takes local
paths), its flag group (`Flags(*flag.FlagSet) any` returns the provider's options value, filled by the
parse), `Defaults`, and `Open(ctx, Request) (Source, error)`. The command imports providers only for that
registration and passes the options value for the root's scheme as `listing.Config.Source.Options`, opaque to the
pipeline. `Open` validates the root without touching the output.

A `registry.Source` is the provider bound to its schema (`registry.Bind[R, P]`: the range type erased by
`source.Erase`, ranges boxed once each, rows untouched; `Run` instantiates the order stage and the part router
for `P`), its schema `Info`, its `Locator` (kind, root, container, local path) and `Listed` (rows emitted, checked
against the rows ordered). Optional interfaces, each found by a type assertion:

| Interface | Meaning | S3 | fs |
|---|---|---|---|
| `Identified` | its part of the resume identity: named fields, each with a canonical form for comparison | source, endpoint (canonical: `NormalizeEndpoint`), region, fetch owner | – |
| `Resumable` | `Restart(after)` before `Root`: list only keys after the last published key | `(K, ⊤]` | – |
| `Capable` | per-instance capabilities (`Resume`): an instance may decline what its type implements | `Resume` | – |
| `Excluder` | leaves an output directory inside its root out, after the output is prepared | – | yes |
| `Reporter` | its stats under its name in the summary, a text line, error count, metadata tier, and for a remote source its `Coverage` (target, filters, complete, stop reason) and `Activity` for swath's run summary | `s3`, coverage | `listing` |

A source that is not resumable (or declines `Resume`) runs without the output lock or journal, as the local
listing always has; `swath resume` on it is an error.

## The contract for the next providers (planned with GCS)

From the architecture review. The current contract holds for S3 and the filesystem; GCS will make it
explicit:
- **Capabilities**, declared once per provider (built so far: `source.Capable` with `Resume`):
  - byte-ordered output or normalized (sorted inside the provider);
  - seekable restart (start-after a key) or token replay;
  - snapshot or live listing.
- **Bounds:** explicit bound inclusivity per provider, as S3's `(lo, hi]` is today; no per-range
  comparators (output order is always unsigned bytes).
- **Bounded cost:** one `Rows` call has a bounded cost; no unbounded in-memory directory or page.
- **Restart:** start after a key; the journal stores no provider state (built: `source.Resumable`). A
  provider that can only replay tokens must re-list to the key or declare `Capabilities.Resume` false.
- **Disposal:** a cancelled run releases every queued range's resources. Built: `source.Disposer`; the order
  stage disposes ranges it never expands, including children an `Expand` emitted before failing. Filesystem
  chunks release their shared directory descriptor this way (`TestCancelReleasesChunkFDs`).
- **Scheduler observation:** a narrow interface (idle capacity, frontier distance, output pressure) replaces
  the provider's own active/queued bookkeeping.

## Next providers

GCS lists in unsigned UTF-8 byte order with continuation tokens: S3-shaped. Azure Blob lists in UTF-16
code-unit order, so pivots must stay out of the zone where it differs from byte order and ranges are sorted
internally. Google Drive is unsorted: a folder is a range, read fully and sorted by the filesystem key rule.
