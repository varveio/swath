# FAQ & troubleshooting

## "No AWS region" / credentials errors

swath resolves credentials from the standard AWS SDK default chain: environment
variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`),
a `--profile NAME` (or the `AWS_PROFILE` default), web-identity tokens
(`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE` — e.g. GKE/EKS workload
identity; swath bundles the `sts` module so this auto-refreshes), and finally
container/instance-role credentials. Region resolves the same way: an explicit
`--region`, else `AWS_REGION` / `AWS_DEFAULT_REGION`, else the profile file or
instance metadata. If nothing resolves a region, swath fails fast at startup
(exit 2) rather than letting an SDK exception surface later — pass `--region`
or set one of the environment variables above.

## Listing a public bucket

Pass `--no-sign-request` to send anonymous, unsigned requests — no
credentials needed. This is the same thing the AWS CLI's flag of the same name
does, and it's the right choice for AWS Open Data buckets and other
public/anonymous-read buckets.

## Listing an S3-compatible endpoint (LocalStack, MinIO, ...)

Pass `--endpoint-url URL`. Path-style addressing turns on automatically once
`--endpoint-url` is set (override with `--no-force-path-style` if your
endpoint needs virtual-hosted style instead). See
[`internals/s3-implementation-compatibility.md`](internals/s3-implementation-compatibility.md)
for the one known deviation this surfaces in practice — see the `%`-cursor
question below.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success, empty result, an already-complete resume, or stdout closed by the downstream reader (broken pipe) |
| `1` | Unrecoverable error: listing failure, non-disk-full output write failure, checkpoint corruption |
| `2` | Bad arguments, invalid URI, invalid configuration, or a guarded refusal (unfinished/foreign output dir, format/extension mismatch) |
| `74` | The output filesystem ran out of space (`EX_IOERR`); resumable only when the run has managed directory-dataset state |
| `75` | Retryable stuck state (`EX_TEMPFAIL`); resumable only when the run has managed directory-dataset state |
| `124` | Stopped by `--max-duration`; resumable only when the run has managed directory-dataset state |
| `130` | Cancelled by SIGINT (Ctrl+C) |
| `143` | Cancelled by SIGTERM (the default `kill`) |

When a managed directory-dataset checkpoint is in play, nonfatal interruptions
leave it for `swath resume <dir>`. Stdout and FILE-kind runs remain
non-resumable regardless of exit code. See
[`usage.md`](usage.md#exit-codes) for the full reference.

## "It OOM'd on `--sort`"

`--sort` buffers listing pages into heap-adaptive staging segments; peak heap
is a function of the heap you grant, not of object count, but disk (the
staging volume) scales with the bucket size — undersizing it is the common
failure mode, not the heap. See
[`usage.md`](usage.md#sizing-sorted-output) for representative-sample sizing
guidance and the disk-sizing guard (`--tune sort.ignore-disk-check`) that refuses
a run before it starts if the staging volume is already too small.

## The `%`-cursor crash against nonconformant S3-compatible endpoints

Real S3 re-percent-encodes a cursor value it echoes back, even a value ending
in a lone/trailing `%` that isn't itself a valid escape. Some S3-compatible
implementations (including the tested LocalStack build) echo the raw bytes
instead, which crashes a client's
`URLDecoder` on decode. swath synthesizes internal split pivots and seed cuts,
but both synthesis paths explicitly exclude `%`; it does not
rewrite copied user input or real-key bounds, which can still expose the endpoint
bug. The tested MinIO build percent-encodes the echo conformantly. See
[`internals/s3-implementation-compatibility.md`](internals/s3-implementation-compatibility.md)
for the full mechanism.

## Why a JVM?

We wanted the concurrency substrate more than we wanted to avoid the runtime.
The engine runs a work-stealing scan over one virtual thread per range with
`ScopedValue`-propagated run context — both shipped as final, non-preview APIs
only from JDK 25 (JEP 506) — instead of hand-rolled async callbacks or a
thread-per-range model that wouldn't scale past a few thousand ranges. The
tradeoff is an honest one: a JVM process, not a static binary, and a real
(if bounded) heap to size.

## When should I use S3 Inventory / S3 Metadata instead?

If a fresh, queryable AWS S3 Inventory or S3 Metadata table already exists for
your bucket, use that — reading a precomputed listing is strictly cheaper than
running any live-`ListObjectsV2` lister, swath included. swath is **LIST-only**
by design: it exists for the buckets where that isn't an option — no inventory
configured, one that's gone stale, or a bucket you don't own and can't
configure. See the Scope section of
[`internals/overview.md`](internals/overview.md#scope) for the full framing.
