# Operating swath against real S3

This is the practical guide to running swath against a live bucket: the
credentials it needs, the minimal IAM policy it requires, how to point it at
public buckets or S3-compatible endpoints, and what a run costs. swath's cost
model is simple — it makes `ListObjectsV2` calls and nothing else, so the bill
is a direct function of how many LIST requests a full listing takes.

## Credentials

swath uses the standard AWS SDK credential chain. With no flags it builds the
SDK default provider, which resolves credentials from, in order, environment
variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`),
the shared config and credentials files, and the container/instance metadata
service (IMDS) when running on AWS compute. Anything the AWS CLI can
authenticate with, swath can too.

- `--profile NAME` selects a named profile from your shared AWS config instead
  of the default chain.
- `--region REGION` sets the region. When omitted, the region resolves from the
  environment (`AWS_REGION` / `AWS_DEFAULT_REGION` or the active profile); if
  none is set, swath exits with a configuration error rather than guessing.

## Minimal IAM policy

swath issues exactly one S3 API operation: `ListObjectsV2`. Every worker page
fetch and every internal probe (the one-key pivot and the `delimiter=/`
structure probe the engine uses to split work) is a `ListObjectsV2` call. swath
does not call `HeadObject`, `GetObject`, `HeadBucket`, `GetBucketLocation`, or
`ListBuckets`, and it does not read object contents. Versioned listing
(`ListObjectVersions`) is not implemented.

`ListObjectsV2` is authorized by the bucket-level `s3:ListBucket` action, so the
least-privilege policy grants that single action on the target bucket and
nothing more:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SwathListBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-bucket"
    }
  ]
}
```

`s3:ListBucket` is a bucket-level permission: its `Resource` is the bucket ARN
(`arn:aws:s3:::my-bucket`), not an object ARN (`.../*`). That one action covers
the whole listing, including the pivot and structure probes.

To restrict swath to a prefix subtree, add an `s3:prefix` condition — but note
that swath descends into sub-prefixes with many distinct `prefix` values as it
splits work, so the condition must permit every prefix under the subtree, not
just the top-level one. When in doubt, grant `s3:ListBucket` on the whole bucket
and scope with the source URI instead.

For requester-pays buckets, pass `--requester-pays requester` so swath sends the
requester-pays header. This changes who is billed, not which IAM action is
required — the caller still only needs `s3:ListBucket`.

## Public buckets and no credentials

For a public bucket that allows anonymous listing, pass `--no-sign-request`.
swath then sends unsigned (anonymous) requests and needs no credentials at all:

```
swath list s3://my-public-bucket/ --no-sign-request -o out/
```

Use this whenever the bucket's policy grants public `s3:ListBucket` — it avoids
an unnecessary credential lookup and works on a machine with no AWS config.

## S3-compatible endpoints

To run swath against MinIO, LocalStack, or another S3-compatible service, point
it at the endpoint with `--endpoint-url`:

```
swath list s3://my-bucket/ --endpoint-url http://localhost:9000 -o out/
```

Setting `--endpoint-url` turns on path-style addressing automatically (the
addressing most non-AWS implementations expect), and the region defaults to
`us-east-1` since it is irrelevant to the server.

One caveat worth knowing: some S3-compatible servers echo request parameters
back verbatim rather than percent-encoding them the way AWS S3 does, which can
surface as a decode error on keys containing a literal `%`. The decode happens in the
AWS SDK itself — its always-installed `DecodeUrlEncodedResponseInterceptor` strict-decodes
the echoed fields, and swath cannot turn that off. What swath does is never *synthesize* a
cursor or pivot containing `%`, so its own split and seed machinery can't trip the bug; a
bound it copies verbatim still can — a `--prefix` **you** pass that ends in a lone `%`, or a
real key of the bucket's own that does and then becomes a `start_after` cursor. See
[`internals/s3-implementation-compatibility.md`](internals/s3-implementation-compatibility.md)
for the mechanism and the exact exclusion.

## Cost model

swath's S3 request charge comes from LIST requests. The ideal object-page call
count is roughly one `ListObjectsV2` request per 1,000 objects (S3 returns up to
1,000 keys per page), plus engine probes, retries, and any crash-tail re-listing.
That overhead is workload- and failure-dependent and can be material; use the
completed run summary for the actual call count. For a bucket of `N` objects,
the idealized estimate before overhead is:

```
object-page LIST requests ≈ N / 1000
price units   ≈ LIST requests / 1000
cost          ≈ (LIST requests / 1000) × price-per-1000-LIST-requests
              ≈ (N / 1,000,000) × price-per-1000-LIST-requests
```

The per-1,000-LIST price is a region- and time-dependent input you supply from
current S3 pricing. Using the CLI summary's $0.005 reference rate only as a
worked example (verify the current price for your region), and excluding overhead:
100,000,000 objects ÷
1,000 objects/request = 100,000 LIST requests; 100,000 requests ÷ 1,000
requests/price-unit = 100 price units; 100 price units × $0.005/price-unit =
$0.50.

**Resume bounds durable-output re-listing.** Every finalized Parquet part is
durable and is never re-listed, and each range restarts from its `durable_cursor`. What a
crash does cost you is the **in-flight** work — a part left half-written is discarded (it
was never durable and never in `manifest.json`), so the keys it held past the last durable
cursor are listed a second time. That re-spend is bounded by what the open parts had
buffered at the moment of the crash, not by how far the run had got:
`--parquet-part-size`, `--part-rotation-interval`, and
`--part-rotation-max-rows` govern that tail.

### Reading the cost from a run summary

The terminal tells you first. Any run that earns an end-of-run block (see
[`usage.md`](usage.md#end-of-run-summary)) prints its estimated LIST spend on stderr, labeled with
the rate it assumed:

```
  ~$0.006 (est. @ $0.005/1k LIST)
```

Stating the rate is the point: `$0.005/1k` is a single-region AWS reference price, and LIST pricing
varies by region and over time, so the label is what lets you rescale honestly from your own rate
rather than trusting a figure swath cannot verify. When the provider is unknown — any run with
`--endpoint-url` (MinIO, R2, LocalStack, self-hosted) — **no dollar figure is printed at all**: the
call count is still yours to price, but swath will not guess someone else's tariff.

The machine-readable summary (by default `_swath_summary.json` in the output directory, or a path
you set with `--report`) carries the same cost, always — including under `--endpoint-url`, where
`cost.basis` names exactly what was assumed so a consumer can discard or recompute it:

- `cost.api_calls` — the total number of `ListObjectsV2` calls the run issued.
  Divide this count by 1,000 to obtain billable price units, then multiply by
  your per-1,000-LIST price.
- `cost.cost_usd` — swath's own estimate of the LIST bill, computed as
  `api_calls × $0.005 / 1000`. The rate is a built-in `us-east-1` reference
  price; if your region or the current price differs, recompute from
  `cost.api_calls` and your own rate.
- `cost.basis` — that assumption, named: `rate_per_1k_usd` (the numeric rate `cost_usd` was derived
  from) and `source` (`aws-list-reference-rate`).
- `efficiency.api_calls_per_1k_objects` — LIST calls per 1,000 objects listed.
  For a well-behaved run this sits near the ideal of ~1.0; a materially higher
  value means the probe/split overhead was significant for this bucket shape.

The same fields appear on the `list_run_summary` log line at the end of a run
(`api_calls`, `cost_usd`, `api_calls_per_1k_objects`), which `-v` enables.

### Why the summary is on by default

swath's default is inverted from most CLIs, deliberately: the machine-readable report has always
been written by default, while the person who ran the command was told nothing. For an hour-long
enumeration that is backwards — the operator is the one who waited. So a run that earns a summary
(over 1.5 s, durable output produced, or an early stop) prints one, and a run that does not stays
silent. What the block means, line by line:

- **objects, elapsed, keys/s** — what the run listed and how fast.
- **API calls and `per 1k objects`** — the bill's shape. swath is billed per request, so `1.00 per
  1k` versus `38 per 1k` is the is-this-bucket-pathological signal, in one token.
- **`in flight avg` and `peak`** — sustained parallelism versus a brief spike. Peak saturates at the
  concurrency ceiling; the average is the number a tuning change actually moves.
- **the faults line** (only when non-zero) — recovered backpressure and retries. These are normal in
  small numbers on a large bucket; they are logged nowhere else at the default level, which is why
  the line exists.
- **cost** — as above, absent when the provider is unknown.

If you want none of it, `--no-stats`. If you want it on every run however short, `--stats`. If you
want it parsed rather than read, `--report`.
