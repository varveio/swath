# Operating swath against object storage

> **swath lists metadata and writes local output.** It never reads, modifies, or deletes
> object contents. The S3 path uses `ListObjectsV2`; it does not call `HeadObject`,
> `GetObject`, `HeadBucket`, `GetBucketLocation`, or `ListBuckets`.

S3 is the supported backend. S3-compatible endpoints must provide globally ordered
`ListObjectsV2` results and a correct `StartAfter` implementation. GCS access through its
XML API is experimental compatibility, not a native GCS backend.

## Credentials and region

With no authentication flags, swath uses the AWS SDK's default credential and region
chains:

```bash
# Shared AWS profile
swath list s3://my-bucket/prefix/ \
  --profile production --region us-east-1

# Environment, web identity, container role, or instance role
swath list s3://my-bucket/prefix/ \
  --region us-east-1

# Anonymous public listing
swath list s3://my-public-bucket/prefix/ \
  --region us-east-1 --no-sign-request
```

If neither `--region` nor the SDK chain resolves a region, swath exits 2 before opening a
checkpoint or sending a request.

Prefer workload identity, task/pod roles, and instance roles over long-lived access keys.
See [Environment variables](configuration.md#environment-variables) for recognized
configuration sources.

## Credentials in Docker

A container does not automatically inherit the host's environment variables or shared
AWS files. Pass the intended credential source deliberately.

### Environment credentials

Forward variable names rather than putting secret values in the command line:

```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e AWS_REGION \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ --region us-east-1
```

### Shared profile files

Mount the AWS directory read-only and point the SDK at its files:

```bash
docker run --rm \
  -v "$HOME/.aws:/aws:ro" \
  -e AWS_SHARED_CREDENTIALS_FILE=/aws/credentials \
  -e AWS_CONFIG_FILE=/aws/config \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ \
  --profile production --region us-east-1
```

This works for profiles whose dependencies are present in the mounted files. Profiles
that rely on a host-only credential process, local SSO cache, or another external file
need that dependency inside the container too.

On ECS, EKS, or another managed runtime, prefer the platform's task, pod, web-identity, or
instance credential path instead of mounting a developer profile.

The [private-bucket getting-started example](getting-started.md#6-list-a-private-bucket)
also adds the writable output mount for a managed Parquet dataset.

## Least-privilege IAM

Every worker page and internal probe is a `ListObjectsV2` request, authorized by the
bucket-level `s3:ListBucket` action:

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

The resource is the bucket ARN without `/*`. Prefix conditions can narrow access, but
they must allow every prefix value swath may issue while discovering and splitting the
requested subtree. The source URI still determines what the run lists.

For requester-pays buckets, add:

```bash
--requester-pays requester
```

This changes billing responsibility, not the required IAM action.

## S3-compatible endpoints

Set an endpoint explicitly:

```bash
swath list s3://my-bucket/prefix/ \
  --endpoint-url http://localhost:9000 \
  --format parquet -o out/
```

`--endpoint-url` enables path-style addressing and supplies `us-east-1` as the default
region. Use `--no-force-path-style` when an implementation requires virtual-hosted style.

A server that accepts S3-shaped requests is not automatically compatible with swath.
The range engine depends on:

- a single bytewise global ordering for the requested listing;
- `StartAfter` as an exclusive lower bound;
- forward progress across paginated results; and
- response encoding compatible with the AWS SDK.

One known edge case involves endpoints that echo a literal `%` cursor instead of the
percent-encoding used by AWS S3, causing the SDK decoder to reject the response. swath
does not synthesize unsafe `%` pivots, but it cannot rewrite a user prefix or real key.
See [S3 implementation compatibility](internals/s3-implementation-compatibility.md).

<a id="gcs-through-its-xml-api"></a>

### Experimental: GCS through the XML API

GCS's XML API accepts OAuth bearer tokens. The following path avoids creating HMAC
interoperability keys:

```bash
swath list s3://some-gcs-bucket/prefix/ \
  --endpoint-url https://storage.googleapis.com \
  --force-path-style \
  --bearer-token-command 'gcloud auth print-access-token' \
  --format parquet -o out/
```

The token command replaces SigV4 signing and runs again every 45 minutes by default. The
command and token are not stored in the checkpoint. Re-pass the command when resuming:

```bash
swath resume out/ \
  --bearer-token-command 'gcloud auth print-access-token'
```

This is an experimental listing-only compatibility path. It has not received the same
conformance coverage as AWS S3, LocalStack, and MinIO. A native GCS backend remains
roadmap work.

## Request cost

The ideal data-page count is approximately one LIST request per 1,000 returned keys.
Add probes, retries, sparse pages, and any unfinished tail re-listed after interruption:

```text
estimated requests before overhead ≈ object count / 1,000
cost = actual requests / 1,000 × current provider price per 1,000 LIST requests
```

Pricing varies by provider, region, storage class, and time. Use the provider's current
price rather than treating a number in this repository as a quote.

The terminal estimate uses a labeled AWS reference rate. Under `--endpoint-url`, swath
omits the dollar estimate because it cannot know the provider's tariff.

The JSON run report is the reliable input for cost accounting:

- `cost.api_calls` — actual `ListObjectsV2` attempts, including probes and retries;
- `cost.cost_usd` and `cost.basis` — swath's labeled reference-rate estimate; and
- `efficiency.api_calls_per_1k_objects` — request overhead relative to returned objects.

Recompute the bill from `cost.api_calls` and the current price. A value materially above
one call per thousand objects means probes, retries, sparse pages, or interruption
overhead mattered.

## Live-listing consistency

S3 does not provide one point-in-time transaction across a long `ListObjectsV2` scan.
Objects added, removed, or renamed while a run is active can affect which live state the
result observes.

`_SUCCESS` means swath completed and published the result of its listing. It does not
convert a changing bucket into a historical snapshot. Use a provider-generated inventory
or another snapshotting mechanism when point-in-time semantics are required.

## Before a large run

1. Confirm that a fresh S3 Inventory or S3 Metadata table is not already available. It is
   normally cheaper to query.
2. Test a representative prefix and inspect request overhead, throughput, output size,
   and the live consistency requirements.
3. Use a managed Parquet directory so interruption can resume.
4. Begin with the default concurrency ceiling. Raise it only after repeated comparable
   runs still gain throughput.
5. For `--sort`, provision staging and final-output disk together; see
   [The sorted merge](performance.md#the-sorted-merge).
6. Retain `_swath_summary.json` with the result. It can contain target URIs, filters,
   slow-range bounds, and key samples, so redact it before sharing outside the bucket's
   trust boundary.

## Filing a support request

Include these four things so an issue or investigation does not stall on missing facts:

- the output of `swath --version` (version, commit, and Java runtime);
- `_swath_summary.json` from the run, with bucket names, keys, and other sensitive
  fields redacted;
- the target object-store provider and region; and
- the output mode used (`--format`: `auto`, `table`, `tsv`, `jsonl`, `parquet`, or the
  diagnostic `discard`, and whether `--sort` was set).
