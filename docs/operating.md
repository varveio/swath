# Operating swath against object storage

swath issues listing requests and writes local output. It never reads object contents and
does not call `HeadObject`, `GetObject`, `HeadBucket`, `GetBucketLocation`, or
`ListBuckets`.

## Credentials and region

With no authentication flags, swath uses the AWS SDK's default credential and region
chains. Common forms are:

```bash
# Shared AWS profile
swath list s3://my-bucket/prefix/ --profile production --region us-east-1

# Environment, web identity, container role, or instance role
swath list s3://my-bucket/prefix/ --region us-east-1

# Anonymous public listing
swath list s3://my-public-bucket/prefix/ --region us-east-1 --no-sign-request
```

If neither `--region` nor the SDK chain resolves a region, swath exits 2 before opening
a checkpoint or sending a request. Prefer workload identities and compute roles over
long-lived environment keys. See [configuration](configuration.md#environment-variables)
for the recognized environment variables.

## Credentials in Docker

A container does not automatically inherit the host's environment variables or shared
AWS files. Pass the credentials source deliberately.

For environment credentials, forward the variables by name rather than placing secrets
in the command line:

```bash
docker run --rm \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ --region us-east-1
```

For a shared-files profile, mount the AWS directory read-only and point the SDK at its
two files:

```bash
docker run --rm \
  -v "$HOME/.aws:/aws:ro" \
  -e AWS_SHARED_CREDENTIALS_FILE=/aws/credentials \
  -e AWS_CONFIG_FILE=/aws/config \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ --profile production --region us-east-1
```

The shared-files example covers profiles whose credentials are available in those files.
Profiles that depend on host-only credential processes, local SSO caches, or external files
also need those dependencies inside the container. On ECS, EKS, or another managed runtime,
prefer its task, pod, web-identity, or instance credential path instead of mounting a local
profile.

The [getting-started private-bucket example](getting-started.md#5-list-your-bucket) adds
the writable output mount used by a managed Parquet dataset.

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

The resource is the bucket ARN, without `/*`. Prefix conditions can narrow access, but
they must allow every prefix value swath may issue while descending and splitting the
requested subtree. The source URI still determines what the run lists.

For requester-pays buckets, add `--requester-pays requester`. It changes billing
responsibility, not the required IAM action.

## S3-compatible endpoints

Set an endpoint explicitly:

```bash
swath list s3://my-bucket/prefix/ \
  --endpoint-url http://localhost:9000 \
  --format parquet -o out/
```

`--endpoint-url` turns on path-style addressing and supplies `us-east-1` as the default
region. Use `--no-force-path-style` if an implementation requires virtual-hosted style.

swath depends on globally ordered `ListObjectsV2` results and `StartAfter`; an endpoint
that implements only the surface syntax is not necessarily compatible. A known edge case
involves servers that echo a literal `%` cursor without the percent-encoding AWS S3 uses,
causing the AWS SDK decoder to fail. swath avoids synthesizing unsafe pivots but cannot
rewrite user prefixes or real keys. See
[S3 implementation compatibility](internals/s3-implementation-compatibility.md).

### GCS through its XML API

GCS's XML API accepts OAuth bearer tokens. A command can mint a fresh token without
creating HMAC interoperability keys:

```bash
swath list s3://some-gcs-bucket/prefix/ \
  --endpoint-url https://storage.googleapis.com \
  --force-path-style \
  --bearer-token-command 'gcloud auth print-access-token' \
  --format parquet -o out/
```

The token command replaces SigV4 signing and is re-run every 45 minutes by default.
It is never persisted. Re-pass it when resuming:

```bash
swath resume out/ --bearer-token-command 'gcloud auth print-access-token'
```

This is a listing-only path; GCS has not been run through the same conformance suite as
AWS S3, LocalStack, and MinIO.

## Request cost

The ideal data-page count is approximately one LIST request per 1,000 returned keys.
Add probes, retries, and any unfinished tail re-listed after interruption:

```text
estimated requests before overhead ≈ object count / 1,000
cost = actual requests / 1,000 × your provider's current per-1,000 LIST price
```

Pricing varies by provider, region, and time. Use the current provider price rather than
treating a number in this repository as a quote. The terminal estimate uses a built-in
AWS reference rate and labels that assumption. Under `--endpoint-url`, the terminal omits
the dollar estimate because swath cannot know the provider's tariff.

The JSON run report is the reliable input for cost accounting:

- `cost.api_calls` — actual `ListObjectsV2` attempts, including probes and retries;
- `cost.cost_usd` and `cost.basis` — swath's labeled reference-rate estimate; and
- `efficiency.api_calls_per_1k_objects` — request overhead relative to returned objects.

Recompute from `cost.api_calls` and your current price. A value materially above one call
per thousand objects means probes, retries, sparse pages, or interruption overhead mattered.

## Before a large run

1. Confirm that a fresh S3 Inventory or S3 Metadata table is not already available; it
   will be cheaper to query.
2. Test a representative prefix and inspect request overhead, throughput, and output size.
3. Use a managed Parquet dataset so an interruption can resume.
4. For `--sort`, provision staging and final-output disk together; see
   [Performance](performance.md#the-sorted-merge).
5. Retain `_swath_summary.json` with the result. It records the target and can contain key
   samples, so redact it before sharing outside the bucket's trust boundary.
