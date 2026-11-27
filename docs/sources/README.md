# Choose a source

A source is what swath lists. Each has a page that follows one outline: what it lists and its URL, the smallest
working command, authentication and permissions, its own options, writing a dataset to it, and its limits.

| Source | URL | Lists | Every version (`--all-versions`) | Resume of a dataset run | A dataset written to it (`-o`) |
|---|---|---|---|---|---|
| [Local and mounted filesystems](local.md) | a path | files and directories | – | no: an interrupted run is run again | a local directory |
| [Amazon S3 and S3-compatible stores](s3.md) | `s3://BUCKET[/PREFIX]` | current objects of a general-purpose bucket | versions and delete markers | yes | `-o s3://BUCKET/PREFIX` |
| [Google Cloud Storage](gcs.md) | `gs://BUCKET[/PREFIX]` | current objects | live and noncurrent generations | yes | `-o gs://BUCKET/PREFIX` |
| [Azure Blob Storage](azure.md) | `az://ACCOUNT/CONTAINER[/PREFIX]` | current blobs; directories on hierarchical namespaces | versions, snapshots and soft-deleted blobs on flat namespaces | flat namespaces | `-o az://ACCOUNT/CONTAINER/PREFIX` |

A dataset of any source can be written to a local directory (`-o DIR`) or to any of the three object stores
(`-o` a bucket prefix with `--work-dir DIR`).

swath reads listings only: it needs permission to list, never to read or write objects. Each object-store
listing sends requests the provider may charge for; the run report estimates their cost on AWS S3 and GCS
([Cost](../size-a-run.md#cost)).

## What has run against the real service

Every source is tested against fakes, emulators and replayed recordings ([testing](../internals/testing.md)).
What has also run against the service itself, as recorded in [measurements](../internals/measurements.md):

| Source | Listing | Every version | A dataset written to it | Credentials |
|---|---|---|---|---|
| Local and mounted filesystems | real filesystems; benchmarks on synthetic trees | – | yes | – |
| Amazon S3 | one bucket of 1.09 billion objects; 35 public buckets of 50.6 billion in all | seven small public buckets | fakes only | anonymous only |
| S3-compatible stores | MinIO only, which omits keys from some listings: swath stops such a listing with `omitted_keys`, but can miss them in a versions listing ([S3-compatible stores](s3.md#s3-compatible-stores)); Cloudflare R2 by key ranges, which can miss keys there, so it is listed folder by folder by default: exact on 21 test shapes and on 83,435 keys at 256 workers, a bucket holding both spellings of a folder name stopping with `protocol` ([Cloudflare R2](s3.md#cloudflare-r2)); Ceph and others not run | not run | fakes only | MinIO access keys |
| Google Cloud Storage | a bucket of 5.1 million objects; public buckets | one small public bucket | yes: the datasets of the 35 S3 buckets | default credentials, anonymous |
| Azure Blob Storage | a public container of 1.84 million blobs | fakes only | fakes only | anonymous only; SAS, Shared Key and Entra ID not run |

Runs killed and resumed against a real store are the small versioned runs above and MinIO; every other kill and
resume is tested against fakes and replays. Credential renewal during a run has not been seen against a real
service. Each source's other limits are under "Limits and known differences" on its page. Before relying on a
store other than AWS, compare one prefix's object count with an independent listing
([S3-compatible stores](s3.md#s3-compatible-stores)).

## Credentials on every object store

Without `--anonymous`, swath uses the provider's credentials and stops before listing (exit 1, cause `auth`)
when there are none or they do not load. It never falls back to unsigned requests: a public bucket or
container needs `--anonymous`, which sends no credentials whatever the environment holds. The error is one
line naming the credential source and the cause's first line; `-v` shows the whole cause
([troubleshooting](../troubleshooting.md#credentials-could-not-be-loaded)).

`--anonymous` beside an explicit credential (`--s3-profile`, `--s3-bearer-token-command`,
`--gcs-credentials-file`, `--azure-sas-token`, a SAS in the source URL) is refused with exit 2. Credentials
only the environment holds are ignored, with one warning naming the variables:

```text
swath: warning: --anonymous: ignoring the GCS credentials in the environment (GOOGLE_APPLICATION_CREDENTIALS)
```

Secrets are not taken from flags, with two exceptions that are never recorded: `--azure-sas-token` and
`--s3-bearer-token-command`. Neither reaches the journal, the report or the terminal (each shows
`[redacted]`), so [`swath resume`](../commands/swath-resume.md) must be given them again.

The report's `diagnostics.source.stats` says where the credentials came from (`credentials`), whether the run was
`anonymous`, and for S3 the `region`.

## Endpoints and TLS

`--endpoint-url URL` sends requests to another service than the provider's own: an S3-compatible store, an
emulator (MinIO, fake-gcs-server, Azurite). It must be `http` or `https` with a host; user information
(`user:password@`), a query and a fragment are refused. The endpoint is part of the run's identity: a resume
uses the same one. There is no cost estimate for a custom endpoint. An endpoint that has never answered and
refuses every connection stops the run after about 30 seconds (exit 75, cause `unreachable`).

`--ca-bundle FILE` trusts the certificates in a PEM file besides the system's (a private certificate
authority). `--no-verify-ssl` checks no certificate, for a test endpoint only: it warns on every run that
has it, a resume included, and the report's `diagnostics.source.stats.no_verify_ssl` is true. Both apply to the
credential lookups swath makes over HTTPS (S3's STS and SSO, GCS's token exchange) as to the listing. A resume
may give either again; `--verify-ssl` undoes a recorded `--no-verify-ssl`.

## Object-store requests

S3, GCS and Azure Blob share the request options:

| Option | Default | Effect |
|---|---|---|
| `--request-timeout DURATION` | 30s | per-request timeout; a request over it is retried |
| `--request-rate N` | no cap | at most N HTTP attempts per second, retries and probes included |
| `--no-adaptive-limit` | the limit is on | do not lower concurrency when the service throttles |

A failed request that can be retried (a throttle, a timeout, a transient 5xx, a cut connection) is tried up
to nine times with exponential backoff (100 ms, at most 5 s apart); a listing page that still fails goes back
to the queue and is tried again later. `--stall-timeout` and
`--max-duration` bound a run that cannot make progress.
Throttling and how to tune for it: [Watch and tune a run](../watch-a-run.md#throttling).

More on each provider, for contributors: [S3](../internals/providers/s3.md), [GCS](../internals/providers/gcs.md),
[Azure Blob](../internals/providers/azblob.md), [local filesystems](../internals/providers/local.md).
