# Sources

```bash
swath list s3://bucket/logs/                         # Amazon S3, the AWS credential chain
swath list s3://bucket --endpoint-url https://minio.example:9000   # an S3-compatible store
swath list gs://bucket/logs/                         # Google Cloud Storage, Application Default Credentials
swath list az://account/container/logs/              # Azure Blob Storage
swath list /data                                     # a local or mounted directory
```

swath reads listings only: it needs permission to list, never to read or write objects. Each object-store
listing sends requests the provider may charge for; the run report estimates their cost on AWS S3 and GCS
([running.md](running.md#cost)).

## Credentials on every object store

Without `--anonymous`, swath uses the provider's credentials and stops before listing (exit 1, cause `auth`)
when there are none or they do not load. It never falls back to unsigned requests: a public bucket or
container needs `--anonymous`, which sends no credentials whatever the environment holds. The error is one
line naming the credential source and the cause's first line; `-v` shows the whole cause
([troubleshooting](troubleshooting.md#credentials-could-not-be-loaded)).

`--anonymous` beside an explicit credential (`--s3-profile`, `--s3-bearer-token-command`,
`--gcs-credentials-file`, `--azure-sas-token`, a SAS in the source URL) is refused with exit 2. Credentials
only the environment holds are ignored, with one warning naming the variables:

```text
swath: warning: --anonymous: ignoring the GCS credentials in the environment (GOOGLE_APPLICATION_CREDENTIALS)
```

Secrets are not taken from flags, with two exceptions that are never recorded: `--azure-sas-token` and
`--s3-bearer-token-command`. Neither reaches the journal, the report or the terminal (each shows
`[redacted]`), so [`swath resume`](commands/swath-resume.md) must be given them again.

The report's `source.stats` says where the credentials came from (`credentials`), whether the run was
`anonymous`, and for S3 the `region`.

## Endpoints and TLS

`--endpoint-url URL` sends requests to another service than the provider's own: an S3-compatible store, an
emulator (MinIO, fake-gcs-server, Azurite). It must be `http` or `https` with a host; user information
(`user:password@`), a query and a fragment are refused. The endpoint is part of the run's identity: a resume
uses the same one. There is no cost estimate for a custom endpoint.

`--ca-bundle FILE` trusts the certificates in a PEM file besides the system's (a private certificate
authority). `--no-verify-ssl` checks no certificate, for a test endpoint only: it warns on every run that
has it, a resume included, and the report's `source.stats.no_verify_ssl` is true. Both apply to the
credential lookups swath makes over HTTPS (S3's STS and SSO, GCS's token exchange) as to the listing. A resume
may give either again; `--verify-ssl` undoes a recorded `--no-verify-ssl`.

## Amazon S3

`s3://BUCKET[/PREFIX]` lists a general-purpose bucket's current objects; `--all-versions` lists every version
and delete marker.

**Permissions.** `s3:ListBucket` on the bucket; `s3:ListBucketVersions` with `--all-versions` (swath never
falls back to a current-object listing when it is refused).

**Credentials.** The AWS SDK's default chain: environment keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`), the shared `config` and `credentials` files and their profiles (`--s3-profile`, else
`AWS_PROFILE`), IAM Identity Center (SSO), web identity, `credential_process`, AssumeRole profiles, container
credentials (ECS, EKS Pod Identity) and EC2 instance roles (IMDSv2). The first lookup is waited for up to 30
seconds (a web-identity token file mounted late included). Refused before any request as configured but
broken: an access key without its secret or the reverse, a session token without both keys, `AWS_ROLE_ARN`
without `AWS_WEB_IDENTITY_TOKEN_FILE`, a profile that does not exist.

**Region.** `--s3-region`, else `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region`, else `us-east-1`.
swath does not discover a bucket's region: a request to the wrong one fails with an error that names the
bucket's region and the option to give (`--s3-region eu-west-1`). STS calls use the environment's or the
profile's region, not `--s3-region`.

**Requester pays.** `--s3-requester-pays` accepts the request charges of a requester-pays bucket.

**Endpoints.** Without `--endpoint-url`, `AWS_ENDPOINT_URL_S3`, else `AWS_ENDPOINT_URL`, is the endpoint
(unless `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` is `true`). A custom endpoint is addressed path-style
(`https://host/BUCKET`); `--no-s3-path-style` addresses the bucket in the host name instead, and
`--s3-path-style` uses the path on AWS's own endpoint. `--s3-fips` and `--s3-dualstack` (else
`AWS_USE_FIPS_ENDPOINT`, `AWS_USE_DUALSTACK_ENDPOINT`) select AWS's FIPS and dual-stack endpoints, and are refused
with a custom endpoint. `AWS_CA_BUNDLE` is the CA bundle when `--ca-bundle` is not given; the bundle and
`--no-verify-ssl` apply to the credential chain (STS, SSO) as to the listing.

**Bearer tokens.** For an S3-compatible endpoint that takes OAuth tokens instead of SigV4, such as GCS's XML
API:

```bash
swath list s3://bucket --endpoint-url https://storage.googleapis.com \
  --s3-bearer-token-command 'gcloud auth print-access-token' -o DIR
```

The command runs under `/bin/sh -c` (on Windows `cmd.exe /d /s /c`, so write it in cmd syntax there); its
one line of output is sent as `Authorization: Bearer TOKEN`, and no AWS credential is looked up. It runs again
after `--s3-bearer-token-refresh-interval` (45 minutes by default). A command that fails, runs past 30 seconds,
or prints nothing or more than one line fails the request with cause `auth`; a stale token is never reused.
The native `gs://` source is the alternative.

**Owner.** `--s3-fetch-owner` fills the `owner_id` and `owner_display_name` columns.

**Not supported.** Directory buckets (Express One Zone, names ending in `--x-s3`; refused with exit 2), access
points, Multi-Region Access Points and Outposts.

## S3-compatible stores

Give the store's URL and, usually, a region it accepts:

```bash
swath list s3://bucket --endpoint-url http://127.0.0.1:9000 --s3-region us-east-1
```

The store must answer `ListObjectsV2` as S3 does: in ascending byte order of the key, with continuation
tokens. swath checks every page's order and refuses a service that breaks it (exit 1, cause `protocol`).
It trusts the store's listing answers, including an empty page (no keys, no continuation) inside a range
as the end of that range. A store that wrongly returns such a "lying empty page" makes swath silently
miss keys: in [#265](https://github.com/varveio/swath/issues/265), a pinned MinIO image returned one with
later keys still to come, and a listing missed 57% of the keys.

Until the defense in #265 lands, compare the object count of a first run against a new S3-compatible
store with an independent count over the same bucket or prefix: the store's own metrics or a plain
sequential listing. [`swath diag list --sequential`](internals/diagnostics.md) lists through one
continuation-token chain, for example:

```bash
swath diag list s3://bucket --endpoint-url http://127.0.0.1:9000 --s3-region us-east-1 --sequential
```

## Google Cloud Storage

`gs://BUCKET[/PREFIX]` lists current objects through the JSON API; `--all-versions` lists every generation,
live and noncurrent. Soft-deleted objects are not listed.

**Permissions.** `storage.objects.list` on the bucket (the same with `--all-versions`). The token's scope is
read-only.

**Credentials.** `--gcs-credentials-file FILE` (a service-account or authorized-user JSON file), else
Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`, the gcloud credentials file, the metadata
server on Google Cloud. Credentials that cannot be found or loaded (no ADC anywhere, a
`GOOGLE_APPLICATION_CREDENTIALS` that names a missing file) stop the run before listing (`auth`).

**Requester pays.** `--gcs-billing-project PROJECT` bills the requests to that project. It is part of the
run's identity.

**Endpoint.** `--endpoint-url` replaces the JSON API's base URL, `https://storage.googleapis.com/storage/v1/`
(for fake-gcs-server).

## Azure Blob Storage

`az://ACCOUNT/CONTAINER[/PREFIX]`, or the same as
`https://ACCOUNT.blob.core.windows.net/CONTAINER[/PREFIX]`, lists a container's current blobs;
`--all-versions` lists every entry: blob versions, snapshots and soft-deleted blobs beside the current blobs.

**Flat namespace only.** Before listing, swath asks the account whether it has a hierarchical namespace
(Data Lake Storage Gen2), and refuses one that does, with exit 2 and cause `unsupported`. When the account
will not say (an anonymous caller is always refused that question), swath asks the account's Data Lake host,
which answers public containers; its refusal of a flat account that has blob soft delete or events on also
establishes a flat namespace. Only when neither establishes the namespace is the run refused too, unless
`--azure-assume-flat` asserts a flat namespace. The assertion is recorded for resume and shown in the report's `namespace`; an account that
reports a hierarchical namespace is refused whatever is asserted. A credential the service refuses during
these checks (an expired SAS, say) ends the run as an authentication failure (exit 1, cause `auth`).

**Permissions.** Permission to list the container's blobs: a SAS with the list permission, or an Entra ID
role that includes it. `--all-versions` needs no more: Azure lists versions, snapshots and soft-deleted blobs to
anyone who may list, anonymous callers of a public container included.

**Every entry** (`--all-versions`, schema `swath/azblob-versions/v1`). Each version, snapshot and soft-deleted
blob is a row, with `version_id`, `snapshot`, `is_deleted`, `deleted_time` and `remaining_retention_days`;
`is_latest` marks the row a current listing has. A name's rows are consecutive, in Azure's order: snapshots and
versions oldest first, the current blob last. Soft-deleted blobs exist only while the account's retention keeps
them (`remaining_retention_days`), so two listings days apart differ. A container without versions, snapshots or
deleted blobs lists the same rows as without the switch, every one `is_latest`. Each entry is a row, so a
container with a long history lists more pages, each a billed listing operation; the format does not change
(Arrow where the account has it).

**Properties.** Each row carries what List Blobs returns for every blob: besides size, times, ETag, MD5,
content type, blob type and access tier, whether the tier is inferred and when it last changed, a pending
rehydration from Archive (`archive_status`, `rehydrate_priority`), the last access time (only where the
account tracks it), content encoding, server encryption, encryption scope and customer-provided key hash,
lease state and tag count. These come in the same responses at no extra request; a column is null where
Azure lists nothing. Three switches ask for more, at the cost of larger responses:

| Switch | Fills | Needs |
|---|---|---|
| `--azure-fetch-metadata` | `metadata`: the user metadata as a JSON object, keys sorted (`{}` for none) | listing permission; up to 8 KiB a blob |
| `--azure-fetch-tags` | `tags`: the blob index tags as a JSON object (`{}` for none) | the right to read tags: a SAS with the `t` permission, or an Entra ID role with the tags read data action (Storage Blob Data Owner); never anonymous |
| `--azure-fetch-immutability` | `immutability_policy_until`, `immutability_policy_mode`, `legal_hold` | listing permission |

Without a switch its columns are null. Azure refuses a whole listing that asks for tags without the right to
read them (401 anonymously, 403 `AuthorizationPermissionMismatch` with a SAS); swath then stops with that
error and names the permission, and never lists without the tags it was asked for. A blob encrypted with a
customer-provided key lists no metadata: its `metadata` is null. The switches are part of the run's identity.

**Credentials,** in this order: `--anonymous`; a SAS from `--azure-sas-token`, else from the source URL's query
(`az://account/container?sv=...&sig=...`), else `AZURE_STORAGE_SAS_TOKEN`; an account key from
`AZURE_STORAGE_KEY` (Shared Key); else Entra ID through the Azure SDK's default chain (environment, workload
identity, managed identity, the Azure CLI and Developer CLI logins). A SAS is never recorded or shown: the
source is shown as `?sig=[redacted]`, and a resume needs the SAS again, by `--azure-sas-token` or
`AZURE_STORAGE_SAS_TOKEN`. A SAS holds only its own fields: one that also names a request parameter swath sets
(`comp`, `restype`, `prefix`, `marker` or another List Blobs parameter, in any case) is refused at startup with
exit 2, naming the parameter, before any request.

**Endpoint.** `--endpoint-url URL` replaces `https://ACCOUNT.blob.core.windows.net`, and the account is then
its first path segment (Azurite: `--endpoint-url http://127.0.0.1:10000 --azure-assume-flat`; Azurite does not report whether a namespace is hierarchical).

**Order.** Azure lists names in UTF-16 order, which differs from byte order for some names at U+E000 and
above (fullwidth forms, emoji). swath reorders them, so an Azure listing is in byte order like every other
source's. `--storage-class` filters on the access tier (`Hot`, `Cool`, `Cold`, `Archive`). Filters do not
narrow an Azure listing, and there is no cost estimate.

## Local and mounted trees

A path lists a directory tree, or a single file as its one row. Its rows are entries: files (every kind that
is not a directory: regular files, symbolic links, devices) and directories, each directory's key ending in
`/`. swath walks the tree through the operating system, so a mounted share (NFS, SMB, a FUSE mount) lists as
any directory does.

A SOURCE that is a file is read as the same entry in a listing of its parent: at the `--metadata` tier, and a
symbolic link as the link (kind `SYMLINK`, its target in `link_target`), never followed. Its key is its name.

| Option | Effect |
|---|---|
| `--metadata names\|stat\|statx` | what is read per entry: names only (no size or time), `stat` (the default), or `statx`, which adds `birth_time` (Linux) |
| `--no-dirs` | omit the directories' rows (they are still walked) |
| `--no-link-target` | do not read symbolic links' targets |
| `--one-file-system` | do not walk into a directory on another device (a mount point), as `du -x` and `find -xdev`; its row is kept |

`--no-dirs` and `--one-file-system` mark a dataset as not the whole tree (`coverage.filters` holds `no_dirs`
or `one_filesystem`). `--one-file-system`, `--allocated` and `--count-links-once` need each entry's stat:
they are refused with `--metadata names`.

The output may not be the listed tree itself or contain it. A new or empty directory inside the tree is
accepted as the output: the walk skips it and the dataset records it in `coverage.exclusions`. `--overwrite`
of an output inside the listed tree is refused (exit 2).

Entries that cannot be read (permissions, entries removed during the walk) are left out: swath names up to
five of them with their cause on stderr, counts the rest, and still exits 0; the report's
`outcome.unreadable` is then true.

A local listing keeps no journal: an interrupted one is run again. On a local tree `--workers` defaults to 4
per CPU, at most 64.

Names that are not valid UTF-8 are listed with their exact bytes: escaped in TSV and the table, as `key_base64`
in JSONL ([output.md](output.md#text-rows)).

## Object-store requests

S3, GCS and Azure Blob share the request options:

| Option | Default | Effect |
|---|---|---|
| `--request-timeout DURATION` | 30s | per-request timeout; a request over it is retried |
| `--request-rate N` | no cap | at most N HTTP attempts per second, retries and probes included |
| `--no-adaptive-limit` | the limit is on | do not lower concurrency when the service throttles |

A failed request that can be retried (a throttle, a timeout, a 5xx, a cut connection) is tried up to nine
times with exponential backoff (100 ms, at most 5 s apart); a listing page that still fails goes back to the
queue and is tried again later. `--stall-timeout` and `--max-duration` bound a run that cannot make progress.
Throttling and how to tune for it: [running.md](running.md#throttling).

More on each provider, for contributors: [S3](internals/providers/s3.md), [GCS](internals/providers/gcs.md),
[Azure Blob](internals/providers/azblob.md), [local trees](internals/providers/local.md).
