# Google Cloud Storage

`gs://BUCKET[/PREFIX]` lists a bucket's current objects through the JSON API, or those whose names start with
the prefix; `--all-versions` lists every generation, live and noncurrent.

## The smallest command

A public Landsat prefix, with no Google account:

```bash
swath du gs://gcp-public-data-landsat/LC08/01/001/ -d 1 --anonymous
```

Without `--anonymous` swath uses your Google credentials.

## Authentication and permissions

**Permissions.** `storage.objects.list` on the bucket (the same with `--all-versions`). The token's scope is
read-only.

**Credentials.** `--gcs-credentials-file FILE` (a service-account or authorized-user JSON file), else
Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`, the gcloud credentials file, the metadata
server on Google Cloud. Credentials that cannot be found or loaded (no ADC anywhere, a
`GOOGLE_APPLICATION_CREDENTIALS` that names a missing file) stop the run before listing (`auth`). Rules shared
by every object store are in [Choose a source](README.md#credentials-on-every-object-store).

## Options for this source

- `--gcs-billing-project PROJECT` bills the requests to that project, for a requester-pays bucket. It is part
  of the run's identity.
- `--endpoint-url` replaces the JSON API's base URL, `https://storage.googleapis.com/storage/v1/` (for
  fake-gcs-server).
- `--all-versions` lists live and noncurrent generations; their metadata differs from S3's versions
  ([Running swath](../running.md#listing-every-version)).

GCS's XML API also answers S3 requests: `s3://` with `--endpoint-url https://storage.googleapis.com` and
`--s3-bearer-token-command 'gcloud auth print-access-token'` lists through it
([Amazon S3](s3.md#options-for-this-source)). The native `gs://` source is the one to prefer.

## Writing a dataset to GCS

`swath list SOURCE -o gs://BUCKET/PREFIX --work-dir DIR` puts the dataset in GCS and keeps only spill and the
run's metadata in `DIR` ([writing to a bucket](../production.md#writing-to-a-bucket)).

- **Credentials:** the destination's are its own: `--output-gcs-credentials-file FILE` (a service account key
  or authorized-user JSON) or Application Default Credentials, with the read-write scope. The source's
  `--anonymous` or credentials never apply to it. A rotated key file is given to `swath resume DIR` again.
- **Permissions:** `storage.objects.create`, `get` and `delete` on the prefix (delete removes this run's own
  orphans, by generation).
- **Endpoint:** `--output-endpoint-url`, as the SDK takes it, is fixed for the run.
- **Leftovers:** unfinished resumable upload sessions expire on their own; GCS keeps them a week. No
  lifecycle rule is needed for them.

## Limits and known differences

- Soft-deleted objects are not listed, with or without `--all-versions`.
- A prefix is a string prefix: `gs://bucket/logs` covers `logs/` and `logs-old/`.
- The run report estimates request cost at GCS's reference rate only with the default service endpoint.
