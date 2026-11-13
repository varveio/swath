# Azure Blob Storage

`az://ACCOUNT/CONTAINER[/PREFIX]`, or the same as `https://ACCOUNT.blob.core.windows.net/CONTAINER[/PREFIX]`,
lists a container's current blobs, or those whose names start with the prefix, in an account with a flat
namespace. `--all-versions` lists every entry: blob versions, snapshots and soft-deleted blobs beside the
current blobs.

## The smallest command

A public container of Azure Open Datasets, with no Azure account:

```bash
swath du az://azureopendatastorage/citydatacontainer/Crime/ -d 1 --anonymous
```

Without `--anonymous` swath uses your Azure credentials.

## Authentication and permissions

**Permissions.** Permission to list the container's blobs: a SAS with the list permission, or an Entra ID
role that includes it. `--all-versions` needs no more: Azure lists versions, snapshots and soft-deleted blobs to
anyone who may list, anonymous callers of a public container included.

**Credentials,** in this order: `--anonymous`; a SAS from `--azure-sas-token`, else from the source URL's query
(`az://account/container?sv=...&sig=...`), else `AZURE_STORAGE_SAS_TOKEN`; an account key from
`AZURE_STORAGE_KEY` (Shared Key); else Entra ID through the Azure SDK's default chain (environment, workload
identity, managed identity, the Azure CLI and Developer CLI logins). A SAS is never recorded or shown: the
source is shown as `?sig=[redacted]`, and a resume needs the SAS again, by `--azure-sas-token` or
`AZURE_STORAGE_SAS_TOKEN`. A SAS that also names a request parameter swath sets (`comp`, `restype`, `prefix`,
`marker` or another List Blobs parameter, in any case) is refused at startup with exit 2, naming the parameter.
Rules shared by every object store are in [Choose a source](README.md#credentials-on-every-object-store).

**Flat namespace only.** Before listing, swath asks the account whether it has a hierarchical namespace (Data
Lake Storage Gen2) and refuses one that does, with exit 2 and cause `unsupported`. When the account will not
say (an anonymous caller is always refused that question), swath asks the account's Data Lake host, whose
refusal of a flat account with blob soft delete or events on also establishes a flat namespace. Only when
neither establishes it is the run refused too, unless `--azure-assume-flat` asserts a flat namespace; an
account that reports a hierarchical namespace is refused whatever is asserted. A credential the service
refuses during these checks ends the run as an authentication failure (exit 1, cause `auth`).

## Options for this source

Each row carries what List Blobs returns for every blob at no extra request (size, times, ETag, MD5, content
type, blob type, access tier and its change time, archive status, last access time where the account tracks
it, encryption, lease state, tag count). Three switches ask for more, at the cost of larger responses; they
are part of the run's identity:

| Switch | Fills | Needs |
|---|---|---|
| `--azure-fetch-metadata` | `metadata`: the user metadata as a JSON object | listing permission |
| `--azure-fetch-tags` | `tags`: the blob index tags as a JSON object | a SAS with the `t` permission, or an Entra ID role that reads tags; never anonymous |
| `--azure-fetch-immutability` | `immutability_policy_until`, `immutability_policy_mode`, `legal_hold` | listing permission |

Azure refuses a whole listing that asks for tags without the right to read them; swath then stops with that
error and names the permission. `--storage-class` filters on the access tier (`Hot`, `Cool`, `Cold`,
`Archive`). `--endpoint-url URL` replaces `https://ACCOUNT.blob.core.windows.net`, and the account is then its
first path segment (Azurite: `--endpoint-url http://127.0.0.1:10000 --azure-assume-flat`).

**Every entry** (`--all-versions`, schema `swath/azblob-versions/v1`). Each version, snapshot and soft-deleted
blob is a row, with `version_id`, `snapshot`, `is_deleted`, `deleted_time` and `remaining_retention_days`;
`is_latest` marks the row a current listing has. A name's rows are consecutive, snapshots and versions oldest
first, the current blob last. Soft-deleted blobs exist only while the account's retention keeps them, so two
listings days apart differ.

## Writing a dataset to Azure Blob Storage

`swath list SOURCE -o az://ACCOUNT/CONTAINER/PREFIX --work-dir DIR` puts the dataset in a container and keeps
only spill and the run's metadata in `DIR` ([writing to a bucket](../production.md#writing-to-a-bucket)).

- **Credentials:** `--output-azure-sas-token TOKEN`, else Entra ID through `DefaultAzureCredential`, whatever
  the source's SAS, account key or `--anonymous`; account keys and connection strings are not taken for a
  destination (make a SAS from the key). The SAS is never recorded: give it to every `swath resume DIR`.
- **Permissions:** read, write and delete on the container's blobs (`sp=rwd`, a container SAS `sr=c`, or an
  account SAS for the blob service with `srt=o`); a container listing is not needed. An Entra ID identity needs
  a data role on the container or its account, `Storage Blob Data Contributor`, or a custom role with the
  blob `read`, `write` and `delete` data actions; a control-plane role such as `Contributor` is not enough.
- **Endpoint:** `--output-endpoint-url` (the blob service base with the account as its first path segment) is
  `https://ACCOUNT.blob.core.windows.net` by default, is never taken from the environment, and is fixed for the
  run. Entra ID needs an `https://` endpoint: an emulator is reached with a SAS.
- **Staged blocks need no rule:** blocks staged by an aborted, failed or killed upload are never a blob, and
  the service discards them a week after the last was staged. The orphans swath deletes are not always gone:
  with blob soft delete on, each is kept until the account's retention period expires; with blob versioning
  on, each becomes a previous version that stays billed until deleted, so add a lifecycle rule that deletes
  previous versions under the dataset prefixes (`daysAfterCreationGreaterThan` on `version`).
- **Entra ID data actions:** a custom role needs
  `Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read`, `.../blobs/write` (Put Blob, Put
  Block and Put Block List) and `.../blobs/delete` (orphans).

## Limits and known differences

- Hierarchical namespaces (Data Lake Storage Gen2) are not supported.
- Azure lists names in UTF-16 order; swath reorders them into byte order, as every other source's.
- Filters do not narrow an Azure listing, and the report has no cost estimate for it.
