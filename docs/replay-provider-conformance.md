# Replay provider conformance matrix

This is the human-readable matrix for [issue #221](https://github.com/varveio/swath/issues/221)
within [epic #220](https://github.com/varveio/swath/issues/220). The
[machine-readable ledger](replay-provider-ledger.json) owns row IDs and evidence status;
the offline validator requires this matrix to match it.
`UNMEASURED` means no native-provider capture exists. `OBSERVED` requires a sanitized
native capture and its SHA-256 checksum. `REPLAY_MATCH` additionally requires an
independent replay test against that capture. Documentation, model reviews, local
emulators, and agreement with a future swath fetcher do not advance a row.

**Current release gate:** a bounded live campaign captured every case below against
dedicated GCS, Azure and S3 resources (see [Live campaign](#live-campaign)). A row is
`OBSERVED` when its sanitized capture is committed and `REPLAY_MATCH` only when replay,
serving the same corpus and walked with its own tokens, matched that capture. Every
other observed row is a pinned replay discrepancy in
`replay-provider-evidence/<provider>/replay-expectations.json`; the GCS and Azure
profiles stay provisional until those rows match. Each row is one case: one request
shape that settles one question. Azure rows name their service versions, and each
version is evidenced separately.

| ID | Provider | Feature or probe | Profile promise pending evidence | Capture SHA-256 | Replay test | State |
| --- | --- | --- | --- | --- | --- | --- |
| gcs-01a | GCS JSON v1 | `startOffset` equal to an existing name is inclusive | Provisional bounded list | — | — | UNMEASURED |
| gcs-01b | GCS JSON v1 | `endOffset` equal to an existing name is exclusive | Provisional bounded list | — | — | UNMEASURED |
| gcs-01c | GCS JSON v1 | Equal `startOffset` and `endOffset` on an existing name | Provisional bounded list | — | — | UNMEASURED |
| gcs-01d | GCS JSON v1 | `startOffset` in a gap between names | Provisional bounded list | — | — | UNMEASURED |
| gcs-01e | GCS JSON v1 | `endOffset` in a gap between names | Provisional bounded list | — | — | UNMEASURED |
| gcs-01f | GCS JSON v1 | Inverted bounds with `startOffset` after `endOffset` | Provisional bounded list | — | — | UNMEASURED |
| gcs-01g | GCS JSON v1 | Both offsets without a prefix | Provisional bounded list | — | — | UNMEASURED |
| gcs-01h | GCS JSON v1 | Offsets before and after every corpus name | Provisional bounded list | — | — | UNMEASURED |
| gcs-01i | GCS JSON v1 | Empty `startOffset` and `endOffset` values | Provisional bounded list | — | — | UNMEASURED |
| gcs-01j | GCS JSON v1 | `startOffset` equal to the prefix | Provisional bounded list | — | — | UNMEASURED |
| gcs-01k | GCS JSON v1 | `endOffset` equal to the prefix | Provisional bounded list | — | — | UNMEASURED |
| gcs-02a | GCS JSON v1 | Prefix with `startOffset` before it and `endOffset` inside it | Provisional bounded list | — | — | UNMEASURED |
| gcs-02b | GCS JSON v1 | Prefix with `startOffset` inside it and `endOffset` after it | Provisional bounded list | — | — | UNMEASURED |
| gcs-02c | GCS JSON v1 | Prefix with both offsets before it | Provisional bounded list | — | — | UNMEASURED |
| gcs-02d | GCS JSON v1 | Prefix with both offsets after it | Provisional bounded list | — | — | UNMEASURED |
| gcs-02e | GCS JSON v1 | Prefix that is a string prefix of longer names | Provisional bounded list | — | — | UNMEASURED |
| gcs-02f | GCS JSON v1 | Prefix equal to a full name with `endOffset` equal to that name | Provisional bounded list | — | — | UNMEASURED |
| gcs-03a | GCS JSON v1 | Delimiter `/` baseline over objects, subtrees, a folder marker and an empty segment | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03b | GCS JSON v1 | Delimiter with `startOffset` equal to a subtree prefix | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03c | GCS JSON v1 | Delimiter with `startOffset` inside a subtree with remaining descendants | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03d | GCS JSON v1 | Delimiter with `startOffset` inside a subtree after its last descendant | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03e | GCS JSON v1 | Delimiter with `startOffset` after a subtree, equal to an object | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03f | GCS JSON v1 | Delimiter with `endOffset` inside a subtree | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03g | GCS JSON v1 | Delimiter with `endOffset` equal to a subtree prefix | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03h | GCS JSON v1 | Delimiter with `endOffset` just after a folder marker | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03i | GCS JSON v1 | Delimiter `/` with a prefix ending in the delimiter | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03j | GCS JSON v1 | Delimiter `/` with a prefix that stops before the delimiter | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03k | GCS JSON v1 | Pipe delimiter that never occurs in names | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03l | GCS JSON v1 | Two-character delimiter `/1` | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03m | GCS JSON v1 | Two-character delimiter `-g` | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03n | GCS JSON v1 | Delimiter with both offsets cutting two subtrees | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03o | GCS JSON v1 | Empty `delimiter` value | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03p | GCS JSON v1 | `includeTrailingDelimiter=true` with delimiter `/` | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-03q | GCS JSON v1 | Delimiter without a prefix from a mid-corpus `startOffset` | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-04a | GCS JSON v1 | BMP, surrogate-neighbor and supplementary names in list order | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| gcs-04b | GCS JSON v1 | `startOffset` at U+E000 | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| gcs-04c | GCS JSON v1 | `endOffset` at U+10000 | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| gcs-04d | GCS JSON v1 | Prefix in NFC form with an NFD sibling | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| gcs-04e | GCS JSON v1 | Prefix differing only in case | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| gcs-04f | GCS JSON v1 | Supplementary-character `startOffset` percent-encoded in lowercase hex | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| gcs-05a | GCS JSON v1 | Raw `+` in a `prefix` value | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05b | GCS JSON v1 | `%2B` in a `prefix` value | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05c | GCS JSON v1 | `%20` in a `prefix` value | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05d | GCS JSON v1 | `%25` before `2B` | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05e | GCS JSON v1 | Malformed percent escape `%ZZ` beside a literal `a%ZZ` name | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05f | GCS JSON v1 | Trailing lone `%` | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05g | GCS JSON v1 | Escape decoding to invalid UTF-8 beside a U+FFFD name | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05h | GCS JSON v1 | Duplicate `prefix` with identical values | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05i | GCS JSON v1 | Duplicate `prefix` with different values | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05j | GCS JSON v1 | Percent-encoded `&`, `=` and `#` | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05k | GCS JSON v1 | Raw `?` inside a value | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05l | GCS JSON v1 | Fully encoded slashes `%2F` | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05m | GCS JSON v1 | Lowercase hex escape `%2b` | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05n | GCS JSON v1 | Parameter name without `=` | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05o | GCS JSON v1 | Every reserved ASCII character in names under one prefix | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05p | GCS JSON v1 | Trailing space in a prefix | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05q | GCS JSON v1 | Raw `=` inside a value | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-05r | GCS JSON v1 | Raw `=` inside a `startOffset` value | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-06a | GCS JSON v1 | `maxResults=0` on a three-object scope | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06b | GCS JSON v1 | `maxResults=0` on a 1,001-object scope | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06c | GCS JSON v1 | `maxResults=1` walk | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06d | GCS JSON v1 | `maxResults=1000` walk over exactly 1,000 objects | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06e | GCS JSON v1 | `maxResults=1001` over 1,001 objects | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06f | GCS JSON v1 | `maxResults=-1` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06g | GCS JSON v1 | `maxResults=abc` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06h | GCS JSON v1 | `maxResults=2147483648` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06i | GCS JSON v1 | `maxResults=9223372036854775808` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06j | GCS JSON v1 | `maxResults=1.5` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06k | GCS JSON v1 | `maxResults=+2` with a raw plus | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06l | GCS JSON v1 | `maxResults=002` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06m | GCS JSON v1 | Duplicate `maxResults` values | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06n | GCS JSON v1 | Empty `maxResults` value | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-06o | GCS JSON v1 | `maxResults=2147483647` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-07a | GCS JSON v1 | Delimiter walk with one entry per page over top-level subtrees | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07b | GCS JSON v1 | Default page size walk over 1,001 objects | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07c | GCS JSON v1 | Delimiter walk with two entries per page | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07d | GCS JSON v1 | Delimiter walk with offsets and one entry per page | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07e | GCS JSON v1 | Flat walk with four entries per page over six names | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07f | GCS JSON v1 | Prefix with no matching names | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07g | GCS JSON v1 | Malformed constant `pageToken` | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07h | GCS JSON v1 | Reused `pageToken` for the same query | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07i | GCS JSON v1 | `pageToken` from a different prefix | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07j | GCS JSON v1 | `pageToken` with a changed page size | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07k | GCS JSON v1 | `pageToken` from a flat walk with an added delimiter | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07l | GCS JSON v1 | `pageToken` with an added `startOffset` past the token position | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07m | GCS JSON v1 | Empty `pageToken` value | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07n | GCS JSON v1 | Flat walk with two entries per page over delimited names | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07o | GCS JSON v1 | `pageToken` from a delimiter walk with the delimiter removed | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07q | GCS JSON v1 | Flat walk with two entries per page over six names | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07r | GCS JSON v1 | Issued `pageToken` with its last character changed | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07s | GCS JSON v1 | Issued `pageToken` truncated by four characters | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07t | GCS JSON v1 | `pageToken` with an `endOffset` excluding every remaining name | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07u | GCS JSON v1 | `pageToken` with a prefix matching only names before it | Complete native-token walk required | — | — | UNMEASURED |
| gcs-07v | GCS JSON v1 | Issued `pageToken` with one decoded byte changed and re-encoded | Complete native-token walk required | — | — | UNMEASURED |
| gcs-08a | GCS JSON v1 | Names with control, whitespace, dot, markup and 1,024-byte length | Strict UTF-8 subset | — | — | UNMEASURED |
| gcs-08b | GCS JSON v1 | Delimiter listing over dot segments and trailing-slash names | Strict UTF-8 subset | — | — | UNMEASURED |
| gcs-08c | GCS JSON v1 | Listing of exploratory creation-probe names | Strict UTF-8 subset | — | — | UNMEASURED |
| gcs-09a | GCS JSON v1 | HTTP Java SDK flat auto-pagination over Unicode names | Must run unmodified with endpoint override | — | — | UNMEASURED |
| gcs-09b | GCS JSON v1 | HTTP Java SDK hierarchy auto-pagination | Must run unmodified with endpoint override | — | — | UNMEASURED |
| gcs-09c | GCS JSON v1 | HTTP Java SDK offsets with auto-pagination | Must run unmodified with endpoint override | — | — | UNMEASURED |
| gcs-10 | GCS JSON v1 | Retry-disabled one-attempt errors; normal SDK retries | Separate request counts | — | — | UNMEASURED |
| gcs-11a | GCS JSON v1 | Explicit `projection=full` under uniform bucket-level access | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11b | GCS JSON v1 | Explicit `projection=noAcl` | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11c | GCS JSON v1 | Unknown projection value | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11d | GCS JSON v1 | `prettyPrint=false` and `alt=json` | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11e | GCS JSON v1 | Nonempty `fields` partial response | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11f | GCS JSON v1 | `versions=true` | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11g | GCS JSON v1 | `matchGlob` | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11h | GCS JSON v1 | `softDeleted=true` with soft delete disabled | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11i | GCS JSON v1 | `includeFoldersAsPrefixes=true` on a flat bucket | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| gcs-11j | GCS JSON v1 | Named false forms of boolean options | Named defaults and transport forms only; other options refused | — | — | UNMEASURED |
| azure-01 | Azure Blob XML `2026-06-06` | Account accepts service version and echoes it | Explicit version only | — | — | UNMEASURED |
| azure-02 | Azure Blob XML `2026-10-06` | Account rollout and version echo | Version individually gated | — | — | UNMEASURED |
| azure-03a | Azure Blob XML | `startFrom` equal to an existing name is inclusive | Provisional flat list | — | — | UNMEASURED |
| azure-03b | Azure Blob XML | `startFrom` in a gap between names | Provisional flat list | — | — | UNMEASURED |
| azure-03c | Azure Blob XML | `startFrom` before the prefix | Provisional flat list | — | — | UNMEASURED |
| azure-03d | Azure Blob XML | `startFrom` after the prefix | Provisional flat list | — | — | UNMEASURED |
| azure-03e | Azure Blob XML | `startFrom` equal to the prefix | Provisional flat list | — | — | UNMEASURED |
| azure-03f | Azure Blob XML | `startFrom` without a prefix | Provisional flat list | — | — | UNMEASURED |
| azure-03g | Azure Blob XML | Empty `startFrom` value | Provisional flat list | — | — | UNMEASURED |
| azure-03h | Azure Blob XML | `endBefore` | Provisional flat list | — | — | UNMEASURED |
| azure-03i | Azure Blob XML | `startFrom` with `endBefore` | Provisional flat list | — | — | UNMEASURED |
| azure-03j | Azure Blob XML | Prefix that is a string prefix of longer names | Provisional flat list | — | — | UNMEASURED |
| azure-03k | Azure Blob XML | `startFrom` inside a prefix that is a string prefix | Provisional flat list | — | — | UNMEASURED |
| azure-04a | Azure Blob XML | Delimiter `/` baseline over objects, subtrees, a folder marker and an empty segment | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04b | Azure Blob XML | Delimiter with `startFrom` equal to a subtree prefix | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04c | Azure Blob XML | Delimiter with `startFrom` inside a subtree with remaining descendants | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04d | Azure Blob XML | Delimiter with `startFrom` inside a subtree after its last descendant | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04e | Azure Blob XML | Delimiter with `startFrom` after a subtree, equal to an object | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04f | Azure Blob XML | Delimiter `/` with a prefix ending in the delimiter | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04g | Azure Blob XML | Delimiter `/` with a prefix that stops before the delimiter | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04h | Azure Blob XML | Pipe delimiter that never occurs in names | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04i | Azure Blob XML | Two-character delimiter `/1` | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04j | Azure Blob XML | Two-character delimiter `-g` | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04k | Azure Blob XML | Empty `delimiter` value | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04l | Azure Blob XML | Delimiter without a prefix from a mid-corpus `startFrom` | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-04m | Azure Blob XML | Delimiter walk with `startFrom` inside a subtree | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-05a | Azure Blob XML | Walk with `startFrom` and one entry per page | Matching `startFrom` retained on continuation; other combinations refused pending evidence | — | — | UNMEASURED |
| azure-05b | Azure Blob XML | `marker` with a later `startFrom` | Matching `startFrom` retained on continuation; other combinations refused pending evidence | — | — | UNMEASURED |
| azure-05c | Azure Blob XML | `marker` with an earlier `startFrom` | Matching `startFrom` retained on continuation; other combinations refused pending evidence | — | — | UNMEASURED |
| azure-05d | Azure Blob XML | `marker` issued under `startFrom` used without it | Matching `startFrom` retained on continuation; other combinations refused pending evidence | — | — | UNMEASURED |
| azure-06a | Azure Blob XML | BMP, surrogate-neighbor and supplementary names in list order | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| azure-06b | Azure Blob XML | `startFrom` at U+E000 | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| azure-06c | Azure Blob XML | Prefix in NFC form with an NFD sibling | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| azure-06d | Azure Blob XML | Prefix differing only in case | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| azure-06e | Azure Blob XML | Supplementary-character `startFrom` in lowercase hex escapes | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| azure-06f | Azure Blob XML | Delimiter `é` over Unicode names | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| azure-07a | Azure Blob XML | Raw `+` in a `prefix` value | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07b | Azure Blob XML | `%2B` in a `prefix` value | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07c | Azure Blob XML | `%20` in a `prefix` value | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07d | Azure Blob XML | `%25` before `2B` | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07e | Azure Blob XML | Malformed percent escape `%ZZ` beside a literal `a%ZZ` name | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07f | Azure Blob XML | Trailing lone `%` | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07g | Azure Blob XML | Escape decoding to invalid UTF-8 beside a U+FFFD name | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07h | Azure Blob XML | Duplicate `prefix` with identical values | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07i | Azure Blob XML | Duplicate `prefix` with different values | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07j | Azure Blob XML | Percent-encoded `&`, `=` and `#` | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07k | Azure Blob XML | Raw `?` inside a value | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07l | Azure Blob XML | Fully encoded slashes `%2F` | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07m | Azure Blob XML | Lowercase hex escape `%2b` | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07n | Azure Blob XML | Parameter name without `=` | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07o | Azure Blob XML | Every reserved ASCII character in names under one prefix | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07p | Azure Blob XML | Trailing space in a prefix | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07q | Azure Blob XML | Raw `=` inside a value | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07r | Azure Blob XML | Names with control, XML-invalid, whitespace and dot characters | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07s | Azure Blob XML | Delimiter listing over dot segments and trailing-slash names | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-07t | Azure Blob XML | Raw `+` in a `startFrom` value | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-08a | Azure Blob XML | `maxresults=0` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08c | Azure Blob XML | `maxresults=1` walk | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08d | Azure Blob XML | `maxresults=5000` walk over exactly 5,000 blobs | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08e | Azure Blob XML | `maxresults=5001` walk over 5,001 blobs | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08f | Azure Blob XML | `maxresults=-1` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08g | Azure Blob XML | `maxresults=abc` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08h | Azure Blob XML | `maxresults=2147483648` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08i | Azure Blob XML | `maxresults=9223372036854775808` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08j | Azure Blob XML | `maxresults=1.5` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08k | Azure Blob XML | `maxresults=+2` with a raw plus | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08l | Azure Blob XML | `maxresults=002` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08m | Azure Blob XML | Duplicate `maxresults` values | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08n | Azure Blob XML | Empty `maxresults` value | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08o | Azure Blob XML | `maxresults=2147483647` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-08p | Azure Blob XML | Duplicate `restype` selector | Zero refused pending evidence | — | — | UNMEASURED |
| azure-09a | Azure Blob XML | Delimiter walk with one entry per page over top-level subtrees | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09b | Azure Blob XML | Default page size walk over 5,001 blobs | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09c | Azure Blob XML | Delimiter walk with two entries per page | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09e | Azure Blob XML | Flat walk with four entries per page over six names | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09f | Azure Blob XML | Prefix with no matching names | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09g | Azure Blob XML | Malformed constant `marker` | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09h | Azure Blob XML | Reused `marker` for the same query | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09i | Azure Blob XML | `marker` from a different prefix | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09j | Azure Blob XML | `marker` with a changed page size | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09k | Azure Blob XML | `marker` from a flat walk with an added delimiter | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09l | Azure Blob XML | Empty `marker` value | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09n | Azure Blob XML | Flat walk with two entries per page over delimited names | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09o | Azure Blob XML | `marker` from a delimiter walk with the delimiter removed | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09q | Azure Blob XML | Flat walk with two entries per page over six names | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09r | Azure Blob XML | Issued `marker` with its last character changed | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09s | Azure Blob XML | Issued `marker` truncated by four characters | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09t | Azure Blob XML | `marker` with a prefix matching only names before it | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09u | Azure Blob XML | `marker` with an `endBefore` excluding every remaining name | Complete native-marker walk required | — | — | UNMEASURED |
| azure-09v | Azure Blob XML | Issued `marker` with one decoded byte changed and re-encoded | Complete native-marker walk required | — | — | UNMEASURED |
| azure-10a | Azure Blob XML `2026-06-06` | Java SDK flat auto-pagination | Must run unmodified with endpoint override | — | — | UNMEASURED |
| azure-10b | Azure Blob XML `2026-06-06` | Java SDK hierarchy auto-pagination | Must run unmodified with endpoint override | — | — | UNMEASURED |
| azure-10c | Azure Blob XML `2026-06-06` | Java SDK `setStartFrom` auto-pagination | Must run unmodified with endpoint override | — | — | UNMEASURED |
| azure-10d | Azure Blob XML `2026-10-06` | Java SDK auto-pagination at 2026-10-06 | Requires a pinned SDK that names the version | — | — | UNMEASURED |
| azure-11 | Azure Blob XML | Retry-disabled one-attempt errors; normal SDK retries | Separate request counts | — | — | UNMEASURED |
| azure-12a | Azure Blob XML | Listing of length and segment creation-probe names | Provisional 1,024 UTF-16 units and 254 segments; native creation probes required | — | — | UNMEASURED |
| azure-13a | Azure Blob XML | Explicit client request ID echo | Echo explicit client request ID; XML accept only | — | — | UNMEASURED |
| azure-13b | Azure Blob XML | Explicit `Accept: application/xml` | Echo explicit client request ID; XML accept only | — | — | UNMEASURED |
| azure-14a | Azure Blob XML | `include=metadata` | Empty or omitted options only; expansions refused | — | — | UNMEASURED |
| azure-14b | Azure Blob XML | Empty `include` value | Empty or omitted options only; expansions refused | — | — | UNMEASURED |
| azure-14c | Azure Blob XML | `include=deleted` with soft delete disabled | Empty or omitted options only; expansions refused | — | — | UNMEASURED |
| azure-14d | Azure Blob XML | `timeout=1` | Empty or omitted options only; expansions refused | — | — | UNMEASURED |
| azure-14e | Azure Blob XML | `timeout=0` | Empty or omitted options only; expansions refused | — | — | UNMEASURED |
| azure-14f | Azure Blob XML | `timeout=abc` | Empty or omitted options only; expansions refused | — | — | UNMEASURED |
| azure-14g | Azure Blob XML | `showonly=files` on a flat account | Empty or omitted options only; expansions refused | — | — | UNMEASURED |
| s3-01a | Amazon S3 ListObjectsV2 | `start-after` equal to an existing key is exclusive | Exclusive `start-after`, intersected with prefix | — | — | UNMEASURED |
| s3-01b | Amazon S3 ListObjectsV2 | `start-after` in a gap between keys | Exclusive `start-after`, intersected with prefix | — | — | UNMEASURED |
| s3-01c | Amazon S3 ListObjectsV2 | `start-after` before the prefix | Exclusive `start-after`, intersected with prefix | — | — | UNMEASURED |
| s3-01d | Amazon S3 ListObjectsV2 | `start-after` after the prefix | Exclusive `start-after`, intersected with prefix | — | — | UNMEASURED |
| s3-01e | Amazon S3 ListObjectsV2 | `start-after` equal to the prefix | Exclusive `start-after`, intersected with prefix | — | — | UNMEASURED |
| s3-01f | Amazon S3 ListObjectsV2 | `start-after` without a prefix | Exclusive `start-after`, intersected with prefix | — | — | UNMEASURED |
| s3-01g | Amazon S3 ListObjectsV2 | Empty `start-after` value | Exclusive `start-after`, intersected with prefix | — | — | UNMEASURED |
| s3-02a | Amazon S3 ListObjectsV2 | Prefix that is a string prefix of longer keys | Byte prefix match | — | — | UNMEASURED |
| s3-02b | Amazon S3 ListObjectsV2 | Prefix with no matching keys | Byte prefix match | — | — | UNMEASURED |
| s3-03a | Amazon S3 ListObjectsV2 | Delimiter `/` baseline over keys, subtrees, a folder marker and an empty segment | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03b | Amazon S3 ListObjectsV2 | Delimiter with `start-after` equal to a subtree prefix | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03c | Amazon S3 ListObjectsV2 | Delimiter with `start-after` inside a subtree with remaining descendants | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03d | Amazon S3 ListObjectsV2 | Delimiter with `start-after` inside a subtree after its last descendant | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03e | Amazon S3 ListObjectsV2 | Delimiter with `start-after` after a subtree, equal to a key | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03f | Amazon S3 ListObjectsV2 | Delimiter `/` with a prefix ending in the delimiter | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03g | Amazon S3 ListObjectsV2 | Delimiter `/` with a prefix that stops before the delimiter | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03h | Amazon S3 ListObjectsV2 | Pipe delimiter that never occurs in keys | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03i | Amazon S3 ListObjectsV2 | Two-character delimiter `/1` | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03j | Amazon S3 ListObjectsV2 | Two-character delimiter `-g` | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03k | Amazon S3 ListObjectsV2 | Empty `delimiter` value | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-03l | Amazon S3 ListObjectsV2 | Delimiter without a prefix from a mid-corpus `start-after` | CommonPrefixes rollup; start-after suppression per existing S3 goldens | — | — | UNMEASURED |
| s3-04a | Amazon S3 ListObjectsV2 | BMP, surrogate-neighbor and supplementary keys in list order | Unsigned UTF-8 order | — | — | UNMEASURED |
| s3-04b | Amazon S3 ListObjectsV2 | `start-after` at U+E000 | Unsigned UTF-8 order | — | — | UNMEASURED |
| s3-04c | Amazon S3 ListObjectsV2 | Prefix in NFC form with an NFD sibling | Unsigned UTF-8 order | — | — | UNMEASURED |
| s3-04d | Amazon S3 ListObjectsV2 | Prefix differing only in case | Unsigned UTF-8 order | — | — | UNMEASURED |
| s3-04e | Amazon S3 ListObjectsV2 | Unicode keys with `encoding-type=url` | Unsigned UTF-8 order | — | — | UNMEASURED |
| s3-05a | Amazon S3 ListObjectsV2 | Raw `+` in a `prefix` value | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05b | Amazon S3 ListObjectsV2 | `%2B` in a `prefix` value | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05c | Amazon S3 ListObjectsV2 | `%20` in a `prefix` value | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05d | Amazon S3 ListObjectsV2 | `%25` before `2B` | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05e | Amazon S3 ListObjectsV2 | Malformed percent escape `%ZZ` beside a literal `a%ZZ` name | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05f | Amazon S3 ListObjectsV2 | Trailing lone `%` | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05g | Amazon S3 ListObjectsV2 | Escape decoding to invalid UTF-8 beside a U+FFFD name | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05h | Amazon S3 ListObjectsV2 | Duplicate `prefix` with identical values | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05i | Amazon S3 ListObjectsV2 | Duplicate `prefix` with different values | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05j | Amazon S3 ListObjectsV2 | Percent-encoded `&`, `=` and `#` | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05k | Amazon S3 ListObjectsV2 | Raw `?` inside a value | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05l | Amazon S3 ListObjectsV2 | Fully encoded slashes `%2F` | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05m | Amazon S3 ListObjectsV2 | Lowercase hex escape `%2b` | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05n | Amazon S3 ListObjectsV2 | Parameter name without `=` | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05o | Amazon S3 ListObjectsV2 | Every reserved ASCII character in names under one prefix | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05p | Amazon S3 ListObjectsV2 | Trailing space in a prefix | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05q | Amazon S3 ListObjectsV2 | Raw `=` inside a value | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-05r | Amazon S3 ListObjectsV2 | Raw `+` in a `start-after` value | Lenient S3 query decoding preserved from existing replay | — | — | UNMEASURED |
| s3-06a | Amazon S3 ListObjectsV2 | `max-keys=0` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06b | Amazon S3 ListObjectsV2 | `max-keys=0` over 1,001 keys | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06c | Amazon S3 ListObjectsV2 | `max-keys=1` walk | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06d | Amazon S3 ListObjectsV2 | `max-keys=1000` walk over exactly 1,000 keys | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06e | Amazon S3 ListObjectsV2 | `max-keys=1001` over 1,001 keys | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06f | Amazon S3 ListObjectsV2 | `max-keys=-1` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06g | Amazon S3 ListObjectsV2 | `max-keys=abc` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06h | Amazon S3 ListObjectsV2 | `max-keys=2147483648` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06i | Amazon S3 ListObjectsV2 | `max-keys=9223372036854775808` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06j | Amazon S3 ListObjectsV2 | `max-keys=1.5` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06k | Amazon S3 ListObjectsV2 | `max-keys=+2` with a raw plus | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06l | Amazon S3 ListObjectsV2 | `max-keys=002` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06m | Amazon S3 ListObjectsV2 | Duplicate `max-keys` values | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06n | Amazon S3 ListObjectsV2 | Empty `max-keys` value | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-06o | Amazon S3 ListObjectsV2 | `max-keys=2147483647` | max-keys 0 to 1,000 with cap; malformed values refused | — | — | UNMEASURED |
| s3-07a | Amazon S3 ListObjectsV2 | Delimiter walk with one entry per page over top-level subtrees | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07b | Amazon S3 ListObjectsV2 | Default page size walk over 1,001 keys | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07c | Amazon S3 ListObjectsV2 | Delimiter walk with two entries per page | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07d | Amazon S3 ListObjectsV2 | Delimiter walk with `start-after` and one entry per page | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07e | Amazon S3 ListObjectsV2 | Flat walk with four entries per page over six keys | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07g | Amazon S3 ListObjectsV2 | Malformed constant `continuation-token` | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07h | Amazon S3 ListObjectsV2 | Reused `continuation-token` for the same query | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07i | Amazon S3 ListObjectsV2 | `continuation-token` from a different prefix | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07j | Amazon S3 ListObjectsV2 | `continuation-token` with a changed page size | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07k | Amazon S3 ListObjectsV2 | `continuation-token` from a flat walk with an added delimiter | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07n | Amazon S3 ListObjectsV2 | Flat walk with two entries per page over delimited keys | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07o | Amazon S3 ListObjectsV2 | `continuation-token` from a delimiter walk with the delimiter removed | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07q | Amazon S3 ListObjectsV2 | Flat walk with two entries per page over six keys | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07r | Amazon S3 ListObjectsV2 | Issued `continuation-token` with its last character changed | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07s | Amazon S3 ListObjectsV2 | Issued `continuation-token` truncated by four characters | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07t | Amazon S3 ListObjectsV2 | `continuation-token` with a prefix matching only keys before it | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07u | Amazon S3 ListObjectsV2 | `continuation-token` with a `start-after` before the token position | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07v | Amazon S3 ListObjectsV2 | `continuation-token` with a `start-after` after the token position | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07w | Amazon S3 ListObjectsV2 | Empty `continuation-token` value | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-07x | Amazon S3 ListObjectsV2 | Issued `continuation-token` with one decoded byte changed and re-encoded | Complete native-token walk; replay-owned continuation tokens | — | — | UNMEASURED |
| s3-08a | Amazon S3 ListObjectsV2 | Keys with control, XML-invalid, whitespace and dot characters without encoding | encoding-type=url encodes Key, Prefix, Delimiter, StartAfter and CommonPrefixes | — | — | UNMEASURED |
| s3-08b | Amazon S3 ListObjectsV2 | The same keys with `encoding-type=url` | encoding-type=url encodes Key, Prefix, Delimiter, StartAfter and CommonPrefixes | — | — | UNMEASURED |
| s3-08c | Amazon S3 ListObjectsV2 | Delimiter listing with `encoding-type=url` and special-character prefix and start-after | encoding-type=url encodes Key, Prefix, Delimiter, StartAfter and CommonPrefixes | — | — | UNMEASURED |
| s3-08d | Amazon S3 ListObjectsV2 | Unknown `encoding-type` value | encoding-type=url encodes Key, Prefix, Delimiter, StartAfter and CommonPrefixes | — | — | UNMEASURED |
| s3-08e | Amazon S3 ListObjectsV2 | Reserved-character keys with `encoding-type=url` | encoding-type=url encodes Key, Prefix, Delimiter, StartAfter and CommonPrefixes | — | — | UNMEASURED |
| s3-08f | Amazon S3 ListObjectsV2 | Creation-probe keys at and over length limits | encoding-type=url encodes Key, Prefix, Delimiter, StartAfter and CommonPrefixes | — | — | UNMEASURED |
| s3-09a | Amazon S3 ListObjectsV2 | Java SDK v2 ListObjectsV2 paginator over Unicode keys | Must run unmodified with endpoint override | — | — | UNMEASURED |
| s3-09b | Amazon S3 ListObjectsV2 | Java SDK v2 paginator with a delimiter | Must run unmodified with endpoint override | — | — | UNMEASURED |
| s3-09c | Amazon S3 ListObjectsV2 | Java SDK v2 paginator with `startAfter` | Must run unmodified with endpoint override | — | — | UNMEASURED |
| s3-10 | Amazon S3 ListObjectsV2 | Retry-disabled one-attempt errors; normal SDK retries | Separate request counts | — | — | UNMEASURED |
| s3-11a | Amazon S3 ListObjectsV2 | `fetch-owner=true` | Synthetic owner when requested; optional attributes refused | — | — | UNMEASURED |
| s3-11b | Amazon S3 ListObjectsV2 | `fetch-owner=false` | Synthetic owner when requested; optional attributes refused | — | — | UNMEASURED |
| s3-11c | Amazon S3 ListObjectsV2 | `fetch-owner=yes` | Synthetic owner when requested; optional attributes refused | — | — | UNMEASURED |
| s3-11d | Amazon S3 ListObjectsV2 | `x-amz-optional-object-attributes: RestoreStatus` header | Synthetic owner when requested; optional attributes refused | — | — | UNMEASURED |
| s3-12a | Amazon S3 ListObjectsV2 | ListObjects V1 with `marker` equal to an existing key | ListObjectsV2 only; V1 recorded for discrimination | — | — | UNMEASURED |
| s3-12b | Amazon S3 ListObjectsV2 | ListObjects V1 delimiter page with `max-keys=1` | ListObjectsV2 only; V1 recorded for discrimination | — | — | UNMEASURED |
| s3-12c | Amazon S3 ListObjectsV2 | `list-type=3` | ListObjectsV2 only; V1 recorded for discrimination | — | — | UNMEASURED |
| s3-12d | Amazon S3 ListObjectsV2 | Duplicate `list-type` | ListObjectsV2 only; V1 recorded for discrimination | — | — | UNMEASURED |
| s3-12e | Amazon S3 ListObjectsV2 | V2 with `marker` | ListObjectsV2 only; V1 recorded for discrimination | — | — | UNMEASURED |

The corpus manifest must record exact object names, content SHA-256 values, account
namespace mode (GCS flat with uniform bucket-level access; Azure flat with HNS off),
region, API and pinned SDK versions, capture date, probe request, expected result,
and sanitized capture checksum. Keep credentials and private infrastructure
identifiers out of committed captures. A capture checksum is calculated **after**
sanitization. Every token walk uses the token returned by its own endpoint; tokens
and page boundaries are not compared byte for byte. Compare complete object and
prefix inventories, order, status, and applicable metadata instead.

Offline SDK smoke tests use official `google-cloud-storage:2.73.0` over HTTP/JSON and
`azure-storage-blob:12.35.1` with explicit `V2026_06_06` and
`azure-core-http-jdk-httpclient:1.1.5`. The GCS SDK's local requests send
`projection=full` and no `fields`; the Azure SDK's local requests send
`x-ms-version: 2026-06-06`, retain `startFrom` on continuation, and transmit `/`
literally in delimiter and startFrom query values. These are client-side request
observations against replay, not native-provider behavior. Both clients complete
auto-paginated local walks; retry-disabled 503 recipes each assert one attempt.

| Feature/error | GCS JSON v1 provisional behavior | Azure Blob XML provisional behavior |
| --- | --- | --- |
| Page capacity | Default 1,000; larger valid requests cap at 1,000; zero refused | Default 5,000; signed-int32 values above 5,000 cap at 5,000; zero, negative and overflow refused |
| Range | Inclusive `startOffset`, exclusive `endOffset`, intersected with prefix | Inclusive `startFrom`; `endBefore` refused for XML |
| Delimiter | Objects and unique prefixes consume one capacity each; offset intersections remain evidence-gated | Interleaved Blob and BlobPrefix consume capacity; `startFrom` plus delimiter refused until observed |
| Marker | Replay-owned `gcs1.` token bound to fixture and listing scope | Replay-owned `az1.` marker bound to fixture, namespace, version and listing scope; matching startFrom retained |
| Unsupported features | Versions, soft deletion, glob/filter evaluation, ACL expansion and partial fields | HNS/ADLS, versions, snapshots, deleted/include expansions, Arrow and `endBefore` |
| Client error | JSON `invalid` 400; not-found 404; wrong-method 405 | XML `InvalidQueryParameterValue` / `OutOfRangeQueryParameterValue` / `InvalidHeaderValue` 400; `ContainerNotFound` 404 |
| Shared serving failure | JSON `backendError` 503 or `internalError` 500 | XML `ServerBusy` 503 or `InternalError` 500 |

Completed replay metrics expose `swath.replay.provider.path` with protocol, path,
and reason tags. GCS records a requested `maxResults` above 1,000 as
`page_limit_clamped/requested_above_1000`. A fixture value that cannot be rendered
under the GCS profile records `fixture_rejected` with a bounded reason such as
`invalid_utf8_name` or `negative_size`; the client receives a provider-shaped 500.
These counters describe replay behavior and do not advance any native evidence row.

| Field class | GCS JSON v1 | Azure Blob XML | Comparison rule |
| --- | --- | --- | --- |
| Exact | Object name, size, timestamp instant at fixture microsecond precision, ordering and bounds | Blob/prefix name text, order, size, Last-Modified instant at second precision, `Encoded` attribute, version/error behavior | Compare membership, type, value, and order; preserve absent versus empty; require canonical replay timestamp spelling |
| Synthetic typed | ETag, generation, metageneration, storage class, content type, full-projection empty ACL | ETag, blob type, tier, lease state/status | Assert replay profile value and type; assert native field validity without asserting native value identity |
| Profile omitted | Creation fields, checksums, owner, no-ACL projection ACL, native links | Creation fields, encryption, empty optional content properties | Native may contain these; replay must omit them |
| Namespace mapped | Bucket origin and self links where supported | `ServiceEndpoint` and `ContainerName` | Map explicit test namespace to replay namespace; verify URL shape and trailing slash |
| Opaque | `nextPageToken` | `NextMarker` | Verify terminal absence/emptiness and completed walk; echoed Azure `Marker` equals that endpoint's own request token |
| Named volatile | Request IDs (`x-goog-request-id`, `x-guploader-uploadid`), `x-goog-gcs-base-ts` and HTTP `Date` | Request IDs and HTTP `Date` | Validate spelling/nonempty value when present; do not compare values or require matching presence |
| Classified transport | `server`, `cache-control`, repeated `vary`, `expires`, `pragma`, `alt-svc`, `transfer-encoding`, `content-length` | `server`, `transfer-encoding`, `content-length` | Require classified nonempty values; validate Content-Length against each sanitized body, not across providers; do not compare transport values |
| Replay diagnostic | `x-swath-replay-error` | `x-swath-replay-error` | Replay-only reason; nonempty when present, never equated to a native header |

Unknown response fields or attributes require review in the future capture campaign.
Missing objects, boundary errors, wrong types, duplicate prefixes, and encoded-name
changes must become comparison failures before conformance can advance. Offline
`GcsProfileComparator` and `AzureProfileComparator` tests now enforce field types,
exact membership/order, namespace mapping, synthetic profile values, and omitted
metadata against adversarial sample pages. Native captures still need to exercise
every row before any behavior becomes `OBSERVED`. The parsers and bounded token walker live in
`swath-replay/src/conformance/java/io/varve/swath/replay/conformance/provider/`;
they do not call replay pager logic.

`ProviderExchangeComparator` adds status, classified header and error-envelope
checks to those body policies. It checks GCS JSON error reasons, Azure XML and
`x-ms-error-code`, the requested Azure service version and client request ID,
content type, and body length. Date and provider request IDs are validated but
their values are volatile; Content-Type media types are compared while charset
parameters may differ. Azure BOM and XML prolog spelling are compared as raw
preamble bytes. Sanitization keeps only classified response headers, hashes
provider request IDs, and adjusts retained Content-Length to sanitized body bytes;
an unclassified response header halts the capture for review. Local tests exercise
these rules; native status, header, and error behavior remains UNMEASURED.
GCS JSON sanitization re-encodes its body, so the `gcs-08` raw-spelling row cannot
advance without a separately reviewed, sanitized raw-wire artifact and checksum.
Native captures may establish that a provider repeats a rolled prefix across pages;
that observation can be recorded without changing the profile. The current replay
comparison refuses a repeated GCS prefix or Azure BlobPrefix explicitly. It never
silently deduplicates one to make a walk appear to match. Azure walk checks each
page's Marker echo against that page's own requested marker and checks Prefix,
Delimiter, and MaxResults echoes against their request values; page size may change
between pages while Prefix and Delimiter remain bound to the same scope.

`scripts/provider-conformance/evidence.py` validates a corpus manifest, the checked-in
ledger and this matrix, and sanitizes independently captured HTTP evidence before
calculating its SHA-256.
For an `exchange` row its input is one JSON exchange with `request.method`, absolute `request.url`,
header name/value pairs, and `response.status`, headers, and `body_base64`. Run
`evidence.py sanitize --input RAW --output SAFE --provider gcs --bucket LIVE_BUCKET
--manifest MANIFEST --probe-id gcs-01` or substitute
`--provider azure --account LIVE_ACCOUNT --container LIVE_CONTAINER` for Azure.
The raw input must also contain `captured_at` in ISO form. The script retains only
named listing headers/query fields, removes the origin, replaces namespace fields
structurally, hashes opaque tokens so consecutive requests can still be linked,
and refuses unknown query fields so a SAS parameter cannot slip into a capture.
For `walk`, `sdk_run`, and `retry_run` rows, pass `--kind` and a raw JSON object
with `steps: [{"phase":"walk","exchange":{...}}, ...]`. Retry runs use one
`retry_disabled` step and at least two `normal_retry` steps. The sanitizer
checks complete, bounded native token chains and emits `provider-capture-run-v1`;
the ledger refuses a single exchange for those rows. Each sanitized request
retains a SHA-256 of the raw token spelling as well as its strict-decoded opaque
token digest. Malformed percent escapes and invalid UTF-8 tokens are refused.
It retains only classified SDK product/version tokens from User-Agent and
x-goog-api-client, removing platform, host and project strings. SDK rows require a
captured token exactly matching the manifest's `sdk_product/sdk_version`.
Azure native captures must use the real account host and `/{container}` path over
HTTPS. The sanitizer checks the raw `ServiceEndpoint` scheme and trailing slash,
then replaces only the account label; it also hashes client request IDs while
checking the response echoed the request value. Error messages/details that can
carry request IDs are redacted without removing the typed error code or changing
the native XML BOM and declaration. Azure SDK replay receipts require the same
explicit client request ID on the captured and replay request, with each response
echo checked independently.
Azure `HEAD` 4xx/5xx error probes retain an empty body and require exactly one
nonempty typed `x-ms-error-code`; they compare status, service version and
classified headers. Nonempty `HEAD` bodies and `HEAD` success
captures are outside this provisional evidence profile. `GET` errors still require
their typed XML envelope and native preamble.
SDK run receipts also require every retained native product/version token to appear
in the matching replay request header. Use the same pinned SDK and JDK for both
runs; a changed runtime or transport may alter auxiliary SDK tokens even when
the listing response is unchanged.
`AzureNamespaceMapping` compares that sanitized native host-style path and
ServiceEndpoint with replay's `/{replay-account}/{container}` path and local
`/{replay-account}/` ServiceEndpoint explicitly; it does not infer equivalence
from a user agent or authentication header.
It never rewrites object names: if a private identifier occurs in a name, sanitization
fails for manual review. Review the sanitized output for additional private data before
publishing it; the printed SHA-256 identifies those exact sanitized bytes.

Run `evidence.py validate --ledger docs/replay-provider-ledger.json
--matrix docs/replay-provider-conformance.md` to check the current UNMEASURED rows.
Promotion to OBSERVED also requires the provider's `--manifest MANIFEST`, a capture
under `--captures DIR`, exact probe request and expected result, and a checksum that
matches that capture's bytes and manifest link. REPLAY_MATCH additionally requires
`--repo-root DIR`, an existing test method, and a checksummed passing result receipt
linked to the same capture. `ProviderMatchReceipt` emits a PASS receipt only after
its GCS or Azure exchange or complete-run comparison succeeds, including the
captured and replay request scopes after explicit namespace and continuation-token
mapping. It requires a clean tested source tree and binds the test source, Git
commit and distribution SHA-256. Validation accepts a later commit only when the
receipt commit is an ancestor and the test source and distribution hashes still
match; pass `--distribution REPLAY_JAR` to
validate against that binary. Structural validation does not replace reviewing
the provider exchange or rerunning the named test.
The bundled rows gcs-01/03/06 and azure-04/08/12 were split into case-specific rows;
the validator still refuses to promote a bundled legacy ID.

Azure replay fixture timestamps are provisionally representable only in UTC
years 1 through 9999; out-of-range values receive a typed fixture rejection.
This is a replay encoding bound, not an observation about native Azure limits.

## Live campaign

`scripts/provider-conformance/campaign.py` provisions a corpus inside a fresh random
`probe/r<UTC time>-<hex>/` prefix of a dedicated bucket or container, captures every
case of the provider's matrix (`gcs_live.py`, `azure_live.py`, `s3_live.py`), runs the
official SDK walks through `scripts/provider-conformance/java`, and cleans up. It sends
no request until `plan` validates the corpus and a conservative cost bound against the
caps, and each run needs `--confirm-live`:

```bash
python3 scripts/provider-conformance/campaign.py plan --provider gcs --markdown
python3 scripts/provider-conformance/campaign.py run --provider gcs --profile full \
    --campaign-dir PRIVATE_DIR --confirm-live
python3 scripts/provider-conformance/campaign.py sanitize --provider gcs \
    --campaign-dir PRIVATE_DIR --run RUN --out docs/replay-provider-evidence/gcs
```

The runner enforces its own hard caps per provider campaign: at most 6,000 objects,
16 MiB of payload, 25,000 HTTP attempts with at least 7,500 kept for cleanup, four
concurrent requests, 30 seconds per request, 30 minutes of setup and probing and 15 of
cleanup. Every attempt, including credential refreshes, retries and failures, is
written to a fsynced campaign ledger before it is sent. A campaign-wide cap closes the
campaign; a per-request cap makes that run INCOMPLETE. The run prefix must list empty
before anything is created, creates never overwrite, and cleanup deletes only names it
lists inside that prefix (conditionally on the listed generation or ETag on GCS and
Azure), then verifies an empty namespace. `recover` repeats cleanup for an interrupted
run. Credentials and account identifiers are redacted before anything is written;
`sanitize` publishes evidence only after a scan of decoded bodies finds no identifier,
credential, email or ACL entity.

`promotion.py rows` regenerates the ledger rows and the table above from the case
matrices; `promotion.py promote` attaches captures and receipts, then runs the
fail-closed validator. `NativeEvidenceReplayTest` replays every committed capture against
replay in the normal build and fails if any outcome differs from its pinned expectation.
