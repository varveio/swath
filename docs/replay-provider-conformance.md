# Replay provider conformance matrix

This is the human-readable matrix for [issue #221](https://github.com/varveio/swath/issues/221)
within [epic #220](https://github.com/varveio/swath/issues/220). The
[machine-readable ledger](replay-provider-ledger.json) owns row IDs and evidence status;
the offline validator requires this matrix to match it.
`UNMEASURED` means no published native-provider capture backs the row. `OBSERVED`
requires a sanitized native capture and its SHA-256 checksum. `REPLAY_MATCH`
additionally requires an independent replay test against that capture. Documentation,
model reviews, local emulators, and agreement with a future swath fetcher do not advance
a row.

**Current release gate:** a bounded [live campaign](#live-campaign) captured every case
below against a dedicated GCS bucket and Azure container. A row is `OBSERVED` when its
sanitized capture is in the [evidence archive](#evidence-archive) and `REPLAY_MATCH` only
when replay, serving the same corpus and walked with its own tokens, matched that capture
and an archived receipt records the match. Every other observed row is a pinned replay
discrepancy, except 62 Azure rows that replay now matches. Those rows stay `OBSERVED` until
their receipts are archived; [Evidence archive](#evidence-archive) explains how the test
derives each expectation from the archive's `<provider>/replay-expectations.json`. The GCS
and Azure profiles stay provisional until the remaining rows match. S3 is out of scope for
this evidence: the S3 campaign stopped before it completed, so no S3 capture is published
and every S3 row stays `UNMEASURED`. Promotion requires a COMPLETE campaign receipt. Each row is one case: one request shape that
settles one question. Azure rows name their service versions, and each version is
evidenced separately.

| ID | Provider | Feature or probe | Profile promise pending evidence | Capture SHA-256 | Replay test | State |
| --- | --- | --- | --- | --- | --- | --- |
| gcs-01a | GCS JSON v1 | `startOffset` equal to an existing name is inclusive | Provisional bounded list | json-v1:d9a6aa7bc53104ce2ad6941906115309bf644d05f97c22f0a713e143ff8b5035 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01b | GCS JSON v1 | `endOffset` equal to an existing name is exclusive | Provisional bounded list | json-v1:f1e9f4678f6ba380d073afd1ca5df2b2d348c9b33ddce1108d580efe18ac38e1 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01c | GCS JSON v1 | Equal `startOffset` and `endOffset` on an existing name | Provisional bounded list | json-v1:4bd4bfb351f845f9793a459e66a38ba437b43f49e2aeaef80685af3925a229b2 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01d | GCS JSON v1 | `startOffset` in a gap between names | Provisional bounded list | json-v1:516bd1292005bc92410728dc6671a25525b8a2832ba7696d636a3587c908619e | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01e | GCS JSON v1 | `endOffset` in a gap between names | Provisional bounded list | json-v1:dc983e626aeae6e1ec58dda471006a42917c19054342f16cc0b9844203bff4f9 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01f | GCS JSON v1 | Inverted bounds with `startOffset` after `endOffset` | Provisional bounded list | json-v1:ccc8505841a7f74304758f6a653df488da50508538150fbc6dfb28ee8bf29e05 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01g | GCS JSON v1 | Both offsets without a prefix | Provisional bounded list | json-v1:b9f8ec26553afb53482db123d0b6fc787794b6f4d87207f9d505cbb09699d500 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01h | GCS JSON v1 | Offsets before and after every corpus name | Provisional bounded list | json-v1:72ca437d7448e991f76f3a73d44607a2869ae1b8ec46af504173f0ac25b4f38b | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01i | GCS JSON v1 | Empty `startOffset` and `endOffset` values | Provisional bounded list | json-v1:b9739b00affb63e63ad999bda5d5112ff6776498ea228505262d09b0949c898f | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01j | GCS JSON v1 | `startOffset` equal to the prefix | Provisional bounded list | json-v1:fcb64a4d429d025ceb57476b583cae8502c9fd543c30388accb3f702e4356d76 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-01k | GCS JSON v1 | `endOffset` equal to the prefix | Provisional bounded list | json-v1:7238b10c02e7ec45add0d055180619cb8907190400e9d23891edcebc819f8784 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-02a | GCS JSON v1 | Prefix with `startOffset` before it and `endOffset` inside it | Provisional bounded list | json-v1:d55690dea6a1361ec4440ec7083c5a91c8a3a1242b8b1a3e28f43bc64614dd68 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-02b | GCS JSON v1 | Prefix with `startOffset` inside it and `endOffset` after it | Provisional bounded list | json-v1:e81e556774e95961daf6a3c280467285be83b0a2d777f6f14d135b8de2e838ac | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-02c | GCS JSON v1 | Prefix with both offsets before it | Provisional bounded list | json-v1:2e283f8d29a764fe5edcb52bab5c1dc80ecf084999137a01077a521969356cee | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-02d | GCS JSON v1 | Prefix with both offsets after it | Provisional bounded list | json-v1:3d93a4b2b787f3a2c9f359fc2d6da3a896c3db8160f32dfd1eb8a4b7af647566 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-02e | GCS JSON v1 | Prefix that is a string prefix of longer names | Provisional bounded list | json-v1:fd52c7d1a3eb8b6bd946fffe576dacc42f4ba2124d0ffa920c74099d87619afb | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-02f | GCS JSON v1 | Prefix equal to a full name with `endOffset` equal to that name | Provisional bounded list | json-v1:76c97912d6d00ff95be2daf28b520536d8a1d96784f68951589f1469098a2b34 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03a | GCS JSON v1 | Delimiter `/` baseline over objects, subtrees, a folder marker and an empty segment | Provisional; fast path gated | json-v1:850050950dd5efb26bf03257a784b1574138be77ae9965d00020c7825266aa30 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03b | GCS JSON v1 | Delimiter with `startOffset` equal to a subtree prefix | Provisional; fast path gated | json-v1:6954246c5924b426987e8ab52afc4c2cfd6890c506c774ff5e8bfcc4e0990d7b | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03c | GCS JSON v1 | Delimiter with `startOffset` inside a subtree with remaining descendants | Provisional; fast path gated | json-v1:af49a57722f8fbedefe981ae6c32c7d3fee32ae5754e118bbd9ed7ef0b00065f | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03d | GCS JSON v1 | Delimiter with `startOffset` inside a subtree after its last descendant | Provisional; fast path gated | json-v1:37bfbbbbf2b27fa5e23fbee3bdd2d50b7165bff82b5f30ccb50fca80945d76a2 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03e | GCS JSON v1 | Delimiter with `startOffset` after a subtree, equal to an object | Provisional; fast path gated | json-v1:8ede4e32d5615d8425d37d62dac5c16bd4c8436e707f5264fb4467358f0b05a1 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03f | GCS JSON v1 | Delimiter with `endOffset` inside a subtree | Provisional; fast path gated | json-v1:3e39e4d7ea32e7bd65b64df636156b80ecaa79bf9a185b83a6efc70987aee90a | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03g | GCS JSON v1 | Delimiter with `endOffset` equal to a subtree prefix | Provisional; fast path gated | json-v1:90780d4845d93077fdd6ad24b38366e342ff308ed5775b1b196ff70cf75e3660 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03h | GCS JSON v1 | Delimiter with `endOffset` just after a folder marker | Provisional; fast path gated | json-v1:e537312ec3aaec6e78d58666556f50cf72f84ffd49bc944141df15aeb56a4d2a | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03i | GCS JSON v1 | Delimiter `/` with a prefix ending in the delimiter | Provisional; fast path gated | json-v1:d0463290e0237bcf28ad6cc4fc0c034cb3997a3f7d3692c1f54bd2c240e3872b | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03j | GCS JSON v1 | Delimiter `/` with a prefix that stops before the delimiter | Provisional; fast path gated | json-v1:481168aaad505c7adae3c87ebd1584153c6b697c1fd97a00f3ea00d948ca93fa | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03k | GCS JSON v1 | Pipe delimiter that never occurs in names | Provisional; fast path gated | json-v1:466458692584e7a6941010a36c5acdfa2059c20bc269ff3e6053e97f2fdc32dd | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03l | GCS JSON v1 | Two-character delimiter `/1` | Provisional; fast path gated | json-v1:2ae2112e6711b27d9ab3eb0151dc31248b85089250372301a7acf8be16f2d225 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03m | GCS JSON v1 | Two-character delimiter `-g` | Provisional; fast path gated | json-v1:7c86197b2e8876c1d6536b4609d5e347e8002046967e9e9fab653d372f5f4f03 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03n | GCS JSON v1 | Delimiter with both offsets cutting two subtrees | Provisional; fast path gated | json-v1:a81d4c77b9001caeedf82febd2c362078cb992ac705ad4fc872ff2d00622ea2e | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03o | GCS JSON v1 | Empty `delimiter` value | Provisional; fast path gated | json-v1:0b561b7ecc5b077400fdd01f300c28138179f71d93b19a4dc41057d9162e0d4b | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-03p | GCS JSON v1 | `includeTrailingDelimiter=true` with delimiter `/` | Provisional; fast path gated | json-v1:fa7d5904645c4944bad6a0bd511433490d406afbf8bef92448e7c3380b44e8a8 | — | OBSERVED |
| gcs-03q | GCS JSON v1 | Delimiter without a prefix from a mid-corpus `startOffset` | Provisional; fast path gated | json-v1:ef2c3bb6242010072a3e75a8013b6ad0fddc2c8183de85bfbb3aae04e741d336 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-04a | GCS JSON v1 | BMP, surrogate-neighbor and supplementary names in list order | Provisional unsigned UTF-8 order | json-v1:bf524208961eb41ed022cc67d11ff950edff11a931f9461ffbb0e16c412f88a2 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-04b | GCS JSON v1 | `startOffset` at U+E000 | Provisional unsigned UTF-8 order | json-v1:8c87dcf9cf235ff212a3336336eda806af34af20d703df7d021e9da35bf6a84b | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-04c | GCS JSON v1 | `endOffset` at U+10000 | Provisional unsigned UTF-8 order | json-v1:dc134fd6731df591f3417e3d427bcfa6afded5158e62b0298db9817e9edaad21 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-04d | GCS JSON v1 | Prefix in NFC form with an NFD sibling | Provisional unsigned UTF-8 order | json-v1:2242c42bcd1a73f3344e59a023b0ceeabd86219e05e48cf9c9e74d77a3b9b426 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-04e | GCS JSON v1 | Prefix differing only in case | Provisional unsigned UTF-8 order | json-v1:33c441151f11492743f01e109d5d04a38edcf53e31bdb74484a69878459ee9c4 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-04f | GCS JSON v1 | Supplementary-character `startOffset` percent-encoded in lowercase hex | Provisional unsigned UTF-8 order | json-v1:4de0b311ea1634a1ffe144a4fecc3886819699d40e87bdbf111b32b33567aa32 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05a | GCS JSON v1 | Raw `+` in a `prefix` value | Strict native parser; duplicate policy gated | json-v1:3ead1bdaf82796f1c553a05fc69f72682bfde2c73c048f4601bf18ed9069cd86 | — | OBSERVED |
| gcs-05b | GCS JSON v1 | `%2B` in a `prefix` value | Strict native parser; duplicate policy gated | json-v1:a614a08c00a9eec51fed053cd90eea55c85ec99dda0cdd166263683dc7e295dd | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05c | GCS JSON v1 | `%20` in a `prefix` value | Strict native parser; duplicate policy gated | json-v1:a5632f7a08f6c3ee9a671ea37810c1cec10d65ee50db33dc16adb24e79317318 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05d | GCS JSON v1 | `%25` before `2B` | Strict native parser; duplicate policy gated | json-v1:51527430d27b5c9e7a165ba871aa738cc764879763ef60971a3de2c9fef663a4 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05e | GCS JSON v1 | Malformed percent escape `%ZZ` beside a literal `a%ZZ` name | Strict native parser; duplicate policy gated | json-v1:c18f01c1e4d4ef6e9f31041707408eb2eaa8fe0da6921c7eb867b1712eee569e | — | OBSERVED |
| gcs-05f | GCS JSON v1 | Trailing lone `%` | Strict native parser; duplicate policy gated | json-v1:b633b21f591930702eb22235b87c2ba5ff35809a2da0c6d0eec5ee51b9806066 | — | OBSERVED |
| gcs-05g | GCS JSON v1 | Escape decoding to invalid UTF-8 beside a U+FFFD name | Strict native parser; duplicate policy gated | json-v1:52d3dd5350e28f246defbf9ed96c2c5b5eb553bfc834ab8fb71ee25bfa31939b | — | OBSERVED |
| gcs-05h | GCS JSON v1 | Duplicate `prefix` with identical values | Strict native parser; duplicate policy gated | json-v1:275f9906d291c5703bc2c65d3d2076677c60fe994b35432ec9e12594013d43d1 | — | OBSERVED |
| gcs-05i | GCS JSON v1 | Duplicate `prefix` with different values | Strict native parser; duplicate policy gated | json-v1:86e58416af708ad1381cc0a3bded80832a00ed14498252ce289992e6d028a94a | — | OBSERVED |
| gcs-05j | GCS JSON v1 | Percent-encoded `&`, `=` and `#` | Strict native parser; duplicate policy gated | json-v1:94cb1faaba0611a021c17d7378d8dbaad49a7f8d4d4f127dfa8374741304cc76 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05k | GCS JSON v1 | Raw `?` inside a value | Strict native parser; duplicate policy gated | json-v1:2e65c6747043cb0b9199185f2da50d1f662bbebb322e554536df343ac34f01ac | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05l | GCS JSON v1 | Fully encoded slashes `%2F` | Strict native parser; duplicate policy gated | json-v1:2630126e66b0f119536d6613fd53a5f37d2ca2e32b5f45cc48234ffc26c29b78 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05m | GCS JSON v1 | Lowercase hex escape `%2b` | Strict native parser; duplicate policy gated | json-v1:162647bb2d1f0b0e83da49d07d76026391104a4d7b94c0d300f4def17098dbf7 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05n | GCS JSON v1 | Parameter name without `=` | Strict native parser; duplicate policy gated | json-v1:5e0b20bb57e6c256a0702b49560190014c001fc385d307d18bb8e3cae9e4534f | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05o | GCS JSON v1 | Every reserved ASCII character in names under one prefix | Strict native parser; duplicate policy gated | json-v1:fe56318b0bf1c3afd5ce11711085e09a65098076af33dd3fe4b004edb82ac95e | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05p | GCS JSON v1 | Trailing space in a prefix | Strict native parser; duplicate policy gated | json-v1:26b8fbe2923e35f536ea6f3603a07d72f8a669f3e6a76572ac7209a63bd39207 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05q | GCS JSON v1 | Raw `=` inside a value | Strict native parser; duplicate policy gated | json-v1:9bca31a9a3084c92b62a03cc8d5f50634ad798cf34f625a84e5b97b5f0d208d5 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-05r | GCS JSON v1 | Raw `=` inside a `startOffset` value | Strict native parser; duplicate policy gated | json-v1:36b1c808cebc12067af4f3fb9f5f0339a1d9b76da33b4249be184e2ef63cba42 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-06a | GCS JSON v1 | `maxResults=0` on a three-object scope | Zero refused pending evidence | json-v1:00aed29120ac8087f79d8b2666b8600de144b03e9581830629b40eaec0478fef | — | OBSERVED |
| gcs-06b | GCS JSON v1 | `maxResults=0` on a 1,001-object scope | Zero refused pending evidence | json-v1:45273937110c0d01a3a158f19579aa7d18c15846540b53b2d5f01e6b4c412e69 | — | OBSERVED |
| gcs-06c | GCS JSON v1 | `maxResults=1` walk | Zero refused pending evidence | json-v1:c313a73553523af112b336326baee71d9f0c0bf94137d4fa4281b2bf2180deac | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-06d | GCS JSON v1 | `maxResults=1000` walk over exactly 1,000 objects | Zero refused pending evidence | json-v1:930425ada8a817df08183e54cc0d8a811c21be631e30fbd190e95f4edf70ce0f | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-06e | GCS JSON v1 | `maxResults=1001` over 1,001 objects | Zero refused pending evidence | json-v1:f153441c92a60a71960b053c9d6e6106845a25c623b7b8de0bd382b43e464602 | — | OBSERVED |
| gcs-06f | GCS JSON v1 | `maxResults=-1` | Zero refused pending evidence | json-v1:bf38d5fdb00d3bb95dd48ef6fe80ac96200c4058d0722c4f8190ef1bb5a61067 | — | OBSERVED |
| gcs-06g | GCS JSON v1 | `maxResults=abc` | Zero refused pending evidence | json-v1:9119ff45d4f0c950e2ad0bcfc57c7a65e8fc8f47b3a3aa9e093841b3e392cc9f | — | OBSERVED |
| gcs-06h | GCS JSON v1 | `maxResults=2147483648` | Zero refused pending evidence | json-v1:f36263d682555ad13490a8bf2988783f8f3692c314be1239aee6526711a29f13 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-06i | GCS JSON v1 | `maxResults=9223372036854775808` | Zero refused pending evidence | json-v1:81580b3363224cbef192cd162549e824d5dc2110aaae0d41f3fd30f6a9963619 | — | OBSERVED |
| gcs-06j | GCS JSON v1 | `maxResults=1.5` | Zero refused pending evidence | json-v1:4b32148f37f79f4e786c96cf9d13803b417bd5c0de7570b3aebcaad6595b3c5d | — | OBSERVED |
| gcs-06k | GCS JSON v1 | `maxResults=+2` with a raw plus | Zero refused pending evidence | json-v1:02942fb8c5a57679970ec8765f4049d60c053f9160b50d4fc96f6cf9289223e0 | — | OBSERVED |
| gcs-06l | GCS JSON v1 | `maxResults=002` | Zero refused pending evidence | json-v1:76b42a2372fb4a140ffa18e3a19e521b27c45dc924259237be6de126bda41a01 | — | OBSERVED |
| gcs-06m | GCS JSON v1 | Duplicate `maxResults` values | Zero refused pending evidence | json-v1:346675e4d5bb108f6cf626c5b9d2f500b09c06fcac34e4057049ddb04aac8ce2 | — | OBSERVED |
| gcs-06n | GCS JSON v1 | Empty `maxResults` value | Zero refused pending evidence | json-v1:e8e6604d31acb1566bbfcd35369eac62911faa6db009443bc10c5d4e9a0114d3 | — | OBSERVED |
| gcs-06o | GCS JSON v1 | `maxResults=2147483647` | Zero refused pending evidence | json-v1:f68228b9b9bb42f13b32cdeba09f052b884f67b4ca7fb29757147eb445522d5f | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07a | GCS JSON v1 | Delimiter walk with one entry per page over top-level subtrees | Complete native-token walk required | json-v1:e5683309cf2bd12c56568d846a00000f2d18e7c852dbe2b2237bd2d098e546ab | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07b | GCS JSON v1 | Default page size walk over 1,001 objects | Complete native-token walk required | json-v1:a0e44c509747305405fab39457db52f9cae24f14880f6a9c366235505cda4ab6 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07c | GCS JSON v1 | Delimiter walk with two entries per page | Complete native-token walk required | json-v1:8f61d4051bc5486bf967d773e596608522f8a60a90d7617182308d2232e3f3a6 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07d | GCS JSON v1 | Delimiter walk with offsets and one entry per page | Complete native-token walk required | json-v1:845e01e42700cc04a6fc500a0ace6daa030f9871b1de96c97f76ac7df7e45a55 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07e | GCS JSON v1 | Flat walk with four entries per page over six names | Complete native-token walk required | json-v1:52e976b9b21738b2488452dd296359a337e697db7385790ee9363e13a1b7f541 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07f | GCS JSON v1 | Prefix with no matching names | Complete native-token walk required | json-v1:dd79ebc9fdd26bdb0366394775e91ed2493ce1e1c74b5e2cbea047dd5bf61f20 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07g | GCS JSON v1 | Malformed constant `pageToken` | Complete native-token walk required | json-v1:d8feac55e037e320852b6b50a5355090422e3450b3e4a3e9c3c4b6aab9f9b207 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07h | GCS JSON v1 | Reused `pageToken` for the same query | Complete native-token walk required | json-v1:c1d1a42c5e655a26d931149d0398349ce2acb58d70bcdc7f120fac8fed5a7474 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07i | GCS JSON v1 | `pageToken` from a different prefix | Complete native-token walk required | json-v1:8f9e43e1a5c21f045f25863b6d1877cd2d2f2c69fc8e1357237895359205892f | — | OBSERVED |
| gcs-07j | GCS JSON v1 | `pageToken` with a changed page size | Complete native-token walk required | json-v1:a56f16c7e7e5bb73051c31191f43fd63a595d31a85c1299bdbad96480639d6e7 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07k | GCS JSON v1 | `pageToken` from a flat walk with an added delimiter | Complete native-token walk required | json-v1:80affa3cb347dcc8226c303e81f471f93eb576f10af7a891cc93ca28cc770cdd | — | OBSERVED |
| gcs-07l | GCS JSON v1 | `pageToken` with an added `startOffset` past the token position | Complete native-token walk required | json-v1:3b30fb028ab17f553203807b5728907dddf0a03aba5138e2acc04b5134f1862f | — | OBSERVED |
| gcs-07m | GCS JSON v1 | Empty `pageToken` value | Complete native-token walk required | json-v1:d4e88d5b60b8b515e010887903dccdb2ebd07872c35bce30fea84e7200ca24ab | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07n | GCS JSON v1 | Flat walk with two entries per page over delimited names | Complete native-token walk required | json-v1:0e574481b555a979b88d8f88655ef05ebfe46974233f523bb1805b256fd2249b | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07o | GCS JSON v1 | `pageToken` from a delimiter walk with the delimiter removed | Complete native-token walk required | json-v1:dda4f39057c3269f47e99e01bce88826ef980b403958e187ffdc432726cc80ed | — | OBSERVED |
| gcs-07q | GCS JSON v1 | Flat walk with two entries per page over six names | Complete native-token walk required | json-v1:d31188a3000367bffb7393e3c99857c433c3d5eab6e50a27fd645391a916bee9 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-07r | GCS JSON v1 | Issued `pageToken` with its last character changed | Complete native-token walk required | json-v1:3ee4aa66617f5b60198856ee6caaf8f0b52e446e56107218bf22062f9525265a | — | OBSERVED |
| gcs-07s | GCS JSON v1 | Issued `pageToken` truncated by four characters | Complete native-token walk required | json-v1:d22f1dedb3eaa44daa738eb52b8f1ff980102cdf8b197453901717acea2808c7 | — | OBSERVED |
| gcs-07t | GCS JSON v1 | `pageToken` with an `endOffset` excluding every remaining name | Complete native-token walk required | json-v1:69271f9cc3e66112bb2da2329dcfd276d49dbb3f7d2004e2fa6a486a044be7bf | — | OBSERVED |
| gcs-07u | GCS JSON v1 | `pageToken` with a prefix matching only names before it | Complete native-token walk required | json-v1:3a58044eeed085f34f8a86533442c33ba39fb439ad88660b1b5367641f4d2b9d | — | OBSERVED |
| gcs-07v | GCS JSON v1 | Issued `pageToken` with one decoded byte changed and re-encoded | Complete native-token walk required | json-v1:aeb727a2a78278a02932dcfb6c1285dd4e6452cb36f34edeeba17a524c189af4 | — | OBSERVED |
| gcs-08a | GCS JSON v1 | Names with control, whitespace, dot, markup and 1,024-byte length | Strict UTF-8 subset | json-v1:c9cc321f55547f668ea09b9192729c57ea1dada97f6d18d7db4fd410e80738cd | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-08b | GCS JSON v1 | Delimiter listing over dot segments and trailing-slash names | Strict UTF-8 subset | json-v1:011a64c59cbcb8527f7edfe1c262238e3e4b4edcf84063faecfa1aab6c2b1a27 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-08c | GCS JSON v1 | Listing of exploratory creation-probe names | Strict UTF-8 subset | json-v1:9a7bdd53e1c110f2c261b5337008684dc18a2a6cb1381c9022385076ba2934e9 | — | OBSERVED |
| gcs-09a | GCS JSON v1 | HTTP Java SDK flat auto-pagination over Unicode names | Must run unmodified with endpoint override | json-v1:ad2c05952a4affae4fe5484a98ba6ef70f81dc1dc67e4e1add4f4ac6a494c5f1 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-09b | GCS JSON v1 | HTTP Java SDK hierarchy auto-pagination | Must run unmodified with endpoint override | json-v1:aa786755d0dcb4a6d6a7474282bdc65ce738ddf6fffaf62038029cabb9fc3cbb | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-09c | GCS JSON v1 | HTTP Java SDK offsets with auto-pagination | Must run unmodified with endpoint override | json-v1:914452dd91690229f5f121d410d1b9e6e5888c1d38abbf922e590d61eb8a61db | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-10 | GCS JSON v1 | Retry-disabled one-attempt errors; normal SDK retries | Separate request counts | — | — | UNMEASURED |
| gcs-11a | GCS JSON v1 | Explicit `projection=full` under uniform bucket-level access | Named defaults and transport forms only; other options refused | json-v1:9a90c3792fb424aba9d6f680baa37a67012eeb9ff3483908a254b03e655b22f7 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-11b | GCS JSON v1 | Explicit `projection=noAcl` | Named defaults and transport forms only; other options refused | json-v1:2d628ccfabb348bc33e2ec8e8e721fd9f4ce90e755aa62d440ec11904f69e1c6 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-11c | GCS JSON v1 | Unknown projection value | Named defaults and transport forms only; other options refused | json-v1:f30a18dfa4681f67a290b3be2f9000786fbee6bfcd4de824c0f3d900f74ed476 | — | OBSERVED |
| gcs-11d | GCS JSON v1 | `prettyPrint=false` and `alt=json` | Named defaults and transport forms only; other options refused | json-v1:4b554c0fdf7b3b52ea93b318258a3f193133349dac2b86790d447d1668882860 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-11e | GCS JSON v1 | Nonempty `fields` partial response | Named defaults and transport forms only; other options refused | json-v1:acbecee44facbd79cbc20274545fd6ce5c3ae60bd99255ee6bfd3f87e636a029 | — | OBSERVED |
| gcs-11f | GCS JSON v1 | `versions=true` | Named defaults and transport forms only; other options refused | json-v1:bed97a950d1f3e9693ccc5bfb486f5b333a6a97ede7e4e41f475ebb4127ec83b | — | OBSERVED |
| gcs-11g | GCS JSON v1 | `matchGlob` | Named defaults and transport forms only; other options refused | json-v1:f90494450cbb372afa7c2f3ec517cd156e62e0b67aa55b92f389a5b3dcfb1892 | — | OBSERVED |
| gcs-11h | GCS JSON v1 | `softDeleted=true` with soft delete disabled | Named defaults and transport forms only; other options refused | json-v1:415e98e73f8a380ed7833204ca24e5ea53ef03c0161e42dcd8a2892c5fdf704b | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| gcs-11i | GCS JSON v1 | `includeFoldersAsPrefixes=true` on a flat bucket | Named defaults and transport forms only; other options refused | json-v1:1337d2d3b41450fa0093837e796f6345bc685542a2a6f92d28ffb53e518b2749 | — | OBSERVED |
| gcs-11j | GCS JSON v1 | Named false forms of boolean options | Named defaults and transport forms only; other options refused | json-v1:243de8cfd1b61bed5b4c314a61822364c7b2beb948700c1b2c5d4791131a7e08 | `json-v1:gcsNativeCapturesMatchReplay` | REPLAY_MATCH |
| azure-01 | Azure Blob XML `2026-06-06` | Account accepts service version and echoes it | Explicit version only | 2026-06-06:46e7d0c7acb2a1abe5c6b5f81834a78d2613e0719e1ec0901683605dd9ab701d | — | OBSERVED |
| azure-02 | Azure Blob XML `2026-10-06` | Account rollout and version echo | Version individually gated | 2026-10-06:1367fce98f1fdaa8c86e015d4e631923f8f5a42ac3addd3d7a3e23d6ad832c0c | — | OBSERVED |
| azure-03a | Azure Blob XML | `startFrom` equal to an existing name is inclusive | Provisional flat list | 2026-06-06:d0fb8d97bbb83c94077de3b161478a3c98272a69242a292fc29d220afe11a99a; 2026-10-06:d0fb8d97bbb83c94077de3b161478a3c98272a69242a292fc29d220afe11a99a | — | OBSERVED |
| azure-03b | Azure Blob XML | `startFrom` in a gap between names | Provisional flat list | 2026-06-06:9128e1ec99de085e2871e0cb2f4f106370316c539d9930b461034f0ed6b188ea; 2026-10-06:9128e1ec99de085e2871e0cb2f4f106370316c539d9930b461034f0ed6b188ea | — | OBSERVED |
| azure-03c | Azure Blob XML | `startFrom` before the prefix | Provisional flat list | 2026-06-06:bf7205bf68765aa8162a96546645aa8b211dd7fc28162a42e50dcd17d0696219; 2026-10-06:bf7205bf68765aa8162a96546645aa8b211dd7fc28162a42e50dcd17d0696219 | — | OBSERVED |
| azure-03d | Azure Blob XML | `startFrom` after the prefix | Provisional flat list | 2026-06-06:197ded61ee74eccc75aca8e74bea283af2f815fe4deeac3ee4adf4091af29486; 2026-10-06:197ded61ee74eccc75aca8e74bea283af2f815fe4deeac3ee4adf4091af29486 | — | OBSERVED |
| azure-03e | Azure Blob XML | `startFrom` equal to the prefix | Provisional flat list | 2026-06-06:1e7ad5561fd6c5330c139f69c23349e1051c60d840a978cd1550f43fda884c2a; 2026-10-06:1e7ad5561fd6c5330c139f69c23349e1051c60d840a978cd1550f43fda884c2a | — | OBSERVED |
| azure-03f | Azure Blob XML | `startFrom` without a prefix | Provisional flat list | 2026-06-06:5be875b6dc4f5aaee6cbf1287ad3a1c96e8eddd10bb99b193cc928541f2046d5; 2026-10-06:5be875b6dc4f5aaee6cbf1287ad3a1c96e8eddd10bb99b193cc928541f2046d5 | — | OBSERVED |
| azure-03g | Azure Blob XML | Empty `startFrom` value | Provisional flat list | 2026-06-06:047972333bdff2b2cc4f60dea1ada13d03546133758f332f23558ce61db9a1b0; 2026-10-06:047972333bdff2b2cc4f60dea1ada13d03546133758f332f23558ce61db9a1b0 | — | OBSERVED |
| azure-03h | Azure Blob XML | `endBefore` | Provisional flat list | 2026-06-06:72c2615eb83500e319536a1be7addb882e634e9f4151db04827ea59e10ef7eda; 2026-10-06:72c2615eb83500e319536a1be7addb882e634e9f4151db04827ea59e10ef7eda | — | OBSERVED |
| azure-03i | Azure Blob XML | `startFrom` with `endBefore` | Provisional flat list | 2026-06-06:1e479ee78b176fa57ac0c1310a5d187ef89ade297309560aeadfffc66f6b30d7; 2026-10-06:1e479ee78b176fa57ac0c1310a5d187ef89ade297309560aeadfffc66f6b30d7 | — | OBSERVED |
| azure-03j | Azure Blob XML | Prefix that is a string prefix of longer names | Provisional flat list | 2026-06-06:eb2fd8d04111492d64077c8e93e9c85dbf645d8634b757fe8b585a24edcd8cd1; 2026-10-06:eb2fd8d04111492d64077c8e93e9c85dbf645d8634b757fe8b585a24edcd8cd1 | — | OBSERVED |
| azure-03k | Azure Blob XML | `startFrom` inside a prefix that is a string prefix | Provisional flat list | 2026-06-06:f6085760d852d7484a460a009683f24bbe379ce31b6c667ccc05fbf192101ded; 2026-10-06:f6085760d852d7484a460a009683f24bbe379ce31b6c667ccc05fbf192101ded | — | OBSERVED |
| azure-04a | Azure Blob XML | Delimiter `/` baseline over objects, subtrees, a folder marker and an empty segment | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:76698d2d7e1a542fcf74a2024ab69ab075a2123b5130c436ff2fc840d8dacb13; 2026-10-06:76698d2d7e1a542fcf74a2024ab69ab075a2123b5130c436ff2fc840d8dacb13 | — | OBSERVED |
| azure-04b | Azure Blob XML | Delimiter with `startFrom` equal to a subtree prefix | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:4b9d417f5a6c961b7e66a4331899dc940852d46778c25d528be5934bf286d06b; 2026-10-06:4b9d417f5a6c961b7e66a4331899dc940852d46778c25d528be5934bf286d06b | — | OBSERVED |
| azure-04c | Azure Blob XML | Delimiter with `startFrom` inside a subtree with remaining descendants | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:cbb25c03e24d970ed73ca435b423167332163817ccf7fa732b8fef4e56586105; 2026-10-06:cbb25c03e24d970ed73ca435b423167332163817ccf7fa732b8fef4e56586105 | — | OBSERVED |
| azure-04d | Azure Blob XML | Delimiter with `startFrom` inside a subtree after its last descendant | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:f7027eb607dd6f2186012dd982e9a67cdf7e347a8164761b5de16e598a0cb41b; 2026-10-06:f7027eb607dd6f2186012dd982e9a67cdf7e347a8164761b5de16e598a0cb41b | — | OBSERVED |
| azure-04e | Azure Blob XML | Delimiter with `startFrom` after a subtree, equal to an object | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:cc5c54628fe7315a1ec5d845b658a791a392b60819ec8ac1cb69b5090d7d7c55; 2026-10-06:cc5c54628fe7315a1ec5d845b658a791a392b60819ec8ac1cb69b5090d7d7c55 | — | OBSERVED |
| azure-04f | Azure Blob XML | Delimiter `/` with a prefix ending in the delimiter | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:7c4333fbaac5384dcb6aa0a8ada842b62f2bda52ae35fd4696e87a7c6650a757; 2026-10-06:7c4333fbaac5384dcb6aa0a8ada842b62f2bda52ae35fd4696e87a7c6650a757 | — | OBSERVED |
| azure-04g | Azure Blob XML | Delimiter `/` with a prefix that stops before the delimiter | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:e52a7fda6725b45d0870af84829d3923b8e9fb0617e26b38ef50032627776ede; 2026-10-06:e52a7fda6725b45d0870af84829d3923b8e9fb0617e26b38ef50032627776ede | — | OBSERVED |
| azure-04h | Azure Blob XML | Pipe delimiter that never occurs in names | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:a38da1ca05b1b6550e36e81aea9a811a3baf4b00a1a6b7aa03578c07e70a1a19; 2026-10-06:a38da1ca05b1b6550e36e81aea9a811a3baf4b00a1a6b7aa03578c07e70a1a19 | — | OBSERVED |
| azure-04i | Azure Blob XML | Two-character delimiter `/1` | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:9d383ec7959bbfcfecfff9c06dd1d972f1d57203f1d89640cb37c46613dc0707; 2026-10-06:9d383ec7959bbfcfecfff9c06dd1d972f1d57203f1d89640cb37c46613dc0707 | — | OBSERVED |
| azure-04j | Azure Blob XML | Two-character delimiter `-g` | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:706e7e89698ec11ad948b8c0b58fc3e0231fd05712853ff7d703e5cd1d00bb29; 2026-10-06:706e7e89698ec11ad948b8c0b58fc3e0231fd05712853ff7d703e5cd1d00bb29 | — | OBSERVED |
| azure-04k | Azure Blob XML | Empty `delimiter` value | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:b8bcfd82fba22b74b641f3aebe38f1909e0499ad38f1ac517076ef9e935d4b67; 2026-10-06:b8bcfd82fba22b74b641f3aebe38f1909e0499ad38f1ac517076ef9e935d4b67 | — | OBSERVED |
| azure-04l | Azure Blob XML | Delimiter without a prefix from a mid-corpus `startFrom` | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:54456f9e3b00b063a0e669e3b3c75df69cc587b1a4a38ecf1b70a32a319bd31d; 2026-10-06:54456f9e3b00b063a0e669e3b3c75df69cc587b1a4a38ecf1b70a32a319bd31d | — | OBSERVED |
| azure-04m | Azure Blob XML | Delimiter walk with `startFrom` inside a subtree | Interleaved Blob and BlobPrefix; `startFrom` keeps a prefix holding a later name | 2026-06-06:435f785bcda78e01dea9c000fe407a682da5dfde7222014308889e6a7bd8a4d1; 2026-10-06:435f785bcda78e01dea9c000fe407a682da5dfde7222014308889e6a7bd8a4d1 | — | OBSERVED |
| azure-05a | Azure Blob XML | Walk with `startFrom` and one entry per page | Matching `startFrom` retained on continuation; other combinations refused pending evidence | 2026-06-06:ebe5fcb2e9fa4428a1892ddef89e7c07d88d83434c53a0e585393140d65ee7ee; 2026-10-06:ebe5fcb2e9fa4428a1892ddef89e7c07d88d83434c53a0e585393140d65ee7ee | — | OBSERVED |
| azure-05b | Azure Blob XML | `marker` with a later `startFrom` | Matching `startFrom` retained on continuation; other combinations refused pending evidence | 2026-06-06:73adbde4d2a4bf4fe5236678d3671b55ca782827d48d11aca156b810c6f35775; 2026-10-06:73adbde4d2a4bf4fe5236678d3671b55ca782827d48d11aca156b810c6f35775 | — | OBSERVED |
| azure-05c | Azure Blob XML | `marker` with an earlier `startFrom` | Matching `startFrom` retained on continuation; other combinations refused pending evidence | 2026-06-06:8b6043e09bb55ed5140fa4bd7727e8b9f2a22c71b6b8b7af960164bf465d7b4f; 2026-10-06:8b6043e09bb55ed5140fa4bd7727e8b9f2a22c71b6b8b7af960164bf465d7b4f | — | OBSERVED |
| azure-05d | Azure Blob XML | `marker` issued under `startFrom` used without it | Matching `startFrom` retained on continuation; other combinations refused pending evidence | 2026-06-06:bef99edd63e11fa304cd22a3fd3e61029083c8d0a58b0de78df5d8d2bedf89f6; 2026-10-06:bef99edd63e11fa304cd22a3fd3e61029083c8d0a58b0de78df5d8d2bedf89f6 | — | OBSERVED |
| azure-06a | Azure Blob XML | BMP, surrogate-neighbor and supplementary names in list order | UTF-16 code-unit order over the byte-sorted fixture | 2026-06-06:e8925d287db3728e4dca798a134b238895052af8c6717e55fe3403f8e4f1d80a; 2026-10-06:e8925d287db3728e4dca798a134b238895052af8c6717e55fe3403f8e4f1d80a | — | OBSERVED |
| azure-06b | Azure Blob XML | `startFrom` at U+E000 | UTF-16 code-unit order over the byte-sorted fixture | 2026-06-06:58c222baea1d6a0fd05b119ea4387dfa50943913dddaea2f46b1e8bba752c8a8; 2026-10-06:58c222baea1d6a0fd05b119ea4387dfa50943913dddaea2f46b1e8bba752c8a8 | — | OBSERVED |
| azure-06c | Azure Blob XML | Prefix in NFC form with an NFD sibling | UTF-16 code-unit order over the byte-sorted fixture | 2026-06-06:8f51c266b4bb4ad9bf140ff59261e51249ea4713cbe91aadc15eb03b73f401a0; 2026-10-06:8f51c266b4bb4ad9bf140ff59261e51249ea4713cbe91aadc15eb03b73f401a0 | — | OBSERVED |
| azure-06d | Azure Blob XML | Prefix differing only in case | UTF-16 code-unit order over the byte-sorted fixture | 2026-06-06:10934eaff3a208c194f781206d2439ed4f2a7c2be20812bc6ec9b4f92b602992; 2026-10-06:10934eaff3a208c194f781206d2439ed4f2a7c2be20812bc6ec9b4f92b602992 | — | OBSERVED |
| azure-06e | Azure Blob XML | Supplementary-character `startFrom` in lowercase hex escapes | UTF-16 code-unit order over the byte-sorted fixture | 2026-06-06:19fef0b90429161352a956eee0951680ee525185bf6d6a087a8761f07e369b02; 2026-10-06:19fef0b90429161352a956eee0951680ee525185bf6d6a087a8761f07e369b02 | — | OBSERVED |
| azure-06f | Azure Blob XML | Delimiter `é` over Unicode names | UTF-16 code-unit order over the byte-sorted fixture | 2026-06-06:ff6ca80f46fa26247e0d37e4f2b9eb7106a0c5d488630b9a25272fc44e04e195; 2026-10-06:ff6ca80f46fa26247e0d37e4f2b9eb7106a0c5d488630b9a25272fc44e04e195 | — | OBSERVED |
| azure-07a | Azure Blob XML | Raw `+` in a `prefix` value | Explicit encoding; raw-wire assertion required | 2026-06-06:0fdcf81044424fa01c4b1f57461a151b992b13f673534c85ddc378c926eb009f; 2026-10-06:0fdcf81044424fa01c4b1f57461a151b992b13f673534c85ddc378c926eb009f | — | OBSERVED |
| azure-07b | Azure Blob XML | `%2B` in a `prefix` value | Explicit encoding; raw-wire assertion required | 2026-06-06:e631e7612971e1c1db8adc00dfb89ae06aa7d95917ffa900b2a20bc29ad44254; 2026-10-06:e631e7612971e1c1db8adc00dfb89ae06aa7d95917ffa900b2a20bc29ad44254 | — | OBSERVED |
| azure-07c | Azure Blob XML | `%20` in a `prefix` value | Explicit encoding; raw-wire assertion required | 2026-06-06:0cff59d8a2ae8c80a74f85cd1274026203175b1d07321155594c4d2af518fe75; 2026-10-06:0cff59d8a2ae8c80a74f85cd1274026203175b1d07321155594c4d2af518fe75 | — | OBSERVED |
| azure-07d | Azure Blob XML | `%25` before `2B` | Explicit encoding; raw-wire assertion required | 2026-06-06:0d4d7c87bfdd235b95ab711d2035344a942fb5477e7ac99044d61df4fd214b7d; 2026-10-06:0d4d7c87bfdd235b95ab711d2035344a942fb5477e7ac99044d61df4fd214b7d | — | OBSERVED |
| azure-07e | Azure Blob XML | Malformed percent escape `%ZZ` beside a literal `a%ZZ` name | Explicit encoding; raw-wire assertion required | 2026-06-06:119bc3a953ef2f2d6d3a82490aa0c2ccd8f2126895ad333788a4c1833b8a85cc; 2026-10-06:119bc3a953ef2f2d6d3a82490aa0c2ccd8f2126895ad333788a4c1833b8a85cc | — | OBSERVED |
| azure-07f | Azure Blob XML | Trailing lone `%` | Explicit encoding; raw-wire assertion required | 2026-06-06:ec6036268610e8e8822fd12ee3876ad4fc3997eb7390ef0212ca0b6078676d81; 2026-10-06:ec6036268610e8e8822fd12ee3876ad4fc3997eb7390ef0212ca0b6078676d81 | — | OBSERVED |
| azure-07g | Azure Blob XML | Escape decoding to invalid UTF-8 beside a U+FFFD name | Explicit encoding; raw-wire assertion required | 2026-06-06:c396fb082874a14276f40909c06499057bf1fb428bd0af0b1057996a4ba3adc8; 2026-10-06:c396fb082874a14276f40909c06499057bf1fb428bd0af0b1057996a4ba3adc8 | — | OBSERVED |
| azure-07h | Azure Blob XML | Duplicate `prefix` with identical values | Explicit encoding; raw-wire assertion required | 2026-06-06:c883bf388273c6c4ab15409d9031349da609489788d5e29863e9688369c27c5b; 2026-10-06:c883bf388273c6c4ab15409d9031349da609489788d5e29863e9688369c27c5b | — | OBSERVED |
| azure-07i | Azure Blob XML | Duplicate `prefix` with different values | Explicit encoding; raw-wire assertion required | 2026-06-06:fa8e876c5228cc3807189525cec129731f017c1516bddb98f8887a2707ea268d; 2026-10-06:fa8e876c5228cc3807189525cec129731f017c1516bddb98f8887a2707ea268d | — | OBSERVED |
| azure-07j | Azure Blob XML | Percent-encoded `&`, `=` and `#` | Explicit encoding; raw-wire assertion required | 2026-06-06:02aac01d5197fc075ccd68d198d4cdfead1ef7a969f4f77acae67f87fc87b138; 2026-10-06:02aac01d5197fc075ccd68d198d4cdfead1ef7a969f4f77acae67f87fc87b138 | — | OBSERVED |
| azure-07k | Azure Blob XML | Raw `?` inside a value | Explicit encoding; raw-wire assertion required | 2026-06-06:298f02a2ae509731f77b7d0dd78da21ad8d8d4f218dbc146a36edeab18aa80c9; 2026-10-06:298f02a2ae509731f77b7d0dd78da21ad8d8d4f218dbc146a36edeab18aa80c9 | — | OBSERVED |
| azure-07l | Azure Blob XML | Fully encoded slashes `%2F` | Explicit encoding; raw-wire assertion required | 2026-06-06:449f8175847e14d5cfad148a21a62351bd6c01baef1dd431ae9f40d4243adcb9; 2026-10-06:449f8175847e14d5cfad148a21a62351bd6c01baef1dd431ae9f40d4243adcb9 | — | OBSERVED |
| azure-07m | Azure Blob XML | Lowercase hex escape `%2b` | Explicit encoding; raw-wire assertion required | 2026-06-06:fcbcc5a9b3b53688502a98d096b5e74b2e7635f82f4b8652387aa2c0ecfde910; 2026-10-06:fcbcc5a9b3b53688502a98d096b5e74b2e7635f82f4b8652387aa2c0ecfde910 | — | OBSERVED |
| azure-07n | Azure Blob XML | Parameter name without `=` | Explicit encoding; raw-wire assertion required | 2026-06-06:ee8b1bbe823fa7b05ab61f43df7b4246c5dd9b95b38424397070f089092b5667; 2026-10-06:ee8b1bbe823fa7b05ab61f43df7b4246c5dd9b95b38424397070f089092b5667 | — | OBSERVED |
| azure-07o | Azure Blob XML | Every reserved ASCII character in names under one prefix | Explicit encoding; raw-wire assertion required | 2026-06-06:c3ce8f464452cf9b9c2768813e2843bed218d7a18a57aa191a2868db5d6ecd53; 2026-10-06:c3ce8f464452cf9b9c2768813e2843bed218d7a18a57aa191a2868db5d6ecd53 | — | OBSERVED |
| azure-07p | Azure Blob XML | Trailing space in a prefix | Explicit encoding; raw-wire assertion required | 2026-06-06:06cb6ba69d119b79139a82b63f0eb64b7d31fc23952cec7ee1f36d04423ecce1; 2026-10-06:06cb6ba69d119b79139a82b63f0eb64b7d31fc23952cec7ee1f36d04423ecce1 | — | OBSERVED |
| azure-07q | Azure Blob XML | Raw `=` inside a value | Explicit encoding; raw-wire assertion required | 2026-06-06:8cec1e6fe431cd9b0a10cbba01f44979c3000dd9f314e145498c3a49df785ef2; 2026-10-06:8cec1e6fe431cd9b0a10cbba01f44979c3000dd9f314e145498c3a49df785ef2 | — | OBSERVED |
| azure-07r | Azure Blob XML | Names with control, XML-invalid, whitespace and dot characters | Explicit encoding; raw-wire assertion required | 2026-06-06:dfb1fcaebb615f9fed440bcbdecbfbd55b52acd7c105b7fa7fd72506278d9574; 2026-10-06:dfb1fcaebb615f9fed440bcbdecbfbd55b52acd7c105b7fa7fd72506278d9574 | — | OBSERVED |
| azure-07s | Azure Blob XML | Delimiter listing over dot segments and trailing-slash names | Explicit encoding; raw-wire assertion required | 2026-06-06:4e7f16b73c9ebcaf9bf443d67e85bbd8765fa6d1c90f6c6d6d9efc90317c03d2; 2026-10-06:4e7f16b73c9ebcaf9bf443d67e85bbd8765fa6d1c90f6c6d6d9efc90317c03d2 | — | OBSERVED |
| azure-07t | Azure Blob XML | Raw `+` in a `startFrom` value | Explicit encoding; raw-wire assertion required | 2026-06-06:6339dd1767c46e972be0687c9067ef55bbe56ee8014781f61cc390d31237e407; 2026-10-06:6339dd1767c46e972be0687c9067ef55bbe56ee8014781f61cc390d31237e407 | — | OBSERVED |
| azure-08a | Azure Blob XML | `maxresults=0` | Zero refused pending evidence | 2026-06-06:ae822f01f4df9317a18f7edc643b8e6165516b70b5960162632455bf1c5db524; 2026-10-06:ae822f01f4df9317a18f7edc643b8e6165516b70b5960162632455bf1c5db524 | — | OBSERVED |
| azure-08c | Azure Blob XML | `maxresults=1` walk | Zero refused pending evidence | 2026-06-06:43b91ac4e0ad1e692e947f0f91b6c711eeadeaeedc7678730ef8cdeeb1e7ef5a; 2026-10-06:43b91ac4e0ad1e692e947f0f91b6c711eeadeaeedc7678730ef8cdeeb1e7ef5a | — | OBSERVED |
| azure-08d | Azure Blob XML | `maxresults=5000` walk over exactly 5,000 blobs | Zero refused pending evidence | 2026-06-06:f89a700f5f08fd966521c5fc44d8abb5719307ea9da47da41ddb107cb6b5fa1d; 2026-10-06:f89a700f5f08fd966521c5fc44d8abb5719307ea9da47da41ddb107cb6b5fa1d | — | OBSERVED |
| azure-08e | Azure Blob XML | `maxresults=5001` walk over 5,001 blobs | Zero refused pending evidence | 2026-06-06:1603ad04f551857da584a0f6da8ba40518163b8c8723a33e8cf3c4929a274275; 2026-10-06:1603ad04f551857da584a0f6da8ba40518163b8c8723a33e8cf3c4929a274275 | — | OBSERVED |
| azure-08f | Azure Blob XML | `maxresults=-1` | Zero refused pending evidence | 2026-06-06:f23e2d515d30cc3e9942a812ccbf4251164d15032d24b69becbc1c6f52541934; 2026-10-06:f23e2d515d30cc3e9942a812ccbf4251164d15032d24b69becbc1c6f52541934 | — | OBSERVED |
| azure-08g | Azure Blob XML | `maxresults=abc` | Zero refused pending evidence | 2026-06-06:2e7a43b500f48751e3058aee1c85411ce29385463e6715c9691d6a7688e88c12; 2026-10-06:2e7a43b500f48751e3058aee1c85411ce29385463e6715c9691d6a7688e88c12 | — | OBSERVED |
| azure-08h | Azure Blob XML | `maxresults=2147483648` | Zero refused pending evidence | 2026-06-06:98218df6a6a0d05108a11768a27bed9b18d643c5bafdf22aee1af971b1e1c3b0; 2026-10-06:98218df6a6a0d05108a11768a27bed9b18d643c5bafdf22aee1af971b1e1c3b0 | — | OBSERVED |
| azure-08i | Azure Blob XML | `maxresults=9223372036854775808` | Zero refused pending evidence | 2026-06-06:d9775afea7e3b157c5312051f2013930f1a9346fde111ccaf105712d1cd17238; 2026-10-06:d9775afea7e3b157c5312051f2013930f1a9346fde111ccaf105712d1cd17238 | — | OBSERVED |
| azure-08j | Azure Blob XML | `maxresults=1.5` | Zero refused pending evidence | 2026-06-06:28080c85e99c8efab9b75475bc315ac0e6828710fa091f263a9b10a0ecbc277f; 2026-10-06:28080c85e99c8efab9b75475bc315ac0e6828710fa091f263a9b10a0ecbc277f | — | OBSERVED |
| azure-08k | Azure Blob XML | `maxresults=+2` with a raw plus | Zero refused pending evidence | 2026-06-06:a222e5110cc130a78d8ea8d6abe657b1017ec45a1776fedcd1119ab3964a4d88; 2026-10-06:a222e5110cc130a78d8ea8d6abe657b1017ec45a1776fedcd1119ab3964a4d88 | — | OBSERVED |
| azure-08l | Azure Blob XML | `maxresults=002` | Zero refused pending evidence | 2026-06-06:5eb435f035096fb90d3a87e8ea846932e7e1338fc439e3569bdbd9cbcbf1f5f5; 2026-10-06:5eb435f035096fb90d3a87e8ea846932e7e1338fc439e3569bdbd9cbcbf1f5f5 | — | OBSERVED |
| azure-08m | Azure Blob XML | Duplicate `maxresults` values | Zero refused pending evidence | 2026-06-06:db9a9456b3f9dde70b258c31067721773877b2f5ad28185d04a15fdc3dc410ae; 2026-10-06:db9a9456b3f9dde70b258c31067721773877b2f5ad28185d04a15fdc3dc410ae | — | OBSERVED |
| azure-08n | Azure Blob XML | Empty `maxresults` value | Zero refused pending evidence | 2026-06-06:846cebf7349538964a5aefad9f6f17652c7afe56e84a16ee636fe24ab4351d1d; 2026-10-06:846cebf7349538964a5aefad9f6f17652c7afe56e84a16ee636fe24ab4351d1d | — | OBSERVED |
| azure-08o | Azure Blob XML | `maxresults=2147483647` | Zero refused pending evidence | 2026-06-06:0762b28af073832745c1767d5d241f275c073c0d5b6b3b158f5eeb2ed3bcab42; 2026-10-06:0762b28af073832745c1767d5d241f275c073c0d5b6b3b158f5eeb2ed3bcab42 | — | OBSERVED |
| azure-08p | Azure Blob XML | Duplicate `restype` selector | Zero refused pending evidence | 2026-06-06:1b54e09db68a172c689bef03c9afcd44269ee3eae501089c87385456e18133f8; 2026-10-06:1b54e09db68a172c689bef03c9afcd44269ee3eae501089c87385456e18133f8 | — | OBSERVED |
| azure-09a | Azure Blob XML | Delimiter walk with one entry per page over top-level subtrees | Complete native-marker walk required | 2026-06-06:060c3e0c3c1eecd388fae3284dc1a004c6ab2eaceae8885555f3621f860df7a0; 2026-10-06:060c3e0c3c1eecd388fae3284dc1a004c6ab2eaceae8885555f3621f860df7a0 | — | OBSERVED |
| azure-09b | Azure Blob XML | Default page size walk over 5,001 blobs | Complete native-marker walk required | 2026-06-06:4b5357aaa113da038130c6c842b09f17bbe0cf99d1adf1b7601fab975b586cc9; 2026-10-06:4b5357aaa113da038130c6c842b09f17bbe0cf99d1adf1b7601fab975b586cc9 | — | OBSERVED |
| azure-09c | Azure Blob XML | Delimiter walk with two entries per page | Complete native-marker walk required | 2026-06-06:e60794ce62d1db445ce1f02db509030f86192764575f0abc664cf872ce09ab35; 2026-10-06:e60794ce62d1db445ce1f02db509030f86192764575f0abc664cf872ce09ab35 | — | OBSERVED |
| azure-09e | Azure Blob XML | Flat walk with four entries per page over six names | Complete native-marker walk required | 2026-06-06:1eefa4042aa845e62a30f7573f92aa22d89b3891ceb705ed448dd4c2a59c6496; 2026-10-06:1eefa4042aa845e62a30f7573f92aa22d89b3891ceb705ed448dd4c2a59c6496 | — | OBSERVED |
| azure-09f | Azure Blob XML | Prefix with no matching names | Complete native-marker walk required | 2026-06-06:b3beba0a5e8bd7f3b55ee1cd7fa0cd5b59f982979dd2956ba3690bd173093213; 2026-10-06:b3beba0a5e8bd7f3b55ee1cd7fa0cd5b59f982979dd2956ba3690bd173093213 | — | OBSERVED |
| azure-09g | Azure Blob XML | Malformed constant `marker` | Complete native-marker walk required | 2026-06-06:84e6273217d754a36dbeaf2ac2880becbcbba9cc8fc5aca58f6e63907aaedd19; 2026-10-06:84e6273217d754a36dbeaf2ac2880becbcbba9cc8fc5aca58f6e63907aaedd19 | — | OBSERVED |
| azure-09h | Azure Blob XML | Reused `marker` for the same query | Complete native-marker walk required | 2026-06-06:fa480bf7388100bf2ed678b7aca69189ff4e74f2b27821334f4252ccb1d058e0; 2026-10-06:fa480bf7388100bf2ed678b7aca69189ff4e74f2b27821334f4252ccb1d058e0 | — | OBSERVED |
| azure-09i | Azure Blob XML | `marker` from a different prefix | Complete native-marker walk required | 2026-06-06:064bab1942db0e9adedf7df73b307e95431c615ed161e9702a8d0e2e7550caed; 2026-10-06:064bab1942db0e9adedf7df73b307e95431c615ed161e9702a8d0e2e7550caed | — | OBSERVED |
| azure-09j | Azure Blob XML | `marker` with a changed page size | Complete native-marker walk required | 2026-06-06:d069368368eed163e47d44bbfc007f142469b346f82198382a518178ec8a16fb; 2026-10-06:d069368368eed163e47d44bbfc007f142469b346f82198382a518178ec8a16fb | — | OBSERVED |
| azure-09k | Azure Blob XML | `marker` from a flat walk with an added delimiter | Complete native-marker walk required | 2026-06-06:318f2f3a9067418c2d13eab312d31dd2ce64aab6e1a99a0cf6b077b5773722ae; 2026-10-06:318f2f3a9067418c2d13eab312d31dd2ce64aab6e1a99a0cf6b077b5773722ae | — | OBSERVED |
| azure-09l | Azure Blob XML | Empty `marker` value | Complete native-marker walk required | 2026-06-06:65d6e0072e2d37da823727ecaac81a854223ba527b2d7ee8bba9038d441a06c5; 2026-10-06:65d6e0072e2d37da823727ecaac81a854223ba527b2d7ee8bba9038d441a06c5 | — | OBSERVED |
| azure-09n | Azure Blob XML | Flat walk with two entries per page over delimited names | Complete native-marker walk required | 2026-06-06:7eb16ab3bc4fac516f952e2918c70d2b9262025fa9baf990bafc6303de37fd38; 2026-10-06:7eb16ab3bc4fac516f952e2918c70d2b9262025fa9baf990bafc6303de37fd38 | — | OBSERVED |
| azure-09o | Azure Blob XML | `marker` from a delimiter walk with the delimiter removed | Complete native-marker walk required | 2026-06-06:115e59cc051e2c41165534fb8dcf332eeda1aff6a37f65e5b857d6eab51e88fa; 2026-10-06:115e59cc051e2c41165534fb8dcf332eeda1aff6a37f65e5b857d6eab51e88fa | — | OBSERVED |
| azure-09q | Azure Blob XML | Flat walk with two entries per page over six names | Complete native-marker walk required | 2026-06-06:095fb8288579f0eeecd877e68eec5c30affc2a193ef2d717d047a8aeacc717f5; 2026-10-06:095fb8288579f0eeecd877e68eec5c30affc2a193ef2d717d047a8aeacc717f5 | — | OBSERVED |
| azure-09r | Azure Blob XML | Issued `marker` with its last character changed | Complete native-marker walk required | 2026-06-06:47261ecc9af7a6d323a828ea62b883db91f270df6383f93d0d65d2aa0f8f7aee; 2026-10-06:47261ecc9af7a6d323a828ea62b883db91f270df6383f93d0d65d2aa0f8f7aee | — | OBSERVED |
| azure-09s | Azure Blob XML | Issued `marker` truncated by four characters | Complete native-marker walk required | 2026-06-06:3ed557a9487125fef2ad31e92fec7a08833cc24692100d0414850fece456755b; 2026-10-06:3ed557a9487125fef2ad31e92fec7a08833cc24692100d0414850fece456755b | — | OBSERVED |
| azure-09t | Azure Blob XML | `marker` with a prefix matching only names before it | Complete native-marker walk required | 2026-06-06:08ce82a18fd6635be4891124e962ad785f5082bcfd7db899e86dcd35dfacb69d; 2026-10-06:08ce82a18fd6635be4891124e962ad785f5082bcfd7db899e86dcd35dfacb69d | — | OBSERVED |
| azure-09u | Azure Blob XML | `marker` with an `endBefore` excluding every remaining name | Complete native-marker walk required | 2026-06-06:e21e85f6c90c5104901407544bf6c13cdfeedebce0c9a913375ee2cab425d5aa; 2026-10-06:e21e85f6c90c5104901407544bf6c13cdfeedebce0c9a913375ee2cab425d5aa | — | OBSERVED |
| azure-09v | Azure Blob XML | Issued `marker` with one decoded byte changed and re-encoded | Complete native-marker walk required | 2026-06-06:b5a9df543b99f34d5bd6897f04abe8d58ca5e1cfbf17b8f2d08fd51fa2e0f9a3; 2026-10-06:b5a9df543b99f34d5bd6897f04abe8d58ca5e1cfbf17b8f2d08fd51fa2e0f9a3 | — | OBSERVED |
| azure-10a | Azure Blob XML `2026-06-06` | Java SDK flat auto-pagination | Must run unmodified with endpoint override | 2026-06-06:9e0d4678efdb3340652df3dab3c2a5e524c1dd33375f62f281f9c1c9442a9d19 | — | OBSERVED |
| azure-10b | Azure Blob XML `2026-06-06` | Java SDK hierarchy auto-pagination | Must run unmodified with endpoint override | 2026-06-06:ddfd9880d0701307467685fa9b12d3ba0f807b24dff8fca78aecd90adf2333e1 | — | OBSERVED |
| azure-10c | Azure Blob XML `2026-06-06` | Java SDK `setStartFrom` auto-pagination | Must run unmodified with endpoint override | 2026-06-06:84d7ef26e23232a3ac8c39a17b4ebffbfa53fc590ecc39200eb58a750b7a6c7c | — | OBSERVED |
| azure-10d | Azure Blob XML `2026-10-06` | Java SDK auto-pagination at 2026-10-06 | Requires a pinned SDK that names the version | — | — | UNMEASURED |
| azure-11 | Azure Blob XML | Retry-disabled one-attempt errors; normal SDK retries | Separate request counts | — | — | UNMEASURED |
| azure-12a | Azure Blob XML | Listing of length and segment creation-probe names | Provisional 1,024 UTF-16 units and 254 segments; native creation probes required | 2026-06-06:2d9ffa102d78ec617f7911149c0d67f52bec6868f2c53d992a72878579ba7519; 2026-10-06:2d9ffa102d78ec617f7911149c0d67f52bec6868f2c53d992a72878579ba7519 | — | OBSERVED |
| azure-13a | Azure Blob XML | Explicit client request ID echo | Echo explicit client request ID; XML accept only | 2026-06-06:e9721bc7dd3a723aa024030f116a67d4c449fc098208703f1ecaa9df6e1b87ee; 2026-10-06:e9721bc7dd3a723aa024030f116a67d4c449fc098208703f1ecaa9df6e1b87ee | — | OBSERVED |
| azure-13b | Azure Blob XML | Explicit `Accept: application/xml` | Echo explicit client request ID; XML accept only | 2026-06-06:84ad6ab732236672963f51c8d007f58393531aab843ec3c3d6f91f576d425131; 2026-10-06:84ad6ab732236672963f51c8d007f58393531aab843ec3c3d6f91f576d425131 | — | OBSERVED |
| azure-14a | Azure Blob XML | `include=metadata` | Empty or omitted options only; expansions refused | 2026-06-06:318c0e65b16dc1628c1f2524ad3a77680a33a8ba20021b65da63eb96725a170c; 2026-10-06:318c0e65b16dc1628c1f2524ad3a77680a33a8ba20021b65da63eb96725a170c | — | OBSERVED |
| azure-14b | Azure Blob XML | Empty `include` value | Empty or omitted options only; expansions refused | 2026-06-06:258ff0d64f0ccb2f57bd730ab12e406a32b7e244a690cf64f468a3c21ece56ac; 2026-10-06:258ff0d64f0ccb2f57bd730ab12e406a32b7e244a690cf64f468a3c21ece56ac | — | OBSERVED |
| azure-14c | Azure Blob XML | `include=deleted` with soft delete disabled | Empty or omitted options only; expansions refused | 2026-06-06:a1222459af72d968f08f1972dec04165ef2fb8312474a350224c648928596275; 2026-10-06:a1222459af72d968f08f1972dec04165ef2fb8312474a350224c648928596275 | — | OBSERVED |
| azure-14d | Azure Blob XML | `timeout=1` | Empty or omitted options only; expansions refused | 2026-06-06:bef629c7a89fb105a5d0db642ba8ee26d16aeae839e2f88c0ce146328c5f002b; 2026-10-06:bef629c7a89fb105a5d0db642ba8ee26d16aeae839e2f88c0ce146328c5f002b | — | OBSERVED |
| azure-14e | Azure Blob XML | `timeout=0` | Empty or omitted options only; expansions refused | 2026-06-06:4aafe1c924cc0ea94150106cb44f1b310e1505e4e0ac74e5389744383f7e2a18; 2026-10-06:4aafe1c924cc0ea94150106cb44f1b310e1505e4e0ac74e5389744383f7e2a18 | — | OBSERVED |
| azure-14f | Azure Blob XML | `timeout=abc` | Empty or omitted options only; expansions refused | 2026-06-06:31cb751dadd8df073546876630452725d90a5e8a29cc2a1171bab70d94a057c3; 2026-10-06:31cb751dadd8df073546876630452725d90a5e8a29cc2a1171bab70d94a057c3 | — | OBSERVED |
| azure-14g | Azure Blob XML | `showonly=files` on a flat account | Empty or omitted options only; expansions refused | 2026-06-06:f486b0d01cab2688573b14a89c7436fd00900d245a90f4e036b59a11af72e29d; 2026-10-06:f486b0d01cab2688573b14a89c7436fd00900d245a90f4e036b59a11af72e29d | — | OBSERVED |
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
| Range | Inclusive `startOffset`, exclusive `endOffset`, intersected with prefix | Inclusive `startFrom` in UTF-16 code-unit order; `endBefore` refused for XML |
| Delimiter | Objects and unique prefixes consume one capacity each; offset intersections remain evidence-gated | Interleaved Blob and BlobPrefix consume capacity; a prefix holding a name at or after `startFrom` is kept |
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
preamble bytes. Every live Azure capture, success and error alike, starts its body with
the UTF-8 BOM (EF BB BF) before the declaration, and replay sends the same three bytes.
Sanitization keeps only classified response headers, hashes
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
--matrix docs/replay-provider-conformance.md --captures docs/replay-provider-evidence.tar.gz`
to check every row. `--captures` takes the archive or an evidence directory and reads its
manifests unless `--manifest MANIFEST` is given. Promotion to OBSERVED requires the
provider's manifest, a capture under the evidence root, exact probe request and expected
result, and a checksum that matches that capture's bytes and manifest link.
REPLAY_MATCH additionally requires `--repo-root DIR`, an existing test method, and a
checksummed passing result receipt linked to the same capture. `ProviderMatchReceipt` emits a PASS receipt only after
its GCS, Azure or S3 comparison succeeds. A single exchange is paired strictly with its
replay request after explicit namespace and continuation-token mapping. A walk or SDK
run is compared as a complete ordered inventory with multiplicities and terminal state:
each endpoint follows its own tokens, page counts may differ, and every replay request
must carry the captured scope. It requires a clean tested source tree and binds the test source, Git
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

The live campaign measures native provider behavior once, under hard caps, so that every
later check runs offline. `scripts/provider-conformance/campaign.py` provisions a corpus
inside a fresh random `probe/r<UTC time>-<hex>/` prefix of a dedicated bucket or
container, captures every case of the provider's matrix (`gcs_live.py`, `azure_live.py`,
`s3_live.py`), runs the official SDK walks through `scripts/provider-conformance/java`,
and cleans up. It sends no request until `plan` validates the corpus and a conservative
cost bound against the caps, and each run needs `--confirm-live`. Raw captures stay in a
private campaign directory; `sanitize` writes one provider's publishable evidence:

```bash
python3 scripts/provider-conformance/campaign.py plan --provider gcs --markdown
python3 scripts/provider-conformance/campaign.py run --provider gcs --profile full \
    --campaign-dir PRIVATE_DIR --confirm-live
python3 scripts/provider-conformance/campaign.py sanitize --provider gcs \
    --campaign-dir PRIVATE_DIR --run RUN --out EVIDENCE_DIR/gcs
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
A probe answered 401, 408, 429 or 5xx, or a walk that ends on one, is an incomplete case
rather than an observation; a 400 or 403 is still observed. If the sanitizer refuses any
completed case's capture, the published receipt is INCOMPLETE and names each refused case,
so `promote` attaches nothing from that run.

`promotion.py rows` regenerates the ledger rows and the table above from the case
matrices; `promotion.py promote` attaches captures and receipts from the evidence
archive, then runs the fail-closed validator.

## Evidence archive

`docs/replay-provider-evidence.tar.gz` holds the published evidence: one directory per
provider (`gcs/`, `azure/`) with the campaign index, corpus manifest, sanitized captures,
GCS raw-wire artifacts, replay match receipts, and `replay-expectations.json`. Ledger
paths such as `gcs/captures/gcs-01a.json` are relative to the archive root, and each
checksum names the stored file's bytes, after decompression for a `.json.gz` capture.
Inspect it with `tar -tzf` or `tar -xzf`.

It leaves out two things:

- **S3.** The S3 campaign stopped before it completed, so no S3 capture is published.
- **A second copy of each Azure case.** Azure keeps its `2026-06-06` captures. Each
  `2026-10-06` capture was compared with the `2026-06-06` capture of the same case after
  normalizing the service-version echo, `Date`, request and client request IDs, capture
  provenance, and an error's raw-body digest (the provider's error message carries the
  request ID and time). `azure/version-equivalence.json` records the digests of both
  sides for every compared case. A `2026-10-06` ledger entry cites the `2026-06-06`
  capture and that record, and the validator recomputes the cited capture's normalized
  digest. It cannot recompute the `2026-10-06` side, because that capture is not archived:
  those digests are what the build computed before it deleted the capture, so the record
  is as trustworthy as the sanitized captures it was built from. The validator requires
  the record to carry that capture's own SHA-256 and normalized digest, so a retained copy
  can still be checked against it. A case that differs keeps its `2026-10-06` capture; in
  this campaign all 98 compared cases matched. `azure-02` exists only at `2026-10-06` and
  keeps its capture.
  The `2026-10-06` manifest is the `2026-06-06` manifest with its version changed, so the
  index keeps only its digest. Replay requests carry the captured service version, so a
  receipt for a `2026-06-06` capture says nothing about replay at `2026-10-06`. A folded
  entry carries no receipt, and a row that names both versions stays `OBSERVED` until
  replay is also run and matched at `2026-10-06`.

The normal build reads the archive directly. `NativeEvidenceReplayTest` extracts it into
a temporary directory and replays every capture against replay serving the same corpus;
each outcome must equal its pinned expectation. The Python tests validate the ledger and
this matrix against the archive and check that it rebuilds byte for byte.

The archive changes only with a re-capture, so the test derives the expectation for each
capture from its archived pin by two rules, both in `NativeEvidenceReplayTest.expectations`.
First, the Azure pins were written while replay omitted the UTF-8 BOM, and each pin that
names the BOM also records what the comparison found with the BOM restored. Replay now
sends the BOM, so such a pin expects that remainder: `match` when nothing else differed,
otherwise the remaining mismatch. Second,
[`replay-provider-expectations.json`](replay-provider-expectations.json) records, by
provider and capture as in the archive, each outcome that a later replay change altered:
`match`, or the remaining mismatch. An entry replaces whatever the first rule left for its
capture, and the test fails on an entry for a capture the archive does not have or one that
repeats that expectation. The file holds the 11 Azure captures that match since replay lists
Azure in UTF-16 code-unit order and serves `startFrom` with a delimiter: `azure-04b` to
`azure-04e`, `azure-04l`, `azure-04m`, `azure-06a`, `azure-06b`, `azure-06e`, `azure-06f`
and `azure-10a`. Of the 103 Azure captures, 62 now expect `match`: 51 by the first rule and
11 by the second.

The archive holds no receipts for those 62 matches, so their rows stay `OBSERVED`. Writing
the receipts and rebuilding the archive, as in steps 2 and 3 below, promotes the five
single-version rows `azure-01`, `azure-02`, `azure-10a`, `azure-10b` and `azure-10c` to
`REPLAY_MATCH`. The other 57 also name `2026-10-06`, whose capture is folded, so they stay
`OBSERVED` as described above. `-PproviderReport=DIR` writes each capture's outcome to
`DIR/<provider>-outcomes.json`, from which a rebuild can write current expectations. A
rebuilt archive pins replay as it then is, so neither rule applies to it: remove the BOM rule
and `replay-provider-expectations.json` in the same change.

`scripts/provider-conformance/evidence_archive.py` builds the archive from an evidence
directory with sorted entries, zero timestamps and owners, mode 0644, and gzip level 9
without a name or time, so the same files always give the same bytes. It archives only
regular files and stops at a link anywhere in an evidence directory. Build it with classic
zlib: zlib-ng compresses the same tar to different bytes, so `build` refuses it there and
`check` compares only the tar and the gzip header. To publish a new campaign:

1. Build the archive from the sanitized evidence and commit it:

   ```bash
   python3 scripts/provider-conformance/evidence_archive.py build --evidence EVIDENCE_DIR \
       --provider gcs --provider azure --fold azure=2026-06-06
   python3 scripts/provider-conformance/evidence_archive.py check
   ```

2. On that clean commit, write receipts for the captures replay matches into the evidence
   directory. Keep the jar: it records the commit, and the receipts name its checksum.
   Each receipt requires a clean working tree, so `EVIDENCE_DIR` must be outside the
   repository or under a gitignored path.

   ```bash
   ./gradlew :swath-replay:jar
   ./gradlew :swath-replay:test --tests '*NativeEvidenceReplayTest' \
       -PproviderReceipts=EVIDENCE_DIR -PproviderDistribution=REPLAY_JAR
   ```

3. Rebuild the archive as in step 1, promote, and commit both:

   ```bash
   python3 scripts/provider-conformance/promotion.py promote \
       --ledger docs/replay-provider-ledger.json --matrix docs/replay-provider-conformance.md \
       --repo-root . --distribution REPLAY_JAR
   ```

Change the archive only with a re-capture. Git keeps every committed version, so each
rebuild adds a full copy, about 1.2 MB, to the repository history.
