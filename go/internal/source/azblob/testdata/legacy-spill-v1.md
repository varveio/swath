# Legacy Azure durable spill fixture

`legacy-spill-v1.tar.gz` contains `.swath/spill/durable/ledger` and four sealed,
checksummed spill files written at PR #460's reviewed head
`4b038173ddfab235da2d8135dd0a41987c3a1f83`, with the original Azure codec
from its base `520fdeb8527180230d4561f5df90d8c667714b19` substituted for the
new payload codec. The writer's durable row codec revision was still 1.

The run UUID is `00112233445566778899aabbccddeeff`, the journal identity digest
is the test string `ident`, and the schema ID is `swath/azblob-objects/v1`.
Six fenced ranges `r0` through `r5` each emitted three pages of 200 rows,
keyed `rN/PPP-JJJ`, with size JJJ and ETag `etag`. Range 0 held the frontier
while the other workers spilled, under a 32 KiB reorder budget. Four files
were certified. Files preserve their real header, frame and whole-file CRCs
and ledger records; no bytes were patched after writing.

The upgrade test intentionally reads these fixed old bytes rather than writing
its fixture with the current codec or current durable revision.
