STATUS: done

## Current task

Identify swath distinctly on every S3 request while preserving the AWS SDK's diagnostic
User-Agent fields. The product token lets S3 operators and request logs attribute swath's
high-throughput listing workload to the actual application and release.

## Where things stand

- Branch: `feat/swath-user-agent`.
- Draft PR: `https://github.com/varveio/swath/pull/104` targeting `main`.
- Implementation commit: `9836557 feat(s3): identify swath in user agent`.
- `S3ClientFactory` prepends `swath/<Implementation-Version>` through the AWS SDK's
  `USER_AGENT_PREFIX`; exploded builds fall back to `swath/development`.
- A loopback HTTP test sends a real `ListObjectsV2` request through the production Apache client
  and asserts the received header begins with the swath token and retains the AWS SDK/S3 markers.
- Scoped `S3ClientFactoryTest` and the full `./gradlew build` integration gate pass.
- Independent review returned READY with no findings.

## Decisions (locked)

- Use `swath/<version>`, not `varve/swath` or `varve-swath/<version>`.
- Do not put a URL, bucket, prefix, hostname, concurrency, run ID, commit, sort mode, or algorithm
  path in the User-Agent.
- Preserve the AWS SDK-generated suffix and the operator-controlled AWS AppId channel.
- Use the canonical manifest `Implementation-Version`; use `development` only when version metadata
  is absent.

## Next step

No implementation or publishing work remains. The human gate should review PR #104 and mark it
ready or merge it when satisfied.

## Files that matter

- `swath-s3/src/main/java/io/varve/swath/store/s3/S3ClientFactory.java`
- `swath-s3/src/test/java/io/varve/swath/store/s3/S3ClientFactoryTest.java`
- `build-logic/src/main/kotlin/swath.java-conventions.gradle.kts`

## Things to avoid

- Do not replace the complete SDK User-Agent; the SDK and transport tokens are useful diagnostics.
- Do not hard-code a release number or duplicate Gradle's version source.
- Do not hard-code AWS AppId to `swath`; operators may use that separate channel for their own
  deployment identity.
