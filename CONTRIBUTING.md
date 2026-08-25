# Contributing to Swath

Thanks for your interest in Swath. This guide covers the normal build, test, and pull
request workflow.

## Ground rules

- Be respectful. This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).
- Report security-sensitive findings through [SECURITY.md](SECURITY.md), not a public
  issue.
- Open an issue before a substantial behavioral or architectural change so the intended
  user contract and implementation approach can be discussed first.
- Keep user documentation and code behavior in the same change. Use
  [the documentation style guide](docs/style.md) for product casing, terminology,
  consistency claims, evidence, and ownership.

## Build and test

Swath is a multi-module Gradle build and requires **JDK 25**. The wrapper supplies Gradle,
but JDK 25 must be available to it.

```bash
./gradlew build -PnoIntegration  # Docker-free per-commit gate
./gradlew build                  # full integration gate (Docker/LocalStack)
./gradlew test                   # fast test tier
./gradlew spotlessApply          # format sources and add SPDX headers
```

Deep, performance, kill-9 resume, replay-conformance, and other opt-in tiers are not all
part of a plain build. The canonical tier map is in
[`docs/ops/dev/TESTING.md`](docs/ops/dev/TESTING.md).

## License headers

Every source file carries an SPDX license header. Spotless applies and checks it. Run
`./gradlew spotlessApply` after adding a source file; the build rejects a missing header.

## Developer Certificate of Origin

Contributions are accepted under the [Developer Certificate of Origin][dco]. Add a
`Signed-off-by` line to every commit to certify that you wrote the patch or otherwise have
the right to submit it under the project's license:

```text
Signed-off-by: Your Name <your.email@example.com>
```

Git adds the line when committing with `-s`:

```bash
git commit -s -m "Describe the change"
```

The sign-off must match the commit author identity.

[dco]: https://developercertificate.org/

## Licensing of contributions

By contributing, you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE), the same license as the project.

## Pull requests

1. Fork the repository or create a topic branch.
2. Keep the change focused and add tests for behavior that could regress.
3. Update the canonical documentation owner rather than adding a second detailed copy.
4. Run `./gradlew build -PnoIntegration`; run the full integration gate when the change
   touches S3 behavior, packaging, or an integration boundary.
5. Open a pull request explaining the user-visible change, technical rationale, and the
   checks you ran.
