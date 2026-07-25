# Contributing to swath

Thanks for your interest in swath. This guide covers how to build, test, and
submit changes.

## Ground rules

- Be respectful — this project follows the [Code of Conduct](CODE_OF_CONDUCT.md).
- For anything security-sensitive, follow [SECURITY.md](SECURITY.md) and report
  privately rather than opening a public issue.
- Open an issue to discuss a substantial change before writing it, so we can
  agree on the approach.

## Building and testing

swath is a multi-module Gradle build on **Java 25**. The Gradle wrapper pins the
toolchain, so you do not need a matching JDK installed globally.

```sh
./gradlew build          # compile + run the fast test tier + assemble artifacts
./gradlew test           # fast test tier only
./gradlew spotlessApply  # apply the SPDX license header to new/changed sources
```

Some tests are gated behind opt-in tiers (`-Pdeep`, `-Pperf`, integration tests
via Testcontainers). The fast tier is what runs per commit; see
`docs/ops/dev/TESTING.md` for the full tier map.

## License headers

Every source file carries an SPDX license header, enforced by Spotless and
checked in CI (`./gradlew spotlessCheck`, part of `check`). If you add a file,
run `./gradlew spotlessApply` to stamp it — the build will otherwise fail with a
missing-header error.

## Developer Certificate of Origin (DCO)

Contributions are accepted under the [Developer Certificate of Origin][dco]. It
is a lightweight statement that you wrote the patch or otherwise have the right
to submit it under the project's license. You certify the DCO by adding a
`Signed-off-by` line to each commit:

```
Signed-off-by: Your Name <your.email@example.com>
```

Git adds this line for you when you commit with `-s`:

```sh
git commit -s -m "your message"
```

The name and email must match your commit author identity. Every commit in a
pull request needs the sign-off.

[dco]: https://developercertificate.org/

## Licensing of contributions

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE), the same license as the project.

## Submitting a pull request

1. Fork the repository and create a topic branch.
2. Make your change with tests; keep commits focused and signed off (`-s`).
3. Run `./gradlew build` and confirm it is green.
4. Open a pull request describing what changed and why.
