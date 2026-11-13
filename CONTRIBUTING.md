# Contributing to swath

Thanks for your interest in swath. This guide covers the normal build, test, and pull
request workflow.

## Ground rules

- Be respectful. This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).
- Report security-sensitive findings through [SECURITY.md](SECURITY.md), not a public
  issue.
- Open an issue before a substantial behavioral or architectural change so the intended
  user contract and implementation approach can be discussed first.
- Keep user documentation and code behavior in the same change. Use
  [the documentation style guide](docs/style.md) for project casing, terminology,
  consistency claims, evidence, and ownership.

## Build and test

swath lives in `go/` and uses the toolchain declared in `go/go.mod`. Build it with `cd go && ./build.sh -o swath`. The Go gate is
`go vet ./... && go test -race ./...`; run focused tests locally and use the sharded CI
for the full suite, as described in [testing](go/docs/internals/testing.md). For a provider, follow the
[provider checklist](go/docs/internals/providers.md#adding-a-provider), including reader, verifier
and conformance integration outside its directory. Regenerate CLI documentation with
`cd go && UPDATE=1 go test ./cmd/swath -run '^TestCLIDoc'`; do not edit the generated synopsis and option blocks.

Documentation is checked like code: `cd go && go test ./docs` checks the relative links (inline,
reference definitions, HTML `href`) and their anchors in the Markdown under `go/`; it does not fetch
external URLs, and the library modules' examples (`go/objfile`, `go/reader`) are `Example`
tests. Prefer an `Example` test to a Markdown code block for library code, so that it compiles and runs.

## License headers

Every Go source file carries an SPDX license header; add one to a new file.

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
4. For `go/`, run the tests of the packages you changed (`go vet ./... && go test -race` on
   them); CI runs the rest.
5. Open a pull request explaining the user-visible change, technical rationale, and the
   checks you ran.
