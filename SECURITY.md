# Security Policy

## Reporting a vulnerability

Please report suspected security vulnerabilities **privately**, not through a
public issue.

Use GitHub's [private vulnerability reporting][gh-report] on this repository
(the **Security** tab → **Report a vulnerability**). If that is unavailable to
you, email **security@varve.io** with the details.

Please include enough to reproduce: the affected version or commit, the command
line, and the observed vs. expected behavior. We aim to acknowledge a report
within a few business days and will keep you updated as we investigate.

Please do not disclose the issue publicly until we have had a chance to release a
fix and coordinate a disclosure timeline with you.

[gh-report]: https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability

## Supported versions

swath is pre-1.0. Security fixes are applied to the latest release and to `main`.
Older tags are not maintained.

## Threat model

swath is a read-only object-store lister. Understanding what it does and does not
trust helps scope what counts as a vulnerability.

- **Object keys are untrusted input.** Keys are arbitrary byte sequences. swath
  treats them byte-exactly. Every human-readable line on stderr (warnings, errors,
  the start and dataset lines) writes control characters, line separators and
  bytes that are not valid UTF-8 as visible escapes such as `\n` and `\x1b`, so a
  hostile name cannot forge a line or drive the terminal; JSON log lines carry the
  exact string with those characters as `\u` escapes, and bytes that are not
  valid UTF-8 read back as U+FFFD there. Rows keep the exact bytes (the `table` and
  TSV formats escape them, JSONL has `key_base64`).
- **A hostile or compromised endpoint is a considered actor.** A response from a
  malicious `--endpoint-url` that makes swath hang, loop without bound, or exhaust
  memory is in scope.
- **The replay server has no authentication.** `swath-replay` is a
  dev/test tool. Bind it to localhost only; never expose it on an untrusted
  network. Its separately published image is not supported as a production service,
  but reports of it reading outside its intended fixture directory are in
  scope.
- **Credentials.** A report that swath logs or writes a credential, or permits an
  untrusted value to inject a log line, is in scope.
