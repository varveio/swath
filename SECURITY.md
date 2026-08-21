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
  treats them byte-exactly and escapes control characters before printing them to
  a terminal. The `--raw-output` mode opts out of that escaping and must only be
  pointed at trusted downstream consumers.
- **A checkpoint is trusted only if swath created it.** `--resume` reads a local
  checkpoint database that swath wrote for a prior run of the same command. Never
  resume from a checkpoint supplied by an untrusted party.
- **A hostile or compromised endpoint is a considered actor.** Against a
  malicious `--endpoint-url`, swath bounds pathological listing *behaviour* —
  stuck cursors, redirect loops, throttle storms — to a *resumable exit* rather
  than an unbounded hang, and it refuses outright, without retrying, any page
  carrying more entries than the request's `max-keys` bound. It does **not** bound
  the size of an individual HTTP response body, which the AWS SDK decodes before
  swath inspects it. Reports that defeat the bounds above (a response shape that
  makes swath hang, loop unbounded, or accept a page past the entry bound) are in
  scope.
- **The replay server has no authentication.** `swath-replay` is a
  dev/test tool. Bind it to localhost only; never expose it on an untrusted
  network. Its separately published image is not supported as a production service,
  but reports of it reading outside its intended fixture directory are in
  scope.
- **Credentials are never taken inline and never persisted.** Credentials resolve
  through the standard AWS SDK provider chain. S3 and OTLP endpoint URIs reject
  userinfo, all query components, and fragments; use the AWS credential chain and
  the collector's external authentication configuration instead. Persisted argv
  replaces endpoint values with a fixed redaction marker, and endpoint-validation
  errors never echo the rejected value. Untrusted bucket/prefix text is
  control-escaped before logging. A report that swath logs or writes a credential,
  or permits an untrusted value to inject a log line, is in scope.
