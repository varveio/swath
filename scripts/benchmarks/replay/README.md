<!-- SPDX-License-Identifier: Apache-2.0 -->
# Replay benchmark runners

Python runners for paired, interleaved replay server benchmarks and their resource arms:

| File | What it does |
| --- | --- |
| `run_pair.py` | interleaved baseline/candidate pairs on disjoint client and server CPU sets; paired log-ratio confidence intervals, A/A noise control and pilot sizing of the repetition count |
| `run_mixed.py` | a fixed 12-round, four-arm (three isolated protocols and one mixed) open-loop comparison from a predeclared plan |
| `run_canned_http.py` | an HTTP client ceiling against a canned server |
| `resource_fault.py` | response-budget and slow-client arms: HTTP response parsing, slow-reader and trickling sockets, budget sizing |
| `resource_observer.py` | samples RSS, file descriptors, threads and `/metrics` of a running arm against declared caps |
| `make_longkey_fixture.py` | a deterministic long-key capture, stamped through `sort-fixture` |

As written, the runners drive the Java replay server (a `swath-replay` `installDist` launcher,
`--java-home`, `jcmd` native-memory summaries) and the Java benchmark client classes
(`io.varve.swath.replay.bench.*`). Both left `main` with the Java implementation and are at
the tag `java-final`, so running a benchmark end to end needs a `java-final` build. The
paired statistics and pilot sizing, the HTTP parsing and socket fault code, and the `/proc`
and metrics sampling do not depend on Java. Their tests run without a JDK, and `ci.yml`
`scripts` runs them:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/benchmarks/replay -p 'test_*.py'
```

The full usage guide for the Java runs is the
[README at `java-final`](https://github.com/varveio/swath/blob/java-final/scripts/benchmarks/replay/README.md).
