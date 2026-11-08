#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Check swath-written datasets against SeekParquet with the format's own conformance adapters.

usage: seekparquet_conformance.py --adapter CMD --reference CMD DATASET...

For each dataset (a directory this build wrote), the reference adapter of the SeekParquet
conformance suite (conformance/harness/reference_adapter.py: a full scan with pyarrow that checks
every rule it can, page index and page bounds included) and the adapter under test (the seekparquet
Go module's validator) must each validate it at the full and digest levels; then the indexed reader under test (CMD, an adapter speaking conformance/protocol.md, for
example the seekparquet Go module's) must answer a set of range, lookup and delimited-listing
requests, whole and paginated through their cursors, exactly as the reference adapter does. The
requests start, end and list at keys taken from the dataset itself, groups of repeated keys
included. Exit status 0 only when every check passed.
"""
import argparse
import base64
import json
import shlex
import subprocess
import sys

# The value columns each schema id projects (every column the protocol encodes).
COLUMNS = {
    "swath/s3-objects/v1": ["size", "last_modified", "etag", "storage_class", "owner_id", "checksum_algorithm"],
    "swath/s3-versions/v1": ["size", "last_modified", "etag", "version_id", "is_latest", "is_delete_marker"],
    "swath/gcs-objects/v1": ["size", "last_modified", "etag", "generation", "md5", "time_created"],
    "swath/gcs-generations/v1": ["size", "generation", "time_deleted"],
    "swath-local/v1": ["size", "last_modified", "kind", "link_target"],
}


class Adapter:
    def __init__(self, command):
        self.proc = subprocess.Popen(shlex.split(command), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.n = 0

    def ask(self, request):
        self.n += 1
        request = dict(request, id=str(self.n))
        self.proc.stdin.write(json.dumps(request) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f"adapter closed its output on {request}")
        response = json.loads(line)
        if response.get("id") != request["id"]:
            raise RuntimeError(f"response {response} to {request}")
        del response["id"]
        return response

    def close(self):
        self.proc.stdin.close()
        if self.proc.wait(timeout=30) != 0:
            raise RuntimeError(f"adapter exited {self.proc.returncode}")


def b64(raw):
    return base64.b64encode(raw).decode()


def key(entry):
    return base64.b64decode(entry["key_base64"])


def cursor(entry):
    if entry["kind"] == "prefix":
        return {"prefix_base64": entry["key_base64"]}
    return {"key_base64": entry["key_base64"], "offset": entry["offset"]}


def pages(adapter, request, limit):
    """The request's entries, page by page through each page's last cursor."""
    out, after = [], None
    while True:
        page = dict(request, limit=limit)
        if after is not None:
            page["after"] = after
        response = adapter.ask(page)
        if response.get("status") != "ok":
            return response
        out += response["entries"]
        if len(response["entries"]) < limit:
            return {"status": "ok", "entries": out}
        after = cursor(response["entries"][-1])


def check_dataset(path, adapter, reference, failures):
    with open(f"{path}/manifest.json") as f:
        manifest = json.load(f)
    rows = sum(p["rows"] for p in manifest["parts"])
    columns = COLUMNS.get(manifest["schema_id"], [])
    checked = 0

    def fail(what, got, want):
        failures.append(f"{path}: {what}: got {json.dumps(got)[:400]}, reference {json.dumps(want)[:400]}")

    if not all(p.get("sha256") for p in manifest["parts"]):
        failures.append(f"{path}: a part declares no sha256: the digest level would check nothing")
    for who, label in ((reference, "reference"), (adapter, "adapter")):
        for level in ("full", "digest"):
            report = who.ask({"op": "validate", "dataset": path, "level": level})
            results = {name: lvl.get("result") for name, lvl in report.get("levels", {}).items()}
            if report.get("status") != "ok" or not results or set(results.values()) != {"complete"}:
                failures.append(f"{path}: {label}: validate {level}: {json.dumps(report)}")
    for who in (adapter, reference):
        if (response := who.ask({"op": "open", "dataset": path})).get("status") != "ok":
            failures.append(f"{path}: open: {json.dumps(response)}")
            return 0

    def same(request, limit=None):
        nonlocal checked
        request = dict(request, dataset=path)
        if limit is None:
            got, want = adapter.ask(request), reference.ask(request)
        else:
            got, want = pages(adapter, request, limit), pages(reference, request, limit)
        checked += 1
        if got != want:
            fail(json.dumps(request), got, want)
        return want

    whole = same({"op": "range", "limit": rows + 1, "columns": columns})
    entries = whole.get("entries", [])
    if len(entries) != rows:
        failures.append(f"{path}: a whole range returned {len(entries)} rows, the manifest {rows}")
    same({"op": "range", "columns": columns}, limit=997)
    keys = sorted({key(e) for e in entries})
    sample = keys[:: max(1, len(keys) // 40)] + keys[-2:]
    for i, k in enumerate(sample):
        end = sample[min(i + 3, len(sample) - 1)]
        for exclusive in (False, True):
            same({"op": "range", "start_base64": b64(k), "start_exclusive": exclusive, "end_base64": b64(end),
                  "limit": 50, "columns": columns[:1]})
        same({"op": "range", "start_base64": b64(k), "end_base64": b64(k + b"\0"), "columns": columns[:1]}, limit=100)
        same({"op": "range", "prefix_base64": b64(k[: len(k) // 2]), "limit": 30})
    prefixes = [b""]
    while prefixes and checked < 400:
        prefix = prefixes.pop(0)
        listing = same({"op": "list", "prefix_base64": b64(prefix), "delimiter_base64": b64(b"/"), "columns": columns[:1]},
                       limit=7)
        same({"op": "list", "prefix_base64": b64(prefix), "delimiter_base64": b64(b"/"), "limit": 1000})
        found = [key(e) for e in listing.get("entries", []) if e["kind"] == "prefix"]
        prefixes += found[:: max(1, len(found) // 4)]
    return checked


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--adapter", required=True, help="the reader under test (a conformance adapter command)")
    ap.add_argument("--reference", required=True, help="the suite's reference adapter command")
    ap.add_argument("datasets", nargs="+")
    args = ap.parse_args()
    adapter, reference = Adapter(args.adapter), Adapter(args.reference)
    failures = []
    for who, name in ((adapter, "adapter"), (reference, "reference")):
        hello = who.ask({"op": "hello"})
        if {"format_version": 0, "draft_revision": 1} not in hello.get("supports", []):
            failures.append(f"{name} does not read draft revision 1: {hello}")
        print(f"{name}: {hello.get('implementation')} {hello.get('version')} targets={hello.get('targets')}")
    for path in args.datasets:
        before = len(failures)
        n = check_dataset(path, adapter, reference, failures)
        print(f"{'PASS' if len(failures) == before else 'FAIL'} {path}: validated full and digest, {n} requests agree"
              if len(failures) == before else f"FAIL {path}: {len(failures) - before} failures after {n} requests")
    adapter.close()
    reference.close()
    for f in failures:
        print("FAIL " + f)
    print("RESULT:", "OK" if not failures else "FAILED")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
