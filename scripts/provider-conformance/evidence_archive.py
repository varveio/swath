#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Build and check the committed provider evidence archive.

The archive is a plain tar of per-provider evidence directories (`gcs/`, `azure/`), gzipped
at level 9. Entries are sorted, carry a zero timestamp, owner and group, and mode 0644, and
the gzip header has no name or timestamp, so rebuilding from the same files gives the same
bytes. Files are stored unchanged: a capture keeps the bytes its ledger checksum names.

`--fold PROVIDER=VERSION` keeps one service version's captures: a capture at another
version that equals the reference capture once `evidence.version_normalized` is replaced by
an entry in `version-equivalence.json` with both digests; a capture that differs is kept.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

import evidence


def pack(root: Path) -> bytes:
    """Deterministic tar.gz of every regular file under `root`."""
    files = sorted((path.relative_to(root).as_posix(), path) for path in root.rglob("*")
                   if path.is_symlink() or not path.is_dir())
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, path in files:
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"evidence holds only regular files: {name}")
            payload = path.read_bytes()
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mode = 0o644
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            archive.addfile(info, io.BytesIO(payload))
    return evidence.deterministic_gzip(buffer.getvalue())


def write_json(path: Path, value: object, sort_keys: bool = False) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=sort_keys) + "\n", encoding="utf-8")


def fold_versions(provider_dir: Path, reference: str) -> dict:
    """Fold a provider's other service versions into its reference version, in place."""
    if (provider_dir / evidence.VERSION_EQUIVALENCE_FILE).exists():
        raise ValueError(f"{provider_dir.name} evidence is already folded; build it without --fold")
    index = evidence.strict_json((provider_dir / "index.json").read_text(encoding="utf-8"))
    expectations_path = provider_dir / "replay-expectations.json"
    expectations = evidence.strict_json(expectations_path.read_text(encoding="utf-8"))
    reference_manifest = evidence.strict_json((provider_dir / index["manifests"][reference]["file"])
                                              .read_text(encoding="utf-8"))
    for version, entry in index["manifests"].items():
        path = provider_dir / entry.get("file", "")
        if version != reference and path.is_file() and evidence.strict_json(path.read_text(encoding="utf-8")) \
                == dict(reference_manifest, api_version=version):
            path.unlink()
            index["manifests"][version] = {"derived_from": reference, "sha256": entry["sha256"]}
    record = {"schema_version": evidence.VERSION_EQUIVALENCE_SCHEMA, "provider": index["provider"],
              "reference_version": reference, "normalized_headers": sorted(evidence.VERSION_NORMALIZED_HEADERS),
              "normalized_fields": [*evidence.VERSION_PROVENANCE_FIELDS, "raw_body_sha256 of error responses"],
              "cases": {}}

    def side(entry: dict) -> tuple[dict, str]:
        payload = evidence.read_evidence(provider_dir / entry["file"])
        if hashlib.sha256(payload).hexdigest() != entry["sha256"]:
            raise ValueError(f"{entry['file']} differs from its index checksum")
        capture = evidence.strict_json(payload.decode("utf-8"))
        return {"sha256": entry["sha256"], "normalized_sha256": evidence.normalized_sha256(capture)}, entry["file"]

    for key, entry in sorted(index["captures"].items()):
        case_id, _, version = key.partition("~")
        reference_key = f"{case_id}~{reference}"
        if not version or version == reference or reference_key not in index["captures"]:
            continue
        reference_entry = index["captures"][reference_key]
        cited, cited_file = side(reference_entry)
        observed, observed_file = side(entry)
        details = {name: value for name, value in entry.items() if name not in {"file", "sha256"}}
        equivalent = cited["normalized_sha256"] == observed["normalized_sha256"] and details == {
            name: value for name, value in reference_entry.items() if name not in {"file", "sha256"}}
        record["cases"][key] = {"equivalent": equivalent,
                                "reference": {"capture": reference_key, "file": cited_file, **cited},
                                "version": observed if equivalent else {"file": observed_file, **observed}}
        if equivalent:
            (provider_dir / observed_file).unlink()
            del index["captures"][key]
            expectations.pop(key, None)
    write_json(provider_dir / evidence.VERSION_EQUIVALENCE_FILE, record)
    write_json(provider_dir / "index.json", index, sort_keys=True)
    write_json(expectations_path, expectations)
    return record


def build(evidence_dir: Path, providers: list[str], out: Path, folds: dict[str, str]) -> bytes:
    """Archive the named providers' evidence directories, folding service versions as asked."""
    with tempfile.TemporaryDirectory(prefix="provider-evidence-") as temp:
        stage = Path(temp)
        for provider in providers:
            if not (evidence_dir / provider / "index.json").is_file():
                raise ValueError(f"{provider} has no evidence index under {evidence_dir}")
            shutil.copytree(evidence_dir / provider, stage / provider)
            if provider in folds:
                record = fold_versions(stage / provider, folds[provider])
                kept = sorted(key for key, case in record["cases"].items() if not case["equivalent"])
                print(f"{provider}: {len(record['cases']) - len(kept)} captures folded into "
                      f"{folds[provider]}; kept {kept or 'none'}")
        payload = pack(stage)
    out.write_bytes(payload)
    return payload


def rebuilds_identically(archive: Path) -> bool:
    """Whether packing the archive's own files reproduces its bytes."""
    with evidence.evidence_root(archive) as root:
        return pack(root) == archive.read_bytes()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    make = sub.add_parser("build", help="archive sanitized evidence directories")
    make.add_argument("--evidence", type=Path, required=True,
                      help="directory holding one evidence directory per provider")
    make.add_argument("--provider", action="append", required=True, choices=sorted(evidence.API_VERSIONS))
    make.add_argument("--fold", action="append", default=[], metavar="PROVIDER=VERSION",
                      help="keep VERSION's captures and record the other versions' equivalence")
    make.add_argument("--out", type=Path, default=evidence.EVIDENCE_ARCHIVE)
    check = sub.add_parser("check", help="verify the archive rebuilds byte for byte")
    check.add_argument("--archive", type=Path, default=evidence.EVIDENCE_ARCHIVE)
    args = parser.parse_args()
    if args.command == "build":
        folds = dict(fold.split("=", 1) for fold in args.fold)
        payload = build(args.evidence, sorted(set(args.provider)), args.out, folds)
        print(f"{args.out}: {len(payload)} bytes")
        return
    if not rebuilds_identically(args.archive):
        raise SystemExit(f"{args.archive} does not rebuild byte for byte")
    print(f"{args.archive} rebuilds byte for byte")


if __name__ == "__main__":
    main()
