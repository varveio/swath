#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Build and check the committed provider evidence archive.

The archive is a plain tar of per-provider evidence directories (`gcs/`, `azure/`), gzipped
at level 9. Entries are sorted, carry a zero timestamp, owner and group, and mode 0644, and
the gzip header has no name or timestamp, so rebuilding from the same files gives the same
bytes. Files are stored unchanged: a capture keeps the bytes its ledger checksum names.
"""

from __future__ import annotations

import argparse
import io
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


def build(evidence_dir: Path, providers: list[str], out: Path) -> bytes:
    """Archive the named providers' evidence directories."""
    with tempfile.TemporaryDirectory(prefix="provider-evidence-") as temp:
        stage = Path(temp)
        for provider in providers:
            if not (evidence_dir / provider / "index.json").is_file():
                raise ValueError(f"{provider} has no evidence index under {evidence_dir}")
            shutil.copytree(evidence_dir / provider, stage / provider)
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
    make.add_argument("--out", type=Path, default=evidence.EVIDENCE_ARCHIVE)
    check = sub.add_parser("check", help="verify the archive rebuilds byte for byte")
    check.add_argument("--archive", type=Path, default=evidence.EVIDENCE_ARCHIVE)
    args = parser.parse_args()
    if args.command == "build":
        payload = build(args.evidence, sorted(set(args.provider)), args.out)
        print(f"{args.out}: {len(payload)} bytes")
        return
    if not rebuilds_identically(args.archive):
        raise SystemExit(f"{args.archive} does not rebuild byte for byte")
    print(f"{args.archive} rebuilds byte for byte")


if __name__ == "__main__":
    main()
