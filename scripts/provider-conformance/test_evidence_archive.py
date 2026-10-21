#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Offline checks of the committed provider evidence archive and its builder."""

import gzip
import io
import os
import tarfile
import tempfile
import unittest
import zlib
from pathlib import Path

import evidence
import evidence_archive


class EvidenceArchiveTest(unittest.TestCase):
    def test_committed_archive_rebuilds_byte_for_byte(self):
        committed = evidence.EVIDENCE_ARCHIVE.read_bytes()
        with evidence.evidence_root(evidence.EVIDENCE_ARCHIVE) as root:
            rebuilt = evidence_archive.pack(root)
        self.assertEqual(gzip.decompress(rebuilt), gzip.decompress(committed))
        self.assertEqual(rebuilt[:10], committed[:10])
        # Deflate output is fixed for classic zlib; zlib-ng compresses the same stream differently.
        if "zlib-ng" not in zlib.ZLIB_RUNTIME_VERSION:
            self.assertEqual(rebuilt, committed)

    def test_pack_ignores_file_metadata_and_sorts_entries(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "gcs/captures").mkdir(parents=True)
            (root / "gcs/index.json").write_bytes(b"{}\n")
            (root / "gcs/captures/b.json").write_bytes(b"b\n")
            (root / "gcs/captures/a.json.gz").write_bytes(evidence.deterministic_gzip(b"a\n"))
            first = evidence_archive.pack(root)
            os.chmod(root / "gcs/index.json", 0o600)
            os.utime(root / "gcs/captures/b.json", (1_000_000, 1_000_000))
            self.assertEqual(evidence_archive.pack(root), first)
        self.assertEqual(first[:10], b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff")
        with tarfile.open(fileobj=io.BytesIO(first), mode="r:gz") as archive:
            members = archive.getmembers()
            self.assertEqual([m.name for m in members],
                             ["gcs/captures/a.json.gz", "gcs/captures/b.json", "gcs/index.json"])
            self.assertTrue(all(m.isfile() and m.mtime == 0 and m.mode == 0o644 and m.uid == m.gid == 0
                                and m.uname == m.gname == "" for m in members))
            # Already-compressed captures are stored unchanged.
            self.assertEqual(archive.extractfile(members[0]).read(), evidence.deterministic_gzip(b"a\n"))

    def test_pack_refuses_links(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "gcs").mkdir()
            (root / "gcs/index.json").write_bytes(b"{}\n")
            (root / "gcs/link.json").symlink_to(root / "gcs/index.json")
            with self.assertRaisesRegex(ValueError, "only regular files"):
                evidence_archive.pack(root)

    def test_reader_refuses_entries_other_than_regular_files(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "evidence.tar.gz"
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                link = tarfile.TarInfo("gcs/index.json")
                link.type = tarfile.SYMTYPE
                link.linkname = "/etc/passwd"
                archive.addfile(link)
            path.write_bytes(evidence.deterministic_gzip(buffer.getvalue()))
            with self.assertRaisesRegex(ValueError, "only regular files"):
                with evidence.evidence_root(path):
                    pass


if __name__ == "__main__":
    unittest.main()
