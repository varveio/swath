#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Offline checks of the committed provider evidence archive and its builder."""

import base64
import copy
import gzip
import hashlib
import io
import json
import os
import struct
import tarfile
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest import mock

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

    def test_build_refuses_links_without_writing_an_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "secret.json").write_bytes(b"credential\n")
            (root / "outside").mkdir()
            out = root / "evidence.tar.gz"
            for link, target in (("gcs/captures/gcs-01a.json", root / "secret.json"),
                                 ("gcs/receipts", root / "outside"), ("gcs/index.json", root / "secret.json")):
                with self.subTest(link=link):
                    evidence_dir = root / link.replace("/", "-") / "evidence"
                    (evidence_dir / "gcs/captures").mkdir(parents=True)
                    if link != "gcs/index.json":
                        (evidence_dir / "gcs/index.json").write_bytes(b"{}\n")
                    (evidence_dir / link).symlink_to(target)
                    with self.assertRaisesRegex(ValueError, "only regular files"):
                        evidence_archive.build(evidence_dir, ["gcs"], out, {})
                    self.assertFalse(out.exists())

    def test_check_compares_the_tar_when_zlib_ng_deflates_differently(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "gcs").mkdir()
            (root / "gcs/index.json").write_bytes(b"{}\n" * 1000)
            packed = evidence_archive.pack(root)
            tar = gzip.decompress(packed)
            # The same tar and gzip header with another valid deflate stream, as zlib-ng writes.
            deflate = zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_HUFFMAN_ONLY)
            other = packed[:10] + deflate.compress(tar) + deflate.flush() + struct.pack(
                "<II", zlib.crc32(tar), len(tar))
            self.assertNotEqual(other, packed)
            archive = root / "evidence.tar.gz"
            archive.write_bytes(other)
            with mock.patch.object(zlib, "ZLIB_RUNTIME_VERSION", "1.3.1"):
                self.assertFalse(evidence_archive.rebuilds_identically(archive))
            with mock.patch.object(zlib, "ZLIB_RUNTIME_VERSION", "1.3.1.zlib-ng"):
                self.assertTrue(evidence_archive.rebuilds_identically(archive))
                archive.write_bytes(evidence.deterministic_gzip(tar[:-1] + b"x"))
                self.assertFalse(evidence_archive.rebuilds_identically(archive))
                with self.assertRaisesRegex(ValueError, "classic zlib"):
                    evidence_archive.build(root, ["gcs"], root / "built.tar.gz", {})
                self.assertFalse((root / "built.tar.gz").exists())

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


class VersionFoldTest(unittest.TestCase):
    """Folding Azure service versions and the validator's checks of an equivalence citation."""

    REFERENCE, OTHER = "2026-06-06", "2026-10-06"

    def manifest(self, version):
        return {"provider": "azure", "namespace_mode": "flat-hns-off", "region": "test-region",
                "api_version": version, "sdk_product": "azsdk-java-azure-storage-blob", "sdk_version": "12.35.1",
                "capture_date": "2026-10-21", "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}

    def capture(self, case_id, version, body=b"<EnumerationResults/>", request_id="1", status=200):
        digest = hashlib.sha256(evidence.canonical(self.manifest(version))).hexdigest()
        # A raw error message carries the request ID; a raw listing body does not.
        raw_body = body + request_id.encode() if status >= 400 else body
        return {"schema_version": "provider-capture-v1", "provider": "azure", "api_version": version,
                "probe_id": case_id, "manifest_sha256": digest, "captured_at": f"{version}T00:00:00Z",
                "raw_body_sha256": hashlib.sha256(raw_body).hexdigest(),
                "request": {"method": "GET", "path": "/{container}", "query": "restype=container&comp=list",
                            "headers": [{"name": "x-ms-version", "value": version}], "token_wire_sha256": []},
                "response": {"status": status, "headers": [
                    {"name": "date", "value": f"{request_id} GMT"}, {"name": "x-ms-version", "value": version},
                    {"name": "x-ms-request-id", "value": "request-id-sha256-" + request_id * 64}],
                    "body_base64": base64.b64encode(body).decode("ascii")}}

    def provider_dir(self, root, captures):
        provider = root / "azure"
        (provider / "captures").mkdir(parents=True)
        index = {"provider": "azure", "manifests": {}, "captures": {}}
        for version in (self.REFERENCE, self.OTHER):
            payload = evidence.canonical(self.manifest(version))
            (provider / f"manifest.{version}.json").write_bytes(payload)
            index["manifests"][version] = {"file": f"manifest.{version}.json",
                                           "sha256": hashlib.sha256(payload).hexdigest()}
        for key, capture in captures.items():
            payload = evidence.canonical(capture)
            (provider / f"captures/{key}.json").write_bytes(payload)
            index["captures"][key] = {"file": f"captures/{key}.json", "kind": "exchange",
                                      "sha256": hashlib.sha256(payload).hexdigest()}
        (provider / "index.json").write_text(json.dumps(index))
        (provider / "replay-expectations.json").write_text(json.dumps({key: "match" for key in captures}))
        return provider

    def ledger(self, provider_dir, case_id):
        cited = f"captures/{case_id}~{self.REFERENCE}.json"
        proof = {"capture_file": f"azure/{cited}", "probe_request": "GET /{container}?restype=container&comp=list",
                 "capture_sha256": hashlib.sha256((provider_dir / cited).read_bytes()).hexdigest(),
                 "expected_result": "one page"}
        return {"rows": [{"id": case_id, "provider": "azure", "evidence_kind": "exchange", "status": "OBSERVED",
                          "api_versions": [self.REFERENCE, self.OTHER],
                          "evidence": {self.REFERENCE: proof, self.OTHER: {
                              **proof, "version_equivalence_file": "azure/version-equivalence.json"}}}]}

    def test_fold_keeps_only_captures_that_differ_beyond_volatile_values(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            error = b"<Error><Code>InvalidInput</Code></Error>"
            provider = self.provider_dir(root, {
                f"azure-03a~{self.REFERENCE}": self.capture("azure-03a", self.REFERENCE),
                f"azure-03a~{self.OTHER}": self.capture("azure-03a", self.OTHER, request_id="2"),
                f"azure-08a~{self.REFERENCE}": self.capture("azure-08a", self.REFERENCE, error, status=400),
                f"azure-08a~{self.OTHER}": self.capture("azure-08a", self.OTHER, error, "2", status=400),
                f"azure-03b~{self.REFERENCE}": self.capture("azure-03b", self.REFERENCE),
                f"azure-03b~{self.OTHER}": self.capture("azure-03b", self.OTHER, b"<EnumerationResults />")})
            record = evidence_archive.fold_versions(provider, self.REFERENCE)
            self.assertEqual({key: case["equivalent"] for key, case in record["cases"].items()},
                             {f"azure-03a~{self.OTHER}": True, f"azure-03b~{self.OTHER}": False,
                              f"azure-08a~{self.OTHER}": True})
            index = json.loads((provider / "index.json").read_text())
            expectations = json.loads((provider / "replay-expectations.json").read_text())
            for kept in (f"azure-03b~{self.OTHER}", f"azure-03b~{self.REFERENCE}", f"azure-03a~{self.REFERENCE}"):
                self.assertIn(kept, index["captures"])
                self.assertIn(kept, expectations)
                self.assertTrue((provider / f"captures/{kept}.json").is_file())
            self.assertNotIn(f"azure-03a~{self.OTHER}", index["captures"])
            self.assertNotIn(f"azure-03a~{self.OTHER}", expectations)
            self.assertFalse((provider / f"captures/azure-03a~{self.OTHER}.json").exists())
            self.assertEqual(record["cases"][f"azure-03b~{self.OTHER}"]["version"]["file"],
                             f"captures/azure-03b~{self.OTHER}.json")
            self.assertEqual(index["manifests"][self.OTHER]["derived_from"], self.REFERENCE)
            self.assertFalse((provider / f"manifest.{self.OTHER}.json").exists())
            with self.assertRaisesRegex(ValueError, "already folded"):
                evidence_archive.fold_versions(provider, self.REFERENCE)
            manifests = evidence.load_manifests(root)
            self.assertEqual(manifests[("azure", self.OTHER)], self.manifest(self.OTHER))
            evidence.validate_ledger(self.ledger(provider, "azure-03a"), manifests, root)

    def test_validator_refuses_unproven_equivalence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            provider = self.provider_dir(root, {
                f"azure-03a~{self.REFERENCE}": self.capture("azure-03a", self.REFERENCE),
                f"azure-03a~{self.OTHER}": self.capture("azure-03a", self.OTHER, request_id="2"),
                f"azure-03b~{self.REFERENCE}": self.capture("azure-03b", self.REFERENCE),
                f"azure-03b~{self.OTHER}": self.capture("azure-03b", self.OTHER, b"<EnumerationResults />")})
            evidence_archive.fold_versions(provider, self.REFERENCE)
            manifests = evidence.load_manifests(root)
            record_path = provider / "version-equivalence.json"
            record = json.loads(record_path.read_text())
            with self.assertRaisesRegex(ValueError, "does not cover"):
                evidence.validate_ledger(self.ledger(provider, "azure-03b"), manifests, root)
            for edit, message in (
                    (lambda r: r["cases"][f"azure-03a~{self.OTHER}"]["version"].update(normalized_sha256="0" * 64),
                     "digests differ"),
                    (lambda r: r["cases"][f"azure-03a~{self.OTHER}"]["reference"].update(
                        file=f"captures/azure-03b~{self.REFERENCE}.json"), "another capture"),
                    (lambda r: r.update(reference_version=self.OTHER), "not for this provider"),
                    (lambda r: r["cases"][f"azure-03a~{self.OTHER}"]["version"].pop("sha256"), "own digests"),
                    (lambda r: r["cases"][f"azure-03a~{self.OTHER}"]["version"].update(
                        sha256=r["cases"][f"azure-03a~{self.OTHER}"]["reference"]["sha256"]), "own digests"),
                    (lambda r: r["cases"].pop(f"azure-03a~{self.OTHER}"), "does not cover")):
                tampered = copy.deepcopy(record)
                edit(tampered)
                record_path.write_text(json.dumps(tampered))
                with self.assertRaisesRegex(ValueError, message):
                    evidence.validate_ledger(self.ledger(provider, "azure-03a"), manifests, root)
            record_path.write_text(json.dumps(record))
            replayed = self.ledger(provider, "azure-03a")
            replayed["rows"][0]["evidence"][self.OTHER]["replay_test"] = "azureNativeCapturesMatchReplay"
            with self.assertRaisesRegex(ValueError, "cannot carry a replay match"):
                evidence.validate_ledger(replayed, manifests, root)
            changed = {**manifests, ("azure", self.OTHER): dict(self.manifest(self.OTHER), region="other")}
            with self.assertRaisesRegex(ValueError, "same corpus manifest"):
                evidence.validate_ledger(self.ledger(provider, "azure-03a"), changed, root)
            # A changed reference capture no longer matches the digest the record checked.
            cited = provider / f"captures/azure-03a~{self.REFERENCE}.json"
            capture = json.loads(cited.read_text())
            capture["response"]["body_base64"] = base64.b64encode(b"<EnumerationResults />").decode("ascii")
            cited.write_bytes(evidence.canonical(capture))
            with self.assertRaisesRegex(ValueError, "another capture"):
                evidence.validate_ledger(self.ledger(provider, "azure-03a"), manifests, root)


if __name__ == "__main__":
    unittest.main()
