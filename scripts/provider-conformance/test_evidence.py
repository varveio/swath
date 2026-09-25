#!/usr/bin/env python3
"""Offline sanitizer and ledger checks; no provider resources are used."""

import base64
import importlib.util
import pathlib
import hashlib
import json
import tempfile
import unittest

MODULE = pathlib.Path(__file__).with_name("evidence.py")
SPEC = importlib.util.spec_from_file_location("provider_evidence", MODULE)
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)


class EvidenceTest(unittest.TestCase):
    def exchange(self):
        body = b'{"kind":"storage#objects","items":[{"bucket":"private-bucket","name":"a"}]}'
        return {
            "captured_at": "2026-09-25T10:00:00Z",
            "request": {"method": "GET", "url": "https://private.example/storage/v1/b/private-bucket/o?prefix=a",
                        "headers": [{"name": "Authorization", "value": "Bearer secret-token"},
                                    {"name": "Accept", "value": "application/json"}]},
            "response": {"status": 200,
                         "headers": [{"name": "x-goog-request-id", "value": "private-id"},
                                     {"name": "Content-Type", "value": "application/json"}],
                         "body_base64": base64.b64encode(body).decode("ascii")},
        }

    def test_sanitizer_removes_origin_credentials_ids_and_namespace(self):
        safe = evidence.sanitize_exchange(self.exchange(), "gcs", {"bucket": "private-bucket"},
                                          "a" * 64, "gcs-basic")
        serialized = evidence.canonical(safe)
        for secret in (b"private.example", b"private-bucket", b"Bearer", b"secret-token", b"private-id"):
            self.assertNotIn(secret, serialized)
        self.assertEqual(safe["request"]["path"], "/storage/v1/b/{bucket}/o")
        self.assertIn(b"{bucket}", base64.b64decode(safe["response"]["body_base64"]))

    def test_sanitizer_refuses_unrecognized_query_that_might_contain_secret(self):
        capture = self.exchange()
        capture["request"]["url"] += "&sig=private-signature"
        with self.assertRaisesRegex(ValueError, "potential secret"):
            evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                       "a" * 64, "gcs-basic")

    def test_sanitizer_refuses_identifier_in_object_name_without_mutating_it(self):
        capture = self.exchange()
        capture["response"]["body_base64"] = base64.b64encode(
            b'{"items":[{"bucket":"private-bucket","name":"private-bucket/1"}]}').decode("ascii")
        with self.assertRaisesRegex(ValueError, "private identifier remained"):
            evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                       "a" * 64, "gcs-basic")

    def test_azure_sanitizer_keeps_name_bytes_and_redacts_only_namespace(self):
        xml = (b'<?xml version="1.0"?><EnumerationResults ServiceEndpoint="https://secretacct.blob.core.windows.net/" '
               b'ContainerName="secretcontainer"><Blobs><Blob><Name>my-data/1</Name></Blob></Blobs>'
               b'<NextMarker>native-opaque</NextMarker></EnumerationResults>')
        capture = {"captured_at": "2026-09-25T10:00:00Z",
                   "request": {"method": "GET",
                               "url": "https://secretacct.blob.core.windows.net/secretacct/secretcontainer"
                                      "?restype=container&comp=list&marker=native-opaque",
                               "headers": [{"name": "Authorization", "value": "secret-key"}]},
                   "response": {"status": 200, "headers": [],
                                "body_base64": base64.b64encode(xml).decode("ascii")}}
        safe = evidence.sanitize_exchange(capture, "azure", {"account": "secretacct",
                                                          "container": "secretcontainer"},
                                          "b" * 64, "azure-basic")
        decoded = base64.b64decode(safe["response"]["body_base64"])
        self.assertIn(b"<Name>my-data/1</Name>", decoded)
        self.assertNotIn(b"secretacct", decoded)
        self.assertNotIn(b"native-opaque", decoded)
        digest = evidence.opaque_digest("native-opaque")
        self.assertEqual(safe["request"]["query"], "restype=container&comp=list&marker=" + digest)
        self.assertIn(digest.encode("ascii"), decoded)

    def test_manifest_and_ledger_cannot_forge_observed_state(self):
        manifest = {"provider": "gcs", "namespace_mode": "flat-ubla", "region": "test-region",
                    "api_version": "json-v1", "sdk_version": "2.73.0", "capture_date": "2026-09-25",
                    "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}
        evidence.validate_manifest(manifest)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            evidence.validate_manifest({**manifest, "objects": manifest["objects"] * 2})
        evidence.validate_ledger({"rows": [{"id": "gcs-basic", "provider": "gcs",
                                            "status": "UNMEASURED"}]})
        with self.assertRaisesRegex(ValueError, "lacks"):
            evidence.validate_ledger({"rows": [{"id": "gcs-basic", "provider": "gcs",
                                                "status": "OBSERVED"}]}, {"gcs": manifest})
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            safe = evidence.sanitize_exchange(self.exchange(), "gcs", {"bucket": "private-bucket"},
                                              hashlib.sha256(evidence.canonical(manifest)).hexdigest(),
                                              "gcs-basic")
            payload = evidence.canonical(safe)
            (root / "capture.json").write_bytes(payload)
            observed = {"id": "gcs-basic", "provider": "gcs", "status": "OBSERVED",
                        "probe_request": "GET /storage/v1/b/{bucket}/o?prefix=a",
                        "expected_result": "one object", "capture_file": "capture.json",
                        "capture_sha256": hashlib.sha256(payload).hexdigest()}
            evidence.validate_ledger({"rows": [observed]}, {"gcs": manifest}, root)
            with self.assertRaisesRegex(ValueError, "SHA-256 differs"):
                evidence.validate_ledger({"rows": [{**observed, "capture_sha256": "f" * 64}]},
                                         {"gcs": manifest}, root)
            with self.assertRaisesRegex(ValueError, "verifiable test"):
                evidence.validate_ledger({"rows": [{**observed, "status": "REPLAY_MATCH",
                                                     "replay_test": "anything"}]}, {"gcs": manifest}, root)
            (root / "EvidenceTest.java").write_text("class EvidenceTest { void exactTest() {} }", encoding="utf-8")
            result = {"status": "PASS", "capture_sha256": observed["capture_sha256"],
                      "test": "exactTest"}
            result_bytes = evidence.canonical(result)
            (root / "result.json").write_bytes(result_bytes)
            matched = {**observed, "status": "REPLAY_MATCH", "replay_test": "exactTest",
                       "replay_test_path": "EvidenceTest.java", "replay_result_file": "result.json",
                       "replay_result_sha256": hashlib.sha256(result_bytes).hexdigest()}
            evidence.validate_ledger({"rows": [matched]}, {"gcs": manifest}, root, root)
            with self.assertRaisesRegex(ValueError, "does not resolve"):
                evidence.validate_ledger({"rows": [{**matched, "replay_test": "inventedTest"}]},
                                         {"gcs": manifest}, root, root)

    def test_checked_in_matrix_matches_machine_ledger(self):
        repo = pathlib.Path(__file__).resolve().parents[2]
        ledger = evidence.strict_json((repo / "docs/replay-provider-ledger.json").read_text())
        matrix = (repo / "docs/replay-provider-conformance.md").read_text()
        evidence.validate_ledger(ledger)
        evidence.validate_matrix(matrix, ledger)
        with self.assertRaisesRegex(ValueError, "matrix differs"):
            evidence.validate_matrix(matrix.replace("gcs-01 |", "gcs-01 |", 1)
                                     .replace("Provisional bounded list", "Unsupported", 1), ledger)


if __name__ == "__main__":
    unittest.main()
