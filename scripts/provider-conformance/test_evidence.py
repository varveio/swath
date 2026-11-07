#!/usr/bin/env python3
"""Offline sanitizer and ledger checks; no provider resources are used."""

import base64
import importlib.util
import io
import os
import pathlib
import hashlib
import json
import tarfile
import tempfile
import subprocess
import unittest
from unittest import mock

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

    def test_namespace_placeholder_does_not_look_like_a_leak_when_bucket_is_bucket(self):
        capture = self.exchange()
        capture["request"]["url"] = capture["request"]["url"].replace("private-bucket", "bucket")
        body = base64.b64decode(capture["response"]["body_base64"]).replace(
            b"private-bucket", b"bucket")
        capture["response"]["body_base64"] = base64.b64encode(body).decode("ascii")
        safe = evidence.sanitize_exchange(capture, "gcs", {"bucket": "bucket"},
                                          "a" * 64, "gcs-basic")
        self.assertEqual(safe["request"]["path"], "/storage/v1/b/{bucket}/o")

    def test_sanitizer_refuses_unrecognized_query_that_might_contain_secret(self):
        capture = self.exchange()
        capture["request"]["url"] += "&sig=private-signature"
        with self.assertRaisesRegex(ValueError, "potential secret"):
            evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                       "a" * 64, "gcs-basic")

    def test_sdk_product_versions_survive_without_host_or_platform_identifiers(self):
        capture = self.exchange()
        capture["request"]["headers"].extend([
            {"name": "User-Agent", "value": "gcloud-java/2.73.0 (Linux; private-host)"},
            {"name": "x-goog-api-client", "value": "gl-java/25 gdcl/2.7.2 private-project"},
        ])
        safe = evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                          "a" * 64, "gcs-sdk")
        headers = {item["name"]: item["value"] for item in safe["request"]["headers"]}
        self.assertEqual(headers["user-agent"], "gcloud-java/2.73.0")
        self.assertEqual(headers["x-goog-api-client"], "gl-java/25 gdcl/2.7.2")
        self.assertNotIn("private-host", evidence.canonical(safe).decode("utf-8"))
        capture["request"]["headers"][2]["value"] = "my-gccl/2.73.0 (Linux; private-host)"
        capture["request"]["headers"][3]["value"] = "my-gccl/2.73.0 private-project"
        with self.assertRaisesRegex(ValueError, "classified product"):
            evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                       "a" * 64, "gcs-sdk")

    def test_sanitizer_reclassifies_response_headers_and_checks_body_length(self):
        capture = self.exchange()
        capture["response"]["headers"].append({"name": "Content-Length", "value": "999"})
        safe = evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                          "a" * 64, "gcs-basic")
        headers = {item["name"]: item["value"] for item in safe["response"]["headers"]}
        self.assertEqual(headers["content-length"],
                         str(len(base64.b64decode(safe["response"]["body_base64"]))))
        self.assertTrue(headers["x-goog-request-id"].startswith("request-id-sha256-"))
        capture["response"]["headers"].append({"name": "x-future-secret", "value": "private"})
        with self.assertRaisesRegex(ValueError, "unclassified response header"):
            evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                       "a" * 64, "gcs-basic")

    def test_sanitized_gcs_error_keeps_typed_message_for_comparator(self):
        capture = self.exchange()
        capture["response"]["status"] = 400
        capture["response"]["body_base64"] = base64.b64encode(
            b'{"error":{"code":400,"message":"Bucket private-bucket failed",'
            b'"errors":[{"reason":"invalid","message":"private-bucket invalid"}]}}').decode("ascii")
        safe = evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                          "a" * 64, "gcs-error")
        decoded = json.loads(base64.b64decode(safe["response"]["body_base64"]))
        self.assertEqual(decoded["error"]["message"], "{redacted-provider-message}")
        self.assertEqual(decoded["error"]["errors"][0]["reason"], "invalid")

    def test_sanitized_azure_error_keeps_named_details_without_request_id_leak(self):
        xml = (b'<?xml version="1.0"?><Error><Code>InvalidQueryParameterValue</Code>'
               b'<Message>bad RequestId: private-id Time: now</Message>'
               b'<QueryParameterName>maxresults</QueryParameterName>'
               b'<QueryParameterValue>secretvalue</QueryParameterValue>'
               b'<RequestId>private-id</RequestId><Time>now</Time></Error>')
        capture = {"captured_at": "2026-09-25T10:00:00Z",
                   "request": {"method": "GET",
                               "url": "https://secretacct.blob.core.windows.net/secretcontainer"
                                      "?restype=container&comp=list&maxresults=0",
                               "headers": [{"name": "x-ms-version", "value": "2026-06-06"}]},
                   "response": {"status": 400, "headers": [
                       {"name": "x-ms-version", "value": "2026-06-06"},
                       {"name": "x-ms-request-id", "value": "private-id"}],
                       "body_base64": base64.b64encode(xml).decode("ascii")}}
        safe = evidence.sanitize_exchange(capture, "azure", {"account": "secretacct",
                                                          "container": "secretcontainer"},
                                          "b" * 64, "azure-error", "2026-06-06")
        decoded = base64.b64decode(safe["response"]["body_base64"])
        self.assertTrue(decoded.startswith(b'<?xml version="1.0"?>'))
        self.assertIn(b"<Code>InvalidQueryParameterValue</Code>", decoded)
        self.assertIn(b"<QueryParameterName>maxresults</QueryParameterName>", decoded)
        self.assertNotIn(b"private-id", decoded)
        self.assertNotIn(b"secretvalue", decoded)
        self.assertIn(b"request-id-sha256-", decoded)

    def test_azure_error_cdata_private_url_is_structurally_redacted(self):
        xml = (b'<?xml version="1.0"?><Error><Code>InvalidQueryParameterValue</Code>'
               b'<Message><![CDATA[https://privateacct.blob.core.windows.net/privatecontainer?sig=secret]]>'
               b'</Message><QueryParameterName>maxresults</QueryParameterName>'
               b'<RequestId>private-id</RequestId></Error>')
        capture = {"captured_at": "2026-09-25T10:00:00Z",
                   "request": {"method": "GET",
                               "url": "https://privateacct.blob.core.windows.net/privatecontainer"
                                      "?restype=container&comp=list&maxresults=0",
                               "headers": [{"name": "x-ms-version", "value": "2026-06-06"}]},
                   "response": {"status": 400, "headers": [
                       {"name": "x-ms-version", "value": "2026-06-06"}],
                       "body_base64": base64.b64encode(xml).decode("ascii")}}
        safe = evidence.sanitize_exchange(capture, "azure", {"account": "privateacct",
                                                          "container": "privatecontainer"},
                                          "b" * 64, "azure-08", "2026-06-06")
        body = base64.b64decode(safe["response"]["body_base64"])
        self.assertIn(b"<Code>InvalidQueryParameterValue</Code>", body)
        self.assertNotIn(b"privateacct", body)
        self.assertNotIn(b"privatecontainer", body)
        self.assertNotIn(b"secret", body)

    def test_azure_error_and_success_keep_native_bom_and_preamble(self):
        preamble = '\ufeff<?xml version="1.0" encoding="utf-8"?>\n'
        error = preamble + '<Error><Code>ServerBusy</Code><Message>private</Message></Error>'
        success = preamble + ('<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/" '
                              'ContainerName="container"><Blobs/><NextMarker/></EnumerationResults>')
        self.assertTrue(evidence.sanitize_azure_body(error, "acct", "container").startswith(preamble))
        self.assertTrue(evidence.sanitize_azure_body(success, "acct", "container").startswith(preamble))

    def test_azure_head_error_capture_is_empty_and_keeps_typed_headers(self):
        capture = {"captured_at": "2026-09-25T10:00:00Z",
                   "request": {"method": "HEAD",
                               "url": "https://acct.blob.core.windows.net/container"
                                      "?restype=container&comp=list&maxresults=0",
                               "headers": [{"name": "x-ms-version", "value": "2026-06-06"}]},
                   "response": {"status": 400,
                                "headers": [{"name": "x-ms-version", "value": "2026-06-06"},
                                            {"name": "x-ms-error-code",
                                             "value": "OutOfRangeQueryParameterValue"},
                                            {"name": "Content-Length", "value": "150"}],
                                "body_base64": ""}}
        safe = evidence.sanitize_exchange(capture, "azure", {"account": "acct",
                                                          "container": "container"},
                                          "b" * 64, "azure-08", "2026-06-06")
        self.assertEqual(safe["response"]["body_base64"], "")
        self.assertIn({"name": "content-length", "value": "150"}, safe["response"]["headers"])
        for invalid_headers in (
            [header for header in capture["response"]["headers"]
             if header["name"] != "x-ms-error-code"],
            [{**header, "value": ""} if header["name"] == "x-ms-error-code" else header
             for header in capture["response"]["headers"]],
            capture["response"]["headers"] + [{"name": "x-ms-error-code", "value": "InternalError"}],
        ):
            malformed = json.loads(json.dumps(capture))
            malformed["response"]["headers"] = invalid_headers
            with self.assertRaisesRegex(ValueError, "x-ms-error-code"):
                evidence.sanitize_exchange(malformed, "azure", {"account": "acct",
                                                           "container": "container"},
                                           "b" * 64, "azure-08", "2026-06-06")
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            manifest = {"provider": "azure", "namespace_mode": "flat-hns-off", "region": "test",
                        "api_version": "2026-06-06", "sdk_product": "azsdk-java-azure-storage-blob",
                        "sdk_version": "12.35.1", "capture_date": "2026-09-25",
                        "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}
            forged = json.loads(json.dumps(safe))
            forged["probe_id"] = "azure-03"
            forged["manifest_sha256"] = hashlib.sha256(evidence.canonical(manifest)).hexdigest()
            forged["response"]["headers"] = [header for header in forged["response"]["headers"]
                                              if header["name"] != "x-ms-error-code"]
            payload = evidence.canonical(forged)
            (root / "forged.json").write_bytes(payload)
            row = {"id": "azure-03", "provider": "azure", "evidence_kind": "exchange",
                   "api_versions": ["2026-06-06"], "status": "OBSERVED", "evidence": {
                       "2026-06-06": {"probe_request":
                           "HEAD /{container}?restype=container&comp=list&maxresults=0",
                           "expected_result": "typed HEAD error", "capture_file": "forged.json",
                           "capture_sha256": hashlib.sha256(payload).hexdigest()}}}
            with self.assertRaisesRegex(ValueError, "x-ms-error-code"):
                evidence.validate_ledger({"rows": [row]}, {("azure", "2026-06-06"): manifest}, root)
        capture["response"]["body_base64"] = base64.b64encode(b"<Error/>").decode("ascii")
        with self.assertRaisesRegex(ValueError, "empty body"):
            evidence.sanitize_exchange(capture, "azure", {"account": "acct",
                                                       "container": "container"},
                                       "b" * 64, "azure-08", "2026-06-06")
        capture["response"]["body_base64"] = ""
        capture["response"]["status"] = 204
        with self.assertRaisesRegex(ValueError, "outside the captured error profile"):
            evidence.sanitize_exchange(capture, "azure", {"account": "acct",
                                                       "container": "container"},
                                       "b" * 64, "azure-08", "2026-06-06")

    def test_azure_encoded_name_leak_is_decoded_before_sanitizer_accepts(self):
        account = "privateaccount"
        encoded = "".join(f"%{byte:02X}" for byte in account.encode("utf-8"))
        xml = (f'<EnumerationResults ServiceEndpoint="https://{account}.blob.core.windows.net/" '
               f'ContainerName="container"><Blobs><Blob><Name Encoded="true">{encoded}</Name>'
               f'</Blob></Blobs><NextMarker/></EnumerationResults>').encode("utf-8")
        capture = {"captured_at": "2026-09-25T10:00:00Z",
                   "request": {"method": "GET",
                               "url": f"https://{account}.blob.core.windows.net/container?restype=container&comp=list",
                               "headers": [{"name": "x-ms-version", "value": "2026-06-06"}]},
                   "response": {"status": 200, "headers": [{"name": "x-ms-version",
                                                         "value": "2026-06-06"}],
                                "body_base64": base64.b64encode(xml).decode("ascii")}}
        with self.assertRaisesRegex(ValueError, "private identifier remained"):
            evidence.sanitize_exchange(capture, "azure", {"account": account,
                                                        "container": "container"},
                                       "b" * 64, "azure-encoded", "2026-06-06")

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
                               "url": "https://secretacct.blob.core.windows.net/secretcontainer"
                                      "?restype=container&comp=list&marker=native-opaque",
                               "headers": [{"name": "Authorization", "value": "secret-key"},
                                           {"name": "x-ms-version", "value": "2026-06-06"},
                                           {"name": "x-ms-client-request-id", "value": "private-request-id"}]},
                   "response": {"status": 200, "headers": [
                       {"name": "x-ms-version", "value": "2026-06-06"},
                       {"name": "x-ms-client-request-id", "value": "private-request-id"}],
                                "body_base64": base64.b64encode(xml).decode("ascii")}}
        safe = evidence.sanitize_exchange(capture, "azure", {"account": "secretacct",
                                                          "container": "secretcontainer"},
                                          "b" * 64, "azure-basic", "2026-06-06")
        decoded = base64.b64decode(safe["response"]["body_base64"])
        self.assertIn(b"<Name>my-data/1</Name>", decoded)
        self.assertNotIn(b"secretacct", decoded)
        self.assertNotIn(b"native-opaque", decoded)
        digest = evidence.opaque_digest("native-opaque")
        self.assertEqual(safe["request"]["query"], "restype=container&comp=list&marker=" + digest)
        self.assertIn(digest.encode("ascii"), decoded)
        self.assertEqual(safe["request"]["path"], "/{container}")
        self.assertIn(b"https://{account}.blob.core.windows.net/", decoded)
        request_id = evidence.opaque_digest("private-request-id")
        self.assertIn(request_id, [item["value"] for item in safe["request"]["headers"]])
        self.assertIn(request_id, [item["value"] for item in safe["response"]["headers"]])
        azure_sdk = json.loads(json.dumps(capture))
        azure_sdk["request"]["headers"].append({"name": "User-Agent",
            "value": "azsdk-java-azure-storage-blob/12.35.1 (Linux; private-host)"})
        sdk_safe = evidence.sanitize_exchange(azure_sdk, "azure", {"account": "secretacct",
                                                        "container": "secretcontainer"},
                                              "b" * 64, "azure-sdk", "2026-06-06")
        self.assertIn("azsdk-java-azure-storage-blob/12.35.1",
                      [item["value"] for item in sdk_safe["request"]["headers"]])
        bad_echo = json.loads(json.dumps(capture))
        bad_echo["response"]["headers"][1]["value"] = "different-id"
        with self.assertRaisesRegex(ValueError, "echo differs"):
            evidence.sanitize_exchange(bad_echo, "azure", {"account": "secretacct",
                                                        "container": "secretcontainer"},
                                       "b" * 64, "azure-basic", "2026-06-06")
        wrong_version = json.loads(json.dumps(capture))
        wrong_version["response"]["headers"][0]["value"] = "2026-10-06"
        with self.assertRaisesRegex(ValueError, "version differs"):
            evidence.sanitize_exchange(wrong_version, "azure", {"account": "secretacct",
                                                            "container": "secretcontainer"},
                                       "b" * 64, "azure-basic", "2026-06-06")

    def test_manifest_and_ledger_cannot_forge_observed_state(self):
        manifest = {"provider": "gcs", "namespace_mode": "flat-ubla", "region": "test-region",
                    "api_version": "json-v1", "sdk_product": "gcloud-java",
                    "sdk_version": "2.73.0", "capture_date": "2026-09-25",
                    "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}
        evidence.validate_manifest(manifest)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            evidence.validate_manifest({**manifest, "objects": manifest["objects"] * 2})
        evidence.validate_ledger({"rows": [{"id": "gcs-basic", "provider": "gcs",
                                            "evidence_kind": "exchange",
                                            "api_versions": ["json-v1"], "status": "UNMEASURED"}]})
        with self.assertRaisesRegex(ValueError, "required probe kind"):
            evidence.validate_ledger({"rows": [{"id": "gcs-07", "provider": "gcs",
                "evidence_kind": "exchange", "api_versions": ["json-v1"],
                "status": "UNMEASURED"}]})
        with self.assertRaisesRegex(ValueError, "split into case-specific rows"):
            evidence.validate_ledger({"rows": [{"id": "gcs-01", "provider": "gcs",
                "evidence_kind": "exchange", "api_versions": ["json-v1"],
                "status": "OBSERVED"}]})
        with self.assertRaisesRegex(ValueError, "lacks"):
            evidence.validate_ledger({"rows": [{"id": "gcs-basic", "provider": "gcs",
                                                "evidence_kind": "exchange",
                                                "api_versions": ["json-v1"], "status": "OBSERVED",
                                                "evidence": {"json-v1": {}}}]},
                                     {("gcs", "json-v1"): manifest})
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            safe = evidence.sanitize_exchange(self.exchange(), "gcs", {"bucket": "private-bucket"},
                                              hashlib.sha256(evidence.canonical(manifest)).hexdigest(),
                                              "gcs-basic")
            payload = evidence.canonical(safe)
            (root / "capture.json").write_bytes(payload)
            proof = {"probe_request": "GET /storage/v1/b/{bucket}/o?prefix=a",
                     "expected_result": "one object", "capture_file": "capture.json",
                     "capture_sha256": hashlib.sha256(payload).hexdigest()}
            observed = {"id": "gcs-basic", "provider": "gcs", "evidence_kind": "exchange",
                        "api_versions": ["json-v1"],
                        "status": "OBSERVED", "evidence": {"json-v1": proof}}
            evidence.validate_ledger({"rows": [observed]}, {("gcs", "json-v1"): manifest}, root)
            with self.assertRaisesRegex(ValueError, "SHA-256 differs"):
                evidence.validate_ledger({"rows": [{**observed,
                    "evidence": {"json-v1": {**proof, "capture_sha256": "f" * 64}}}]},
                    {("gcs", "json-v1"): manifest}, root)
            with self.assertRaisesRegex(ValueError, "verifiable test"):
                evidence.validate_ledger({"rows": [{**observed, "status": "REPLAY_MATCH",
                    "evidence": {"json-v1": {**proof, "replay_test": "anything"}}}]},
                    {("gcs", "json-v1"): manifest}, root)
            (root / "EvidenceTest.java").write_text("class EvidenceTest { @Test void exactTest() {} }",
                                                     encoding="utf-8")
            (root / "replay.jar").write_bytes(b"replay-binary")
            for arguments in (("init",), ("config", "user.name", "Replay Test"),
                              ("config", "user.email", "replay-test@example.invalid"),
                              ("add", "-A"), ("commit", "-m", "fixture")):
                subprocess.run(("git", "-C", str(root), *arguments), check=True,
                               capture_output=True)
            commit = subprocess.run(("git", "-C", str(root), "rev-parse", "HEAD"),
                                    check=True, capture_output=True, text=True).stdout.strip()
            result = {"status": "PASS", "capture_sha256": proof["capture_sha256"],
                      "test": "exactTest", "api_version": "json-v1",
                      "test_path": "EvidenceTest.java",
                      "test_source_sha256": hashlib.sha256((root / "EvidenceTest.java").read_bytes()).hexdigest(),
                      "schema_version": "provider-match-receipt-v2",
                      "comparison": "ProviderExchangeComparator", "replay_commit": commit,
                      "evidence_kind": "exchange", "compared_steps": 1,
                      "distribution_sha256": hashlib.sha256(b"replay-binary").hexdigest(),
                      "distribution_name": "replay.jar"}
            result_bytes = evidence.canonical(result)
            (root / "result.json").write_bytes(result_bytes)
            matched_proof = {**proof, "replay_test": "exactTest",
                             "replay_test_path": "EvidenceTest.java", "replay_result_file": "result.json",
                             "replay_result_sha256": hashlib.sha256(result_bytes).hexdigest()}
            matched = {**observed, "status": "REPLAY_MATCH", "evidence": {"json-v1": matched_proof}}
            evidence.validate_ledger({"rows": [matched]}, {("gcs", "json-v1"): manifest},
                                     root, root, commit, root / "replay.jar")
            (root / "EvidenceTest.java").write_text("class EvidenceTest { @Test void exactTest() { } }",
                                                     encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "receipt does not prove"):
                evidence.validate_ledger({"rows": [matched]}, {("gcs", "json-v1"): manifest},
                                         root, root, commit, root / "replay.jar")
            (root / "EvidenceTest.java").write_text("class EvidenceTest { @Test void exactTest() {} }",
                                                     encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not resolve"):
                evidence.validate_ledger({"rows": [{**matched,
                    "evidence": {"json-v1": {**matched_proof, "replay_test": "inventedTest"}}}]},
                    {("gcs", "json-v1"): manifest}, root, root, commit, root / "replay.jar")
            with self.assertRaisesRegex(ValueError, "ancestor"):
                evidence.validate_ledger({"rows": [matched]}, {("gcs", "json-v1"): manifest},
                                         root, root, "b" * 40, root / "replay.jar")

    def test_walk_row_refuses_single_exchange_and_run_requires_continuation(self):
        manifest = {"provider": "gcs", "namespace_mode": "flat-ubla", "region": "test",
                    "api_version": "json-v1", "sdk_product": "gcloud-java",
                    "sdk_version": "2.73.0", "capture_date": "2026-09-25",
                    "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            digest = hashlib.sha256(evidence.canonical(manifest)).hexdigest()
            single = evidence.sanitize_exchange(self.exchange(), "gcs", {"bucket": "private-bucket"},
                                                digest, "gcs-07")
            payload = evidence.canonical(single)
            (root / "capture.json").write_bytes(payload)
            row = {"id": "gcs-07", "provider": "gcs", "evidence_kind": "walk",
                   "api_versions": ["json-v1"], "status": "OBSERVED",
                   "evidence": {"json-v1": {"probe_request":
                       "GET /storage/v1/b/{bucket}/o?prefix=a", "expected_result": "two pages",
                       "capture_file": "capture.json", "capture_sha256": hashlib.sha256(payload).hexdigest()}}}
            with self.assertRaisesRegex(ValueError, "typed run"):
                evidence.validate_ledger({"rows": [row]}, {("gcs", "json-v1"): manifest}, root)
            first = self.exchange()
            first["response"]["body_base64"] = base64.b64encode(
                b'{"items":[{"bucket":"private-bucket","name":"a"}],"nextPageToken":"opaque"}'
            ).decode("ascii")
            second = self.exchange()
            second["request"]["url"] += "&pageToken=opaque"
            run = evidence.sanitize_run({"steps": [{"exchange": first}, {"exchange": second}]},
                                        "gcs", {"bucket": "private-bucket"}, digest,
                                        "gcs-07", "json-v1", "walk")
            run_bytes = evidence.canonical(run)
            (root / "capture.json").write_bytes(run_bytes)
            row["evidence"]["json-v1"]["capture_sha256"] = hashlib.sha256(run_bytes).hexdigest()
            evidence.validate_ledger({"rows": [row]}, {("gcs", "json-v1"): manifest}, root)
            second["request"]["url"] = second["request"]["url"].replace("opaque", "wrong")
            with self.assertRaisesRegex(ValueError, "continuation token"):
                evidence.sanitize_run({"steps": [{"exchange": first}, {"exchange": second}]},
                                      "gcs", {"bucket": "private-bucket"}, digest,
                                      "gcs-07", "json-v1", "walk")

    def test_token_wire_spelling_is_distinct_and_malformed_utf8_is_refused(self):
        capture = self.exchange()
        capture["request"]["url"] += "&pageToken=%2F"
        first = evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                           "a" * 64, "gcs-05")
        capture["request"]["url"] = capture["request"]["url"].replace("%2F", "%2f")
        second = evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                            "a" * 64, "gcs-05")
        self.assertEqual(first["request"]["query"], second["request"]["query"])
        self.assertNotEqual(first["request"]["token_wire_sha256"],
                            second["request"]["token_wire_sha256"])
        for bad in ("%zz", "%FF"):
            capture["request"]["url"] = capture["request"]["url"].split("&pageToken=")[0] \
                + "&pageToken=" + bad
            with self.assertRaises((ValueError, UnicodeError)):
                evidence.sanitize_exchange(capture, "gcs", {"bucket": "private-bucket"},
                                           "a" * 64, "gcs-05")

    def test_sdk_promotion_requires_exact_product_and_version_token(self):
        manifest = {"provider": "gcs", "namespace_mode": "flat-ubla", "region": "test",
                    "api_version": "json-v1", "sdk_product": "gcloud-java",
                    "sdk_version": "2.7", "capture_date": "2026-09-25",
                    "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}
        first = self.exchange()
        first["request"]["headers"].append({"name": "User-Agent",
                                            "value": "gcloud-java/2.73.0"})
        first["response"]["body_base64"] = base64.b64encode(
            b'{"items":[{"bucket":"private-bucket","name":"a"}],"nextPageToken":"opaque"}'
        ).decode("ascii")
        second = json.loads(json.dumps(first))
        second["request"]["url"] += "&pageToken=opaque"
        second["response"]["body_base64"] = base64.b64encode(
            b'{"items":[{"bucket":"private-bucket","name":"b"}]}'
        ).decode("ascii")
        digest = hashlib.sha256(evidence.canonical(manifest)).hexdigest()
        run = evidence.sanitize_run({"steps": [{"exchange": first}, {"exchange": second}]},
                                    "gcs", {"bucket": "private-bucket"}, digest,
                                    "gcs-09", "json-v1", "sdk_run")
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            payload = evidence.canonical(run)
            (root / "run.json").write_bytes(payload)
            row = {"id": "gcs-09", "provider": "gcs", "evidence_kind": "sdk_run",
                   "api_versions": ["json-v1"], "status": "OBSERVED", "evidence": {"json-v1": {
                       "probe_request": "GET /storage/v1/b/{bucket}/o?prefix=a",
                       "expected_result": "two SDK pages", "capture_file": "run.json",
                       "capture_sha256": hashlib.sha256(payload).hexdigest()}}}
            with self.assertRaisesRegex(ValueError, "exact pinned product/version"):
                evidence.validate_ledger({"rows": [row]}, {("gcs", "json-v1"): manifest}, root)
            retry = self.exchange()
            retry["request"]["headers"].append({"name": "User-Agent",
                                                "value": "gcloud-java/2.73.0"})
            retry["response"]["status"] = 503
            retry["response"]["body_base64"] = base64.b64encode(
                b'{"error":{"code":503,"message":"busy","errors":[{"reason":"backendError"}]}}'
            ).decode("ascii")
            retry_run = evidence.sanitize_run({"steps": [
                {"phase": "retry_disabled", "exchange": retry},
                {"phase": "normal_retry", "exchange": retry},
                {"phase": "normal_retry", "exchange": retry}]},
                "gcs", {"bucket": "private-bucket"}, digest,
                "gcs-10", "json-v1", "retry_run")
            retry_bytes = evidence.canonical(retry_run)
            (root / "retry.json").write_bytes(retry_bytes)
            retry_row = {"id": "gcs-10", "provider": "gcs", "evidence_kind": "retry_run",
                         "api_versions": ["json-v1"], "status": "OBSERVED", "evidence": {"json-v1": {
                             "probe_request": "GET /storage/v1/b/{bucket}/o?prefix=a",
                             "expected_result": "separate retry attempt counts", "capture_file": "retry.json",
                             "capture_sha256": hashlib.sha256(retry_bytes).hexdigest()}}}
            with self.assertRaisesRegex(ValueError, "exact pinned product/version"):
                evidence.validate_ledger({"rows": [retry_row]}, {("gcs", "json-v1"): manifest}, root)

    def test_checked_in_matrix_matches_machine_ledger(self):
        repo = pathlib.Path(__file__).resolve().parents[2]
        ledger = evidence.strict_json((repo / "docs/replay-provider-ledger.json").read_text())
        matrix = (repo / "docs/replay-provider-conformance.md").read_text()
        evidence.validate_matrix(matrix, ledger)
        # Every archived capture is checked against its manifest. REPLAY_MATCH receipts also
        # bind a replay distribution, which `evidence.py validate --distribution` checks, and Go
        # receipts the binary go/build.sh builds at their commit, which `validate --repo-root`
        # rebuilds; here those rows are held to the OBSERVED proof they contain.
        observed = json.loads(json.dumps(ledger))
        for row in observed["rows"]:
            for proof in row.get("evidence", {}).values():
                proof.pop("go_replay", None)
            if row["status"] == "REPLAY_MATCH":
                row["status"] = "OBSERVED"
                for proof in row["evidence"].values():
                    for key in ("replay_test", "replay_test_path", "replay_result_file", "replay_result_sha256"):
                        proof.pop(key, None)
        with evidence.evidence_root(evidence.EVIDENCE_ARCHIVE) as root:
            evidence.validate_ledger(observed, evidence.load_manifests(root), root)
        with self.assertRaisesRegex(ValueError, "matrix differs"):
            evidence.validate_matrix(matrix.replace("gcs-01 |", "gcs-01 |", 1)
                                     .replace("Provisional bounded list", "Unsupported", 1), ledger)

    def test_azure_promotion_requires_each_service_version_and_matching_headers(self):
        base_manifest = {"provider": "azure", "namespace_mode": "flat-hns-off", "region": "test-region",
                         "api_version": "2026-06-06", "sdk_product": "azsdk-java-azure-storage-blob",
                         "sdk_version": "12.35.1",
                         "capture_date": "2026-09-25",
                         "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}
        version10 = {**base_manifest, "api_version": "2026-10-06"}
        proof = {"probe_request": "GET /{container}?restype=container&comp=list",
                 "expected_result": "one blob", "capture_file": "capture.json",
                 "capture_sha256": "f" * 64}
        with self.assertRaisesRegex(ValueError, "separate evidence"):
            evidence.validate_ledger({"rows": [{"id": "azure-03", "provider": "azure",
                "evidence_kind": "exchange",
                "api_versions": ["2026-06-06", "2026-10-06"], "status": "OBSERVED",
                "evidence": {"2026-06-06": proof}}]})
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            wrong_version_capture = {"provider": "azure", "api_version": "2026-06-06",
                "probe_id": "azure-02", "manifest_sha256": hashlib.sha256(
                    evidence.canonical(version10)).hexdigest(),
                "request": {"method": "GET", "path": "/{container}",
                            "query": "restype=container&comp=list",
                            "headers": [{"name": "x-ms-version", "value": "2026-06-06"}]},
                "response": {"status": 200,
                             "headers": [{"name": "x-ms-version", "value": "2026-06-06"}],
                             "body_base64": ""}}
            data = evidence.canonical(wrong_version_capture)
            (root / "capture.json").write_bytes(data)
            promoted = {"id": "azure-02", "provider": "azure", "evidence_kind": "exchange",
                "api_versions": ["2026-10-06"], "status": "OBSERVED",
                "evidence": {"2026-10-06": {**proof,
                    "capture_sha256": hashlib.sha256(data).hexdigest()}}}
            with self.assertRaisesRegex(ValueError, "provenance/version"):
                evidence.validate_ledger({"rows": [promoted]},
                    {("azure", "2026-10-06"): version10}, root)


# A stand-in for go/build.sh: the "binary" is go/main.go, the target settings and the commit, so a
# test can compute the bytes the receipt commit builds without a Go toolchain.
FAKE_BUILD = """#!/bin/sh
set -eu
cd "$(dirname "$0")"
[ "$1" = -o ] && [ "$3" = ./cmd/swath-replay ]
[ -z "${FAKE_ENV_LOG:-}" ] || env >> "$FAKE_ENV_LOG"
{ cat main.go; printf '%s/%s cgo=%s %s' "$GOOS" "$GOARCH" "$CGO_ENABLED" "$(git rev-parse HEAD)"; } > "$2"
"""
# A stand-in for the go command: `go test` writes the outcome reports FAKE_OUTCOMES names.
FAKE_GO = """#!/bin/sh
set -eu
[ "$1" = test ]
[ -z "${FAKE_ENV_LOG:-}" ] || env >> "$FAKE_ENV_LOG"
mkdir -p "$SWATH_PROVIDER_REPORT/generated" "$SWATH_REPLAY_PROVIDER_REPORT"
cat "$FAKE_OUTCOMES/gcs.json" > "$SWATH_PROVIDER_REPORT/generated/gcs-outcomes.json"
cat "$FAKE_OUTCOMES/azure.json" > "$SWATH_REPLAY_PROVIDER_REPORT/azure-outcomes.json"
"""


class GoReceiptTest(unittest.TestCase):
    """A `producer: go` receipt binds the Go evidence test sources and go/build.sh's binary at its
    commit; both are recomputed there by `validate`, and the commit must be an ancestor."""

    MANIFEST = {"provider": "gcs", "namespace_mode": "flat-ubla", "region": "test-region",
                "api_version": "json-v1", "sdk_product": "gcloud-java", "sdk_version": "2.73.0",
                "capture_date": "2026-09-25", "objects": [{"name": "a", "size": 1, "sha256": "a" * 64}]}

    def git(self, *arguments):
        return subprocess.run(("git", "-C", str(self.root), *arguments), check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)
        return self.git("rev-parse", "HEAD")

    def write(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name) / "repo"
        self.root.mkdir()
        self.git("init", "-q")
        self.git("config", "user.name", "Replay Test")
        self.git("config", "user.email", "replay-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.write("go/go.mod", "module example.invalid/replay\n\ngo 1.26.0\n\ntoolchain go1.26.10\n")
        self.write("go/go.sum", "")
        self.write("go/main.go", "package main\n")
        build = self.write("go/build.sh", FAKE_BUILD)
        build.chmod(0o755)
        self.write("go/internal/replay/evidence/doc.go", "package evidence\n")
        self.write("go/internal/replay/evidence/evidence_test.go",
                   "package evidence\n\nfunc TestGCSNativeCapturesMatchReplay(t *testing.T) {}\n")
        self.write("go/sortedparquet/sortedparquettest/sortedparquettest.go", "package sortedparquettest\n")
        self.write("go/internal/replay/azure/evidence_test.go",
                   "package azure_test\n\nfunc TestAzureEvidenceReplay(t *testing.T) {}\n")
        self.write("go/internal/replay/azure/evidencecmp_test.go", "package azure_test\n")
        self.write("go/internal/replay/azure/sdk_test.go", "package azure_test\n")
        # Not part of the bound set: the route's other tests do not run in the evidence replay.
        self.write("go/internal/replay/azure/port_test.go", "package azure\n")
        self.write("EvidenceTest.java", "class EvidenceTest { @Test void exactTest() {} }")
        self.write(".gitignore", "/evidence/\n")
        self.digest = hashlib.sha256(evidence.canonical(self.MANIFEST)).hexdigest()
        safe = evidence.sanitize_exchange(EvidenceTest.exchange(self), "gcs", {"bucket": "private-bucket"},
                                          self.digest, "gcs-basic")
        self.capture = evidence.canonical(safe)
        self.write("evidence/gcs/captures/gcs-basic.json", "")
        (self.root / "evidence/gcs/captures/gcs-basic.json").write_bytes(self.capture)
        self.receipt_commit = self.commit("fixture")

    def binary_sha256(self, commit):
        main = subprocess.run(("git", "-C", str(self.root), "show", f"{commit}:go/main.go"), check=True,
                              capture_output=True).stdout
        return hashlib.sha256(main + f"linux/amd64 cgo=0 {commit}".encode()).hexdigest()

    def receipt(self, **changes):
        sources = {path: hashlib.sha256(subprocess.run(
            ("git", "-C", str(self.root), "show", f"{self.receipt_commit}:{path}"), check=True,
            capture_output=True).stdout).hexdigest() for path in (
            "go/go.mod", "go/go.sum", "go/internal/replay/evidence/doc.go",
            "go/internal/replay/evidence/evidence_test.go",
            "go/sortedparquet/sortedparquettest/sortedparquettest.go",
            "go/internal/replay/azure/evidence_test.go", "go/internal/replay/azure/evidencecmp_test.go",
            "go/internal/replay/azure/sdk_test.go")}
        receipt = {"schema_version": "provider-match-receipt-v2", "producer": "go",
                   "comparison": "ProviderExchangeComparator", "status": "PASS",
                   "test": "TestGCSNativeCapturesMatchReplay",
                   "test_path": "go/internal/replay/evidence/evidence_test.go",
                   "test_sources": sorted(sources),
                   "test_source_sha256": hashlib.sha256(evidence.canonical(sources)).hexdigest(),
                   "api_version": "json-v1", "evidence_kind": "exchange", "compared_steps": 1,
                   "capture_sha256": hashlib.sha256(self.capture).hexdigest(),
                   "replay_commit": self.receipt_commit, "go_toolchain": "go1.26.10",
                   "distribution_sha256": self.binary_sha256(self.receipt_commit),
                   "distribution_name": "swath-replay-linux-amd64"}
        receipt.update(changes)
        return {key: value for key, value in receipt.items() if value is not None}

    def row(self, receipt, slot="go"):
        payload = evidence.canonical(receipt)
        path = self.root / "evidence/gcs/receipts/go/gcs-basic.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        proof = {"probe_request": "GET /storage/v1/b/{bucket}/o?prefix=a", "expected_result": "one object",
                 "capture_file": "gcs/captures/gcs-basic.json",
                 "capture_sha256": hashlib.sha256(self.capture).hexdigest()}
        cited = {"replay_test": receipt.get("test"), "replay_test_path": receipt.get("test_path"),
                 "replay_result_file": "gcs/receipts/go/gcs-basic.json",
                 "replay_result_sha256": hashlib.sha256(payload).hexdigest()}
        if slot == "go":
            return {"id": "gcs-basic", "provider": "gcs", "evidence_kind": "exchange", "api_versions": ["json-v1"],
                    "status": "OBSERVED", "evidence": {"json-v1": {**proof, "go_replay": cited}}}
        return {"id": "gcs-basic", "provider": "gcs", "evidence_kind": "exchange", "api_versions": ["json-v1"],
                "status": "REPLAY_MATCH", "evidence": {"json-v1": {**proof, **cited}}}

    def validate(self, row, head=None, distribution=None):
        evidence.validate_ledger({"rows": [row]}, {("gcs", "json-v1"): self.MANIFEST}, self.root / "evidence",
                                 self.root, head or self.git("rev-parse", "HEAD"), distribution)

    def test_go_receipt_validates_at_its_commit_and_later_commits(self):
        self.validate(self.row(self.receipt()))
        self.write("go/main.go", "package main // changed after the receipt\n")
        later = self.commit("change the replay")
        # The binary is recomputed at the receipt commit, so a later commit keeps the receipt valid.
        self.validate(self.row(self.receipt()), later)
        self.assertEqual(evidence.go_matrix_cell(self.row(self.receipt())), "`json-v1:TestGCSNativeCapturesMatchReplay`")

    def test_changed_binary_hash_is_refused(self):
        with self.assertRaisesRegex(ValueError, "binary differs"):
            self.validate(self.row(self.receipt(distribution_sha256="f" * 64)))
        later = self.commit_change("go/main.go", "package main // changed\n")
        # A receipt naming the later commit's binary but the earlier commit is refused too.
        with self.assertRaisesRegex(ValueError, "binary differs"):
            self.validate(self.row(self.receipt(distribution_sha256=self.binary_sha256(later))), later)

    def commit_change(self, path, text):
        self.write(path, text)
        return self.commit("change " + path)

    def test_changed_test_source_is_refused(self):
        with self.assertRaisesRegex(ValueError, "test sources differ from its commit"):
            self.validate(self.row(self.receipt(test_source_sha256="f" * 64)))
        with self.assertRaisesRegex(ValueError, "test sources differ from its commit"):
            self.validate(self.row(self.receipt(test_sources=["go/internal/replay/evidence/evidence_test.go"])))
        # A test of the route outside the bound set may change; a bound source may not.
        self.commit_change("go/internal/replay/azure/port_test.go", "package azure // changed\n")
        self.validate(self.row(self.receipt()))
        for path in ("go/internal/replay/azure/sdk_test.go", "go/go.sum"):
            with self.subTest(path=path):
                self.commit_change(path, "// changed\n")
                with self.assertRaisesRegex(ValueError, "changed since the receipt commit"):
                    self.validate(self.row(self.receipt()))
                self.git("reset", "-q", "--hard", "HEAD~1")
        self.commit_change("go/internal/replay/evidence/new_test.go", "package evidence\n")
        with self.assertRaisesRegex(ValueError, "changed since the receipt commit"):
            self.validate(self.row(self.receipt()))

    def test_dirty_evidence_sources_are_refused_and_unrelated_edits_pass(self):
        """The working tree's bytes are checked too: staged, unstaged and added sources are refused,
        an edit outside the named set is not."""
        row = self.row(self.receipt())
        self.validate(row)
        self.write("docs/ledger-note.md", "unrelated unstaged edit\n")
        self.write("go/internal/replay/azure/port_test.go", "package azure // unrelated unstaged edit\n")
        self.validate(row)
        self.git("add", "docs/ledger-note.md")
        self.validate(row)
        source = "go/internal/replay/evidence/evidence_test.go"
        original = (self.root / source).read_text()
        for name, change in (("unstaged", lambda: self.write(source, original + "// edited\n")),
                             ("staged", lambda: (self.write(source, original + "// edited\n"),
                                                 self.git("add", source))),
                             ("added", lambda: self.write("go/internal/replay/evidence/extra_test.go",
                                                          "package evidence\n")),
                             ("added in a staged directory", lambda: (
                                 self.write("go/sortedparquet/sortedparquettest/extra.go", "package x\n"),
                                 self.git("add", "go/sortedparquet/sortedparquettest/extra.go"))),
                             ("go.sum", lambda: self.write("go/go.sum", "x\n")),
                             ("deleted", lambda: (self.root / source).unlink())):
            with self.subTest(name):
                change()
                with self.assertRaisesRegex(ValueError, "working tree differ"):
                    self.validate(row)
                self.git("reset", "-q", "--hard", "HEAD")
                self.git("clean", "-qfd", "go")
                self.git("reset", "-q")
                self.validate(row)

    def test_non_ancestor_commit_is_refused(self):
        main = self.git("rev-parse", "--abbrev-ref", "HEAD")
        self.git("checkout", "-q", "-b", "side")
        side = self.commit_change("go/main.go", "package main // side\n")
        self.git("checkout", "-q", main)
        head = self.commit_change("README", "main line\n")
        receipt = self.receipt(replay_commit=side, distribution_sha256=self.binary_sha256(side))
        with self.assertRaisesRegex(ValueError, "not an ancestor"):
            self.validate(self.row(receipt), head)

    def test_go_receipt_claiming_java_fields_is_refused(self):
        java_fields = {"test": "exactTest", "test_path": "EvidenceTest.java"}
        with self.assertRaisesRegex(ValueError, "test sources differ|another producer"):
            self.validate(self.row(self.receipt(**java_fields)))
        with self.assertRaisesRegex(ValueError, "another producer"):
            self.validate(self.row(self.receipt(distribution_name="replay.jar")))
        for missing in ("test_sources", "go_toolchain"):
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, "another producer"):
                self.validate(self.row(self.receipt(**{missing: None})))
        with self.assertRaisesRegex(ValueError, "toolchain differs"):
            self.validate(self.row(self.receipt(go_toolchain="go1.26.9")))
        # A Go receipt cited as the Java receipt, and a Java receipt cited as the Go one.
        (self.root / "replay.jar").write_bytes(b"replay-binary")
        with self.assertRaisesRegex(ValueError, "not a java receipt"):
            self.validate(self.row(self.receipt(), slot="java"), distribution=self.root / "replay.jar")
        java = self.receipt(producer=None, test_sources=None, go_toolchain=None, **java_fields,
                            test_source_sha256=hashlib.sha256((self.root / "EvidenceTest.java").read_bytes()).hexdigest(),
                            distribution_sha256=hashlib.sha256(b"replay-binary").hexdigest(),
                            distribution_name="replay.jar")
        with self.assertRaisesRegex(ValueError, "not a go receipt"):
            self.validate(self.row(java))
        # The same Java receipt is valid in the Java slot; with a Go field it is refused there.
        self.validate(self.row(java, slot="java"), distribution=self.root / "replay.jar")
        with self.assertRaisesRegex(ValueError, "another producer"):
            self.validate(self.row({**java, "go_toolchain": "go1.26.10"}, slot="java"),
                          distribution=self.root / "replay.jar")

    def test_folded_version_cannot_carry_a_go_receipt(self):
        with self.assertRaisesRegex(ValueError, "cannot carry a replay match"):
            evidence.validate_version_equivalence({"provider": "azure", "id": "azure-03a"}, "2026-10-06",
                                                  {"go_replay": {}}, {}, self.root)

    def test_go_receipts_written_from_the_go_replay_outcomes_validate(self):
        index = {"provider": "gcs", "captures": {"gcs-basic": {"file": "captures/gcs-basic.json"},
                                                 "gcs-other": {"file": "captures/gcs-basic.json"}}}
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            for name, payload in (("gcs/index.json", json.dumps(index).encode()),
                                  ("gcs/captures/gcs-basic.json", self.capture),
                                  ("azure/index.json", b'{"provider": "azure", "captures": {}}')):
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
        (self.root / "docs").mkdir()
        (self.root / "docs/replay-provider-evidence.tar.gz").write_bytes(buffer.getvalue())
        self.receipt_commit = self.commit("archive")
        tools = self.root.parent / "tools"
        outcomes = self.root.parent / "outcomes"
        for directory in (tools, outcomes):
            directory.mkdir()
        (tools / "go").write_text(FAKE_GO, encoding="utf-8")
        (tools / "go").chmod(0o755)
        (outcomes / "gcs.json").write_text('{"gcs-basic": "match", "gcs-other": "mismatch: x"}')
        (outcomes / "azure.json").write_text("{}")
        environment = {"PATH": f"{tools}{os.pathsep}{os.environ['PATH']}", "FAKE_OUTCOMES": str(outcomes)}
        out = self.root / "evidence"
        with mock.patch.dict(os.environ, environment):
            self.write("untracked", "dirty")
            with self.assertRaisesRegex(ValueError, "clean tested source tree"):
                evidence.write_go_receipts(self.root, self.root.parent, out)
            (self.root / "untracked").unlink()
            self.assertEqual(evidence.write_go_receipts(self.root, self.root.parent, out), {"azure": 0, "gcs": 1})
        written = evidence.strict_json((out / "gcs/receipts/go/gcs-basic.json").read_text())
        self.assertEqual(written, self.receipt())
        self.assertFalse((out / "gcs/receipts/go/gcs-other.json").exists())
        row = self.row(written)
        self.assertEqual(row["evidence"]["json-v1"]["go_replay"]["replay_result_sha256"],
                         hashlib.sha256((out / "gcs/receipts/go/gcs-basic.json").read_bytes()).hexdigest())
        self.validate(row)
        # An outcome report that misses an archived capture writes nothing.
        (outcomes / "gcs.json").write_text('{"gcs-basic": "match"}')
        with mock.patch.dict(os.environ, environment), self.assertRaisesRegex(ValueError, "does not cover"):
            evidence.write_go_receipts(self.root, self.root.parent, self.root.parent / "elsewhere")

    def test_go_build_and_test_ignore_inherited_go_configuration(self):
        """A Go config file or workspace can add `-overlay=` and so substitute evidence sources while
        git stays clean; every Go build and test runs with the configuration disabled."""
        self.test_go_receipts_written_from_the_go_replay_outcomes_validate()
        tools = self.root.parent / "tools"
        log = self.root.parent / "env.log"
        config = self.root.parent / "goenv"
        config.write_text("GOFLAGS=-overlay=/elsewhere/overlay.json\n", encoding="utf-8")
        hostile = {"GOENV": str(config), "GOFLAGS": "-overlay=/elsewhere/overlay.json", "GOWORK": "/elsewhere/go.work",
                   "GOROOT": "/elsewhere/goroot", "GODEBUG": "x=1", "GOFIPS140": "latest", "CGO_CFLAGS": "-DX",
                   "GOPROXY": "https://proxy.example.invalid", "GOEXPERIMENT": "boringcrypto",
                   "PATH": f"{tools}{os.pathsep}{os.environ['PATH']}", "FAKE_ENV_LOG": str(log),
                   "FAKE_OUTCOMES": str(self.root.parent / "outcomes")}
        (self.root.parent / "outcomes" / "gcs.json").write_text('{"gcs-basic": "match", "gcs-other": "match"}')
        evidence.go_distribution_sha256.cache_clear()
        with mock.patch.dict(os.environ, hostile):
            evidence.write_go_receipts(self.root, self.root.parent, self.root.parent / "out")
            evidence.go_distribution_sha256(self.root, self.git("rev-parse", "HEAD"))
        # Three runs: the receipt build, the replay test, and validate's rebuild.
        text = log.read_text()
        self.assertEqual(text.count("GOENV=off\n"), 3)
        self.assertEqual(text.count("GOWORK=off\n"), 3)
        self.assertEqual(text.count("GOFLAGS=\n"), 3)
        for dropped in ("GOROOT=", "GODEBUG=", "GOFIPS140=", "CGO_CFLAGS=", "overlay", "boringcrypto", "/elsewhere"):
            self.assertNotIn(dropped, text)
        self.assertEqual(text.count("GOPROXY=https://proxy.example.invalid\n"), 3)


if __name__ == "__main__":
    unittest.main()
