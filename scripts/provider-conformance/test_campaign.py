#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Offline checks for the live campaign guards; no provider is contacted."""

import base64
import json
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.parse import parse_qsl, quote, unquote

import azure_live
import campaign
import evidence
import gcs_live
import matrix
import s3_live
from campaign import (CLEANUP, PROBE, SETUP, WINDOW_ACTIVE, WINDOW_CLEANUP, BoundExceeded, Bounds,
                      Campaign, CampaignLedger, Listed, NoAuthorizer, OwnershipJournal, Redactor, Request)
from matrix import Case, CorpusObject, PriceTable, TokenSource, Untested

RUN = "probe/r20261021t120000z-0123abcd/"


class Clock:
    def __init__(self):
        self.now = 1_000.0

    def __call__(self):
        return self.now


class FakeStore:
    """An in-memory listing service behind the fake connector."""

    def __init__(self, foreign=(), fail_deletes=False, page=2, repeat_token=False, preexisting=()):
        self.objects = {name: b"x" for name in (*foreign, *preexisting)}
        self.sent = []
        self.credential_queries = []
        self.fail_deletes = fail_deletes
        self.page = page
        self.repeat_token = repeat_token
        self.lock = threading.Lock()
        self.padding = ""

    def exchange(self, request, credential_query, extra_headers, deadline, max_bytes, on_bytes):
        with self.lock:
            self.sent.append((request.method, request.path, request.query))
            self.credential_queries.append(credential_query)
            status, headers, body = self._respond(request)
        on_bytes(len(body))
        if len(body) > max_bytes:
            raise BoundExceeded("response exceeded the per-response byte cap")
        return status, headers, body

    def _respond(self, request):
        if request.path.startswith("/o/"):
            name = unquote(request.path[3:])
            if request.method == "PUT":
                if name in self.objects:
                    return 412, [], b""
                self.objects[name] = request.body
                return 201, [], b""
            if self.fail_deletes:
                return 500, [], b""
            version = dict(parse_qsl(request.query)).get("if")
            if name in self.objects and version not in (None, "v1"):
                return 412, [], b""
            return (204, [], b"") if self.objects.pop(name, None) is not None else (404, [], b"")
        query = dict(parse_qsl(request.query, keep_blank_values=True))
        names = sorted(n for n in self.objects if n.startswith(query.get("prefix", "")))
        start = int(query.get("token", "0")) if query.get("token", "0").isdigit() else 0
        chunk = names[start:start + self.page]
        next_token = None
        if start + self.page < len(names):
            repeat = self.repeat_token and "walk" in query
            next_token = "0" if repeat else str(start + self.page)
        body = json.dumps({"items": [{"name": n, "size": len(self.objects[n])} for n in chunk],
                           "next": next_token, "padding": self.padding}).encode()
        return 200, [("content-type", "application/json")], body


class FakeProvider:
    name = "fake"
    api_versions = ("v1",)

    def __init__(self, objects=3, cases=None):
        self.object_count = objects
        self.case_list = cases

    def redactions(self):
        return {"private-account": "{account}", "sig=private-signature": "{sas}"}

    def private_identifiers(self):
        return ["private-account", "private-signature"]

    def authorizer(self, client):
        return CredentialQuery()

    def corpus(self, run_prefix, profile):
        return [CorpusObject(f"{run_prefix}o/{i}", b"ab", "o") for i in range(self.object_count)]

    def cases(self, run_prefix, profile):
        if self.case_list is not None:
            return self.case_list
        return [Case("fake-01", "exchange", "q", "exchange", "prefix=" + quote(run_prefix)),
                Case("fake-02", "walk", "q", "walk", "prefix=" + quote(run_prefix)),
                Case("fake-03", "token", "q", "token", "prefix=" + quote(run_prefix),
                     token=TokenSource("fake-02", 0)),
                Case("fake-04", "flip", "q", "token", "prefix=" + quote(run_prefix),
                     token=TokenSource("fake-02", 0, "flip"))]

    def untested(self):
        return [Untested("nothing", "fake")]

    def prices(self):
        return PriceTable("fake", "2026-10-21", 0.000005, 0.02, 0.12)

    def upload(self, item):
        return Request("PUT", "private-account.invalid", "/o/" + quote(item.name, safe=""), body=item.payload)

    def created(self, response):
        return response.status == 201

    def delete(self, name, version):
        return Request("DELETE", "private-account.invalid", "/o/" + quote(name, safe=""),
                       f"if={version}" if version else "")

    def deleted(self, response):
        return response.status == 204

    def list_request(self, query, version, headers=()):
        return Request("GET", "private-account.invalid", "/list", query, tuple(headers))

    def with_token(self, query, token):
        return query + "&token=" + token

    def page(self, body):
        parsed = json.loads(body)
        return [Listed(i["name"], size=i["size"], updated="2026-10-21T00:00:00Z", version="v1")
                for i in parsed["items"]], parsed["next"]

    def inventory_query(self, prefix):
        return "prefix=" + quote(prefix) if prefix else ""

    def version_echoed(self, version, response):
        return True

    def version_unavailable(self, response):
        return response.status == 400

    def manifest_fields(self):
        return {"api_version": "v1"}

    def sdk_options(self, case, version):
        return {"provider": "fake"}

    def sdk_environment(self):
        return {"SECRET": "value"}

    def sdk_identity(self):
        return {"version": "1"}

    def sanitize_names(self):
        return {}

    def canonical_name(self, name):
        return name

    def raw_wire(self, raw, manifest_sha256, probe_id, capture_sha256):
        return None

    def scrub(self, body):
        return body


class CredentialQuery(NoAuthorizer):
    def credential_query(self):
        return "sig=private-signature"


def campaign_for(directory, provider=None, store=None, bounds=Bounds(), clock=None, **options):
    store = store or FakeStore()
    run = Campaign(provider or FakeProvider(), Path(directory), bounds, connector=store, clock=clock or Clock(),
                   sdk=campaign.SdkLauncher("/jdk", Path(directory) / "cp.txt"), sleep=lambda s: None,
                   **options)
    return run, store


def run_dir(tmp):
    return Path(tmp) / "runs" / RUN.rstrip("/").rsplit("/", 1)[-1]


class BoundsTest(unittest.TestCase):
    def test_bounds_can_only_tighten_and_must_be_finite(self):
        self.assertEqual(Bounds().tightened(concurrency=2, active_seconds=60).concurrency, 2)
        for relaxed in ({"max_attempts": 25_001}, {"concurrency": 5}, {"request_seconds": 31},
                        {"cleanup_reserve": 7_499}, {"max_objects": 6_001}, {"active_seconds": float("nan")},
                        {"max_payload_bytes": 16 * campaign.MIB + 1}, {"max_cost_usd": 2.51},
                        {"concurrency": True}, {"max_walk_pages": 0}):
            with self.assertRaises(ValueError, msg=relaxed):
                Bounds(**relaxed)

    def test_reserve_keeps_its_share_and_leaves_room(self):
        Bounds(max_attempts=100, cleanup_reserve=30)
        with self.assertRaises(ValueError):
            Bounds(max_attempts=100, cleanup_reserve=29)
        with self.assertRaises(ValueError):
            Bounds(max_attempts=7_500)


class LedgerTest(unittest.TestCase):
    def test_active_phases_never_touch_the_cleanup_reserve(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = CampaignLedger(Path(tmp) / "a.jsonl", Bounds(max_attempts=10, cleanup_reserve=3), Clock())
            ledger.open(WINDOW_ACTIVE)
            for _ in range(7):
                ledger.acquire(SETUP, "PUT", "/o")
            with self.assertRaises(BoundExceeded):
                ledger.acquire(PROBE, "GET", "/list")
            ledger.open(WINDOW_CLEANUP)
            for _ in range(3):
                ledger.acquire(CLEANUP, "DELETE", "/o")
            with self.assertRaises(BoundExceeded):
                ledger.acquire(CLEANUP, "DELETE", "/o")
            self.assertEqual(ledger.attempts, 10)
            self.assertTrue(ledger.exhausted)
            ledger.close()

    def test_budget_time_and_exhaustion_persist_across_crashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.jsonl"
            clock = Clock()
            bounds = Bounds(max_attempts=20, cleanup_reserve=6, active_seconds=100)
            first = CampaignLedger(path, bounds, clock)
            first.open(WINDOW_ACTIVE)
            first.acquire(SETUP, "PUT", "/o")
            clock.now += 30
            first.acquire(SETUP, "PUT", "/o")
            first.allowance("sdk", 5, planned_until=clock.now + 20)
            first.mark_exhausted("synthetic")
            # Crash: no settle, no window close, and a torn final record.
            first._file.write('{"kind": "se')
            first._file.close()
            first._lock_file.close()
            second = CampaignLedger(path, bounds, clock)
            self.assertEqual(second.attempts, 8)
            self.assertAlmostEqual(second.window_used[WINDOW_ACTIVE], 50.0 + campaign.HEARTBEAT_SECONDS)
            self.assertEqual(second.exhausted, ["synthetic"])
            second.close()

    def test_a_reopened_window_charges_the_crashed_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "a.jsonl"
            clock = Clock()
            first = CampaignLedger(path, Bounds(active_seconds=100), clock)
            first.open(WINDOW_ACTIVE)
            clock.now += 30
            first.acquire(SETUP, "PUT", "/o")
            first._file.close()
            first._lock_file.close()
            second = CampaignLedger(path, Bounds(active_seconds=100), clock)
            second.open(WINDOW_ACTIVE)
            clock.now += 10
            second.acquire(SETUP, "PUT", "/o")
            second._file.close()
            second._lock_file.close()
            third = CampaignLedger(path, Bounds(active_seconds=100), clock)
            self.assertAlmostEqual(third.window_used[WINDOW_ACTIVE], 40.0 + campaign.HEARTBEAT_SECONDS)
            third.close()

    def test_a_second_process_cannot_share_the_campaign(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = CampaignLedger(Path(tmp) / "a.jsonl", Bounds(), Clock())
            with self.assertRaises(BoundExceeded):
                CampaignLedger(Path(tmp) / "a.jsonl", Bounds(), Clock())
            first.close()

    def test_allowance_is_refunded_only_by_settlement(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = CampaignLedger(Path(tmp) / "a.jsonl", Bounds(max_attempts=20, cleanup_reserve=6), Clock())
            ledger.open(WINDOW_ACTIVE)
            granted = ledger.allowance("sdk", 50, planned_until=0)
            self.assertEqual(granted, 14)
            with self.assertRaises(BoundExceeded):
                ledger.acquire(PROBE, "GET", "/list")
            with self.assertRaises(ValueError):
                ledger.settle("sdk", granted, granted + 1)
            ledger.settle("sdk", granted, 3)
            self.assertEqual(ledger.attempts, 3)
            ledger.close()

    def test_response_bytes_are_capped_campaign_wide(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = CampaignLedger(Path(tmp) / "a.jsonl", Bounds(max_total_response_bytes=10), Clock())
            ledger.charge_bytes(10)
            with self.assertRaises(BoundExceeded):
                ledger.charge_bytes(1)
            ledger.close()

    def test_attempts_outside_an_open_window_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = CampaignLedger(Path(tmp) / "a.jsonl", Bounds(), Clock())
            with self.assertRaises(BoundExceeded):
                ledger.acquire(SETUP, "PUT", "/o")
            ledger.open(WINDOW_CLEANUP)
            with self.assertRaises(BoundExceeded):
                ledger.acquire(SETUP, "PUT", "/o")
            ledger.close()


class OwnershipTest(unittest.TestCase):
    def test_names_outside_the_run_prefix_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = OwnershipJournal(Path(tmp) / "owned.jsonl", RUN, Redactor({}))
            journal.intent(CorpusObject(RUN + "a", b"", "g"))
            for bad in ("probe/other/a", RUN, "a", RUN[:-1] + "x/a"):
                with self.assertRaises(ValueError):
                    journal.intent(CorpusObject(bad, b"", "g"))
            with self.assertRaises(ValueError):
                OwnershipJournal(Path(tmp) / "x.jsonl", "probe/", Redactor({}))
            with (Path(tmp) / "owned.jsonl").open("a") as handle:
                handle.write(json.dumps({"event": "intent", "name": "production/data"}) + "\n")
            with self.assertRaises(ValueError):
                journal.entries()


class CampaignRunTest(unittest.TestCase):
    def test_complete_run_deletes_only_its_prefix_and_writes_no_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            foreign = ["someone-else/keep", "probe/r20200101t000000z-ffffffff/keep"]
            run, store = campaign_for(tmp, store=FakeStore(foreign=foreign))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)  # foreign names remain
            cleanup = receipt["cleanup"]
            self.assertEqual((cleanup["deleted"], cleanup["leftovers"]), (3, []))
            self.assertEqual(sorted(cleanup["foreign"]), sorted(foreign))
            self.assertTrue(all(name in store.objects for name in foreign))
            deleted = [(path, query) for method, path, query in store.sent if method == "DELETE"]
            self.assertTrue(all(unquote(path[3:]).startswith(RUN) and query == "if=v1" for path, query in deleted))
            for path in Path(tmp).rglob("*"):
                if path.is_file():
                    text = path.read_text(errors="replace")
                    self.assertNotIn("private-account", text, path)
                    self.assertNotIn("private-signature", text, path)
            self.assertTrue(all(q == "sig=private-signature" for q in store.credential_queries))
            self.assertEqual(receipt["cases"]["fake-02"]["pages"], 2)
            self.assertTrue(receipt["cases"]["fake-03"]["complete"])

    def test_clean_namespace_run_is_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, _ = campaign_for(tmp)
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.COMPLETE, receipt["incomplete_reasons"])
            self.assertTrue(receipt["cleanup"]["verified_empty"])
            manifest = json.loads((run_dir(tmp) / "manifest.json").read_text())
            self.assertEqual(len(manifest["objects"]), 3)
            flipped = json.loads((run_dir(tmp) / "raw" / "fake-04.json").read_text())
            self.assertTrue(flipped["request"]["url"].endswith("&token=A"))

    def test_an_occupied_prefix_is_never_provisioned_or_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, store = campaign_for(tmp, store=FakeStore(preexisting=[RUN + "o/0"]))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)
            self.assertFalse(receipt["cleanup"]["claimed"])
            self.assertIn(RUN + "o/0", store.objects)
            self.assertFalse([m for m, _, _ in store.sent if m in ("PUT", "DELETE")])

    def test_budget_exhaustion_is_sticky_and_still_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Active work may use 48 attempts, so 60 objects cannot all be created; the
            # 52-attempt reserve still covers discovery, 47 deletes and verification.
            run, store = campaign_for(tmp, provider=FakeProvider(objects=60), store=FakeStore(page=100),
                                      bounds=Bounds(max_attempts=100, cleanup_reserve=52))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)
            self.assertTrue(any("budget" in reason for reason in receipt["incomplete_reasons"]))
            self.assertLessEqual(receipt["ledger"]["attempts"], 100)
            self.assertTrue(receipt["cleanup"]["verified_empty"], receipt["cleanup"])

    def test_oversized_response_is_never_retried_into_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FakeStore()
            store.padding = "x" * 200
            run, _ = campaign_for(tmp, store=store, bounds=Bounds(max_response_bytes=100))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)
            self.assertTrue(any("per-response" in reason for reason in receipt["incomplete_reasons"]))

    def test_cleanup_bound_exhaustion_is_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, _ = campaign_for(tmp, bounds=Bounds(max_total_response_bytes=900))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)

    def test_active_deadline_is_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = Clock()
            store = FakeStore()
            original = store.exchange

            def slow(*args):
                clock.now += 40
                return original(*args)

            store.exchange = slow
            run, _ = campaign_for(tmp, store=store, clock=clock, bounds=Bounds(active_seconds=100))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)
            self.assertTrue(any("time window" in reason for reason in receipt["incomplete_reasons"]))

    def test_a_window_overrun_by_a_fraction_of_a_second_is_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = Clock()
            store = FakeStore()
            original = store.exchange
            calls = []

            def slow(request, *args):
                calls.append(request.method)
                if request.method == "PUT":
                    clock.now += 3.34
                return original(request, *args)

            store.exchange = slow
            run, _ = campaign_for(tmp, store=store, clock=clock, bounds=Bounds(active_seconds=10))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)

    def test_failed_cleanup_records_leftovers(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, _ = campaign_for(tmp, store=FakeStore(fail_deletes=True))
            receipt = run.run("full", RUN)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)
            self.assertEqual(len(receipt["cleanup"]["failed"]), 6)  # two rounds of three
            self.assertEqual(len(receipt["cleanup"]["leftovers"]), 3)

    def test_recovery_deletes_what_an_interrupted_run_left(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, store = campaign_for(tmp, store=FakeStore(fail_deletes=True))
            run.run("full", RUN)
            store.fail_deletes = False
            recovery, _ = campaign_for(tmp, store=store)
            receipt = recovery.recover(run_dir(tmp))
            self.assertTrue(receipt["cleanup"]["verified_empty"], receipt)
            self.assertEqual(receipt["cleanup"]["deleted"], 3)

    def test_repeated_continuation_token_stops_a_walk(self):
        with tempfile.TemporaryDirectory() as tmp:
            cases = [Case("fake-02", "walk", "q", "walk", "prefix=" + quote(RUN) + "&walk=1")]
            run, _ = campaign_for(tmp, provider=FakeProvider(cases=cases), store=FakeStore(repeat_token=True, page=1))
            receipt = run.run("full", RUN)
            self.assertFalse(receipt["cases"]["fake-02"]["complete"])
            self.assertEqual(receipt["cases"]["fake-02"]["reason"], "repeated continuation token")

    def test_unavailable_version_is_skipped_and_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            cases = [Case("fake-01", "gate", "q", "exchange", "prefix=x", versions=("v1", "v2"), version_gate=True),
                     Case("fake-02", "after", "q", "exchange", "prefix=x", versions=("v1", "v2"))]
            store = FakeStore()
            original = store.exchange

            def versioned(request, *args):
                if ("x-ms-version", "v2") in request.headers:
                    return 400, [], b""
                return original(request, *args)

            store.exchange = versioned

            class Versioned(FakeProvider):
                def list_request(self, query, version, headers=()):
                    return Request("GET", "fake.invalid", "/list", query, (("x-ms-version", version or "v1"),))

            run, _ = campaign_for(tmp, provider=Versioned(cases=cases), store=store)
            receipt = run.run("full", RUN)
            self.assertFalse(receipt["cases"]["fake-02~v2"]["complete"])
            self.assertEqual(receipt["cases"]["fake-02~v1"]["status"], 200)
            self.assertEqual(receipt["status"], campaign.INCOMPLETE)

    def _sdk_run(self, tmp, result, returncode=0):
        (Path(tmp) / "cp.txt").write_text("classpath")
        cases = [Case("fake-09", "sdk", "q", "sdk_run", sdk={"walk": "flat", "max_attempts": 8})]
        seen = {}

        def runner(command, env, **kwargs):
            seen["env"], seen["timeout"] = env, kwargs["timeout"]
            seen["options"] = json.loads(Path(command[command.index("--options") + 1]).read_text())
            with open(command[command.index("--output") + 1], "w") as pipe:
                pipe.write(json.dumps(result))
            return subprocess.CompletedProcess(command, returncode, "private-account leaked to stdout", "")

        run, _ = campaign_for(tmp, provider=FakeProvider(cases=cases), sdk_runner=runner)
        return run.run("full", RUN), seen

    def test_sdk_attempts_are_charged_twice_and_identity_stays_out_of_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            step = {"phase": "walk", "exchange": {"captured_at": "t", "request": {
                "method": "GET", "url": "https://private-account.invalid/list", "headers": []},
                "response": {"status": 200, "headers": [], "body_base64": base64.b64encode(
                    b'{"items": [], "next": null}').decode()}}}
            receipt, seen = self._sdk_run(tmp, {"status": "COMPLETE", "storage_attempts": 2, "auth_attempts": 1,
                                                "response_bytes": 10, "steps": [step],
                                                "sdk": {"version": "1"}, "java_version": "25"})
            self.assertEqual(seen["options"]["maxAttempts"], 4)
            self.assertNotIn("SECRET", json.dumps(seen["options"]))
            self.assertEqual(seen["env"]["SECRET"], "value")
            self.assertLessEqual(seen["timeout"], campaign.SDK_MAX_SECONDS)
            self.assertEqual(receipt["cases"]["fake-09"]["charged_attempts"], 6)
            self.assertEqual(receipt["ledger"]["attempts_by_phase"]["sdk"], 6)
            self.assertNotIn("private-account", (run_dir(tmp) / "sdk" / "fake-09.log").read_text())
            self.assertNotIn("private-account", (run_dir(tmp) / "raw" / "fake-09.json").read_text())

    def test_untrusted_sdk_results_keep_the_full_allowance(self):
        for result, code in (({"status": "COMPLETE", "storage_attempts": 0, "auth_attempts": 0,
                               "response_bytes": 0, "steps": [], "sdk": {"version": "1"}}, 0),
                             ({"status": "COMPLETE", "storage_attempts": 1, "auth_attempts": 0,
                               "response_bytes": 0, "steps": [{}], "sdk": {"version": "1"}}, 7),
                             ({"status": "COMPLETE", "storage_attempts": 9, "auth_attempts": 0,
                               "response_bytes": 0, "steps": [{}], "sdk": {"version": "1"}}, 0)):
            with tempfile.TemporaryDirectory() as tmp:
                receipt, _ = self._sdk_run(tmp, result, code)
                self.assertEqual(receipt["status"], campaign.INCOMPLETE)
                self.assertEqual(receipt["ledger"]["attempts_by_phase"]["sdk"], 8)

    def test_corpus_over_caps_is_refused_before_any_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            run, store = campaign_for(tmp, provider=FakeProvider(objects=7), bounds=Bounds(max_objects=6))
            with self.assertRaises(BoundExceeded):
                run.run("full", RUN)
            self.assertEqual(store.sent, [])


class PublicationTest(unittest.TestCase):
    def test_redactor_covers_mixed_escapes(self):
        redactor = Redactor({"private-bucket": "{bucket}"})
        for spelling in ("private\\u002dbucket", "privat&#101;-bucket",
                         "PRIVATE&#45;bucket", "priv%61te-bucket", "private&#x2D;BUCKET"):
            self.assertEqual(redactor.text(spelling), "{bucket}", spelling)

    def test_redactor_covers_encoded_and_case_variants(self):
        redactor = Redactor({"Private-Account": "{account}", "a b/c": "{x}"})
        self.assertEqual(redactor.text("https://private-account.blob/a%20b%2Fc?q=a%20b/c"),
                         "https://{account}.blob/{x}?q={x}")

    def test_private_scan_decodes_bodies_and_rejects_credentials(self):
        campaign.scan_private({"name": "probe/x", "at": "p/at@x"}, ["private-account"])
        hidden = base64.b64encode(b'{"owner":"user-someone@example.com"}').decode()
        for leak in ({"body_base64": base64.b64encode(b"PRIVATE-ACCOUNT").decode()},
                     {"body_base64": hidden}, {"url": "https://x/?sv=1&sig=abc"},
                     {"h": "Bearer abc"}, {"u": "private%2Daccount"}, {"acl": "project-owners-123"}):
            with self.assertRaises(ValueError, msg=leak):
                campaign.scan_private(leak, ["private-account"])

    def test_cost_bound_at_full_caps_is_within_budget(self):
        for provider in ProviderMatrixTest.PROVIDERS:
            bound = campaign.cost_bound(provider.prices(), campaign.HARD_BOUNDS)
            self.assertLessEqual(bound["total_usd"], campaign.HARD_BOUNDS.max_cost_usd)


class EvidenceStorageTest(unittest.TestCase):
    def test_gzip_evidence_is_checksummed_over_its_decompressed_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = b'{"a":1}\n'
            path = Path(tmp) / "capture.json.gz"
            path.write_bytes(campaign.deterministic_gzip(payload))
            self.assertEqual(evidence.read_evidence(path), payload)

    def test_duplicate_query_probe_rows_match_the_java_receipt(self):
        source = (campaign.REPO_ROOT / "swath-replay/src/conformance/java/io/varve/swath/replay/"
                  "conformance/provider/ProviderMatchReceipt.java").read_text()
        declared = source.split("DUPLICATE_QUERY_PROBES = java.util.Set.of(", 1)[1].split(");", 1)[0]
        self.assertEqual({part.strip().strip('"') for part in declared.split(",")},
                         evidence.DUPLICATE_QUERY_PROBES)

    def test_duplicate_query_cases_are_exempt(self):
        for provider in ProviderMatrixTest.PROVIDERS:
            for case in provider.cases(RUN, "full"):
                names = [part.split("=", 1)[0] for part in case.query.split("&") if part]
                names += provider.selector_fields() if hasattr(provider, "selector_fields") else []
                if len(names) != len(set(names)):
                    self.assertIn(case.id, evidence.DUPLICATE_QUERY_PROBES)


class ProviderMatrixTest(unittest.TestCase):
    PROVIDERS = (gcs_live.GcsProvider("example-bucket-x9", "example-project-x9", "us-east1", None),
                 azure_live.AzureProvider("exampleaccountx9", "conformance", "sv=1&sig=x9x9", "eastus"),
                 s3_live.S3Provider("example-bucket-x9", "us-east-1", {"AWS_ACCESS_KEY_ID": "AKIDEXAMPLEX9",
                                                                     "AWS_SECRET_ACCESS_KEY": "secret-x9",
                                                                     "AWS_SESSION_TOKEN": "token-x9"}))

    def test_corpus_fits_caps_and_stays_inside_the_run_prefix(self):
        for provider in self.PROVIDERS:
            for profile in ("smoke", "full"):
                plan = campaign.build_plan(provider, campaign.HARD_BOUNDS, RUN, profile)
                self.assertLessEqual(plan["objects"], campaign.HARD_BOUNDS.max_objects)

    def test_token_sources_exist_and_kinds_match_required_rows(self):
        for provider in self.PROVIDERS:
            cases = provider.cases(RUN, "full")
            by_id = {case.id: case for case in cases}
            for case in cases:
                if case.token and case.token.case_id:
                    self.assertEqual(by_id[case.token.case_id].kind, "walk")
                required = evidence.REQUIRED_KINDS.get(case.id)
                if required:
                    self.assertEqual(case.kind, required, case.id)

    def test_azure_sas_is_validated_and_never_recorded(self):
        with self.assertRaises(ValueError):
            azure_live.SasAuthorizer("sv=1&unexpected=2&sig=x")
        provider = self.PROVIDERS[1]
        self.assertNotIn("sig", provider.list_request("prefix=a", "2026-06-06").recorded_url())
        sdk_case = next(case for case in provider.cases(RUN, "full") if case.kind == "sdk_run")
        self.assertNotIn("sig", json.dumps(provider.sdk_options(sdk_case, "2026-06-06")))

    def test_sigv4_matches_the_published_example_and_is_never_recorded(self):
        from datetime import datetime, timezone
        # AWS SigV4 documentation example: GET ?max-keys=2&prefix=J on examplebucket.
        signer = s3_live.SigV4Authorizer({"AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
                                          "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                                          "AWS_SESSION_TOKEN": ""}, "us-east-1",
                                         lambda: datetime(2013, 5, 24, tzinfo=timezone.utc))
        request = Request("GET", "examplebucket.s3.amazonaws.com", "/", "max-keys=2&prefix=J")
        headers = dict(signer.headers(PROBE, request))
        self.assertTrue(headers["Authorization"].endswith(
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
            "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"))
        self.assertEqual(s3_live.sigv4_query("prefix=J&max-keys=2&a=b+c%2b"), "a=b%2Bc%2B&max-keys=2&prefix=J")
        self.assertNotIn("Authorization", request.recorded_url())

    def test_creates_never_overwrite_and_deletes_are_conditional(self):
        gcs, azure, s3 = self.PROVIDERS
        self.assertIn(("If-None-Match", "*"), s3.upload(s3.corpus(RUN, "full")[0]).headers)
        self.assertIn("ifGenerationMatch=0", gcs.upload(gcs.corpus(RUN, "full")[0]).query)
        self.assertEqual(gcs.delete(RUN + "a", "17").query, "ifGenerationMatch=17")
        self.assertIn(("If-None-Match", "*"), azure.upload(azure.corpus(RUN, "full")[0]).headers)
        self.assertIn(("If-Match", '"0x1"'), azure.delete(RUN + "a", '"0x1"').headers)

    def test_token_mutations_keep_outer_encoding(self):
        self.assertEqual(matrix.mutate_token("abcA", "flip"), "abcB")
        self.assertEqual(matrix.mutate_token("abcdefgh", "truncate"), "abcd")
        with self.assertRaises(ValueError):
            TokenSource()


if __name__ == "__main__":
    unittest.main()
