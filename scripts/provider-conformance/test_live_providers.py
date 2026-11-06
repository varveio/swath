# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Offline checks for the GCS and Azure live adapters' credential handling."""

import os
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

import azure_live
import campaign
import gcs_live
from campaign import PROBE, Request


class FakeCredentials:
    """google-auth user credentials: expiry is a naive UTC datetime, as google-auth keeps it."""

    def __init__(self, expires_in: timedelta):
        self.token = "token-x9"
        self.expiry = datetime.now(timezone.utc).replace(tzinfo=None) + expires_in
        self.refreshes = 0

    def refresh(self, transport):
        self.refreshes += 1
        self.expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1)


def authorizer(credentials: FakeCredentials) -> gcs_live.GcsAuthorizer:
    # The real constructor loads a credentials file through google-auth; only the refresh
    # decision is under test here.
    authorizer = gcs_live.GcsAuthorizer.__new__(gcs_live.GcsAuthorizer)
    authorizer.credentials = credentials
    authorizer.transport = gcs_live.GatedAuthRequest(None)
    authorizer.lock = threading.Lock()
    return authorizer


@unittest.skipUnless(hasattr(time, "tzset"), "needs time.tzset")
class GcsTokenExpiryTest(unittest.TestCase):
    def setUp(self):
        self.saved = os.environ.get("TZ")

    def tearDown(self):
        if self.saved is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self.saved
        time.tzset()

    def zone(self, posix_tz: str):
        os.environ["TZ"] = posix_tz
        time.tzset()

    def test_valid_token_is_not_refreshed_east_of_utc(self):
        self.zone("XST-9")  # UTC+9
        credentials = FakeCredentials(timedelta(hours=1))
        request = Request("GET", gcs_live.HOST, "/storage/v1/b/x/o", "")
        headers = authorizer(credentials).headers(PROBE, request)
        self.assertEqual(credentials.refreshes, 0)
        self.assertEqual(headers, [("Authorization", "Bearer token-x9")])

    def test_expiring_token_is_refreshed_west_of_utc(self):
        self.zone("XST8")  # UTC-8
        credentials = FakeCredentials(timedelta(seconds=gcs_live.REFRESH_MARGIN_SECONDS - 60))
        authorizer(credentials).headers(PROBE, Request("GET", gcs_live.HOST, "/storage/v1/b/x/o", ""))
        self.assertEqual(credentials.refreshes, 1)


class AzureSasPrefixTest(unittest.TestCase):
    SAS = "sv=2026-06-06&sr=c&sp=rl&sig=x9x9sig"

    def test_leading_question_mark_is_accepted_and_stripped_everywhere(self):
        self.assertEqual(azure_live.SasAuthorizer("?" + self.SAS).credential_query(), self.SAS)
        provider = azure_live.AzureProvider("exampleaccountx9", "conformance", "?" + self.SAS, "eastus")
        self.assertEqual(provider.sas, self.SAS)
        self.assertEqual(provider.authorizer(None).credential_query(), self.SAS)
        self.assertIn(self.SAS, provider.redactions())
        self.assertNotIn("?" + self.SAS, provider.redactions())
        self.assertIn(self.SAS, provider.private_identifiers())
        self.assertEqual(provider.sdk_environment()["AZURE_CONTAINER_SAS"], self.SAS)
        redactor = campaign.Redactor(provider.redactions())
        for spelling in ("https://h/c?" + self.SAS, "https://h/c?restype=container&" + self.SAS):
            self.assertNotIn("x9x9sig", redactor.text(spelling))
            self.assertIn("{sas}", redactor.text(spelling))

    def test_question_mark_alone_is_still_refused(self):
        with self.assertRaises(ValueError):
            azure_live.SasAuthorizer("?")
        with self.assertRaises(ValueError):
            azure_live.AzureProvider("exampleaccountx9", "conformance", "?", "eastus")


if __name__ == "__main__":
    unittest.main()
