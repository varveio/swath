#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Bounded live listing-conformance campaign: provision, probe, capture, clean up.

The core is provider-neutral. A provider adapter supplies the corpus, request builders,
case matrix and price table; this module owns every guard:

* hard caps that can only be tightened, checked before any request is sent;
* campaign-wide attempt, response-byte and time accounting persisted before each send,
  under an exclusive campaign lock, with a cleanup reserve setup and probes cannot touch;
* bounded concurrency, an absolute per-request deadline, and a sticky record of any
  exhausted bound so a run can never be reported COMPLETE afterwards;
* exclusive ownership of an empty, randomly named run prefix before anything is created,
  and cleanup that deletes only names listed inside that prefix, conditionally on the
  listed generation or ETag, then verifies an empty namespace;
* redaction of every provider identity and credential before anything is written, and a
  publication scan over decoded bodies before evidence leaves the private run directory.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import fcntl
import hashlib
import html
import http.client
import json
import math
import os
import re
import secrets
import socket
import ssl
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Protocol
from urllib.parse import quote, unquote
from xml.etree.ElementTree import ParseError as ElementTreeError

import evidence
from matrix import (Case, CorpusObject, PriceTable, RUN_PREFIX, RUN_PREFIX_ROOT, Untested, case_record,
                    mutate_token)

MIB = 1024 * 1024
REPO_ROOT = evidence.REPO_ROOT

# Phases. Setup, probes and SDK walks share the active window and may not touch the
# cleanup reserve; cleanup has its own window and may use the whole attempt cap.
SETUP, PROBE, SDK, CLEANUP = "setup", "probe", "sdk", "cleanup"
ACTIVE_PHASES = frozenset({SETUP, PROBE, SDK})
WINDOW_ACTIVE, WINDOW_CLEANUP = "active", "cleanup"
COMPLETE, INCOMPLETE = "COMPLETE", "INCOMPLETE"
RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})
# The JDK can resend a request below the SDK capture gate (stale keep-alive connections);
# every attempt the SDK driver reports is charged this many times.
SDK_RESEND_FACTOR = 2
SDK_RESPONSE_BYTE_ALLOWANCE = 32 * MIB
SDK_MAX_SECONDS = 300.0
CLEANUP_ROUNDS = 2
CLEANUP_DELETE_ATTEMPTS = 1
# Share of the response-byte budget setup and probes leave for cleanup's listings
# (64 MiB at the hard cap).
CLEANUP_BYTE_SHARE = 8
HEARTBEAT_SECONDS = 5.0
# A pooled connection idle longer than this is replaced before use: providers close idle
# keep-alive connections, and a reused dead socket would fail a request it never sent.
IDLE_RECONNECT_SECONDS = 2.0
# Provisioning starts only if the remaining cleanup window could delete the whole corpus at
# this conservative rate (about a tenth of the observed concurrent delete rate) plus listings.
CLEANUP_SECONDS_PER_OBJECT = 0.1
CLEANUP_LISTING_SECONDS = 60.0
INTEGER_BOUNDS = frozenset({"max_objects", "max_payload_bytes", "max_attempts", "cleanup_reserve", "concurrency",
                            "max_walk_pages", "max_response_bytes", "max_total_response_bytes"})


class BoundExceeded(Exception):
    """A campaign bound refused further work. The run becomes INCOMPLETE."""


class TransportFailure(Exception):
    """An attempt was made and counted, but no complete HTTP response was obtained."""


HARD_CAPS = {"max_objects": 6000, "max_payload_bytes": 16 * MIB, "max_attempts": 25_000,
             "cleanup_reserve": 7_500, "concurrency": 4, "request_seconds": 30.0,
             "active_seconds": 30 * 60.0, "cleanup_seconds": 15 * 60.0, "max_walk_pages": 64,
             "max_response_bytes": 8 * MIB, "max_total_response_bytes": 512 * MIB, "max_cost_usd": 2.50}
# The cleanup reserve must stay at least this share of the attempt cap.
RESERVE_SHARE = HARD_CAPS["cleanup_reserve"] / HARD_CAPS["max_attempts"]


@dataclass(frozen=True)
class Bounds:
    """Campaign caps. Any instance is validated against HARD_CAPS: values can only tighten."""

    max_objects: int = HARD_CAPS["max_objects"]
    max_payload_bytes: int = HARD_CAPS["max_payload_bytes"]
    max_attempts: int = HARD_CAPS["max_attempts"]
    cleanup_reserve: int = HARD_CAPS["cleanup_reserve"]
    concurrency: int = HARD_CAPS["concurrency"]
    request_seconds: float = HARD_CAPS["request_seconds"]
    active_seconds: float = HARD_CAPS["active_seconds"]
    cleanup_seconds: float = HARD_CAPS["cleanup_seconds"]
    max_walk_pages: int = HARD_CAPS["max_walk_pages"]
    max_response_bytes: int = HARD_CAPS["max_response_bytes"]
    max_total_response_bytes: int = HARD_CAPS["max_total_response_bytes"]
    max_cost_usd: float = HARD_CAPS["max_cost_usd"]

    def __post_init__(self) -> None:
        for name, hard in HARD_CAPS.items():
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) \
                    or value <= 0 or (name in INTEGER_BOUNDS and not isinstance(value, int)):
                raise ValueError(f"bound {name} must be a positive finite number of the right type")
            if name != "cleanup_reserve" and value > hard:
                raise ValueError(f"bound {name}={value} relaxes the hard cap {hard}")
        if self.cleanup_reserve < math.ceil(self.max_attempts * RESERVE_SHARE) \
                or self.cleanup_reserve >= self.max_attempts:
            raise ValueError("cleanup reserve must keep its share and leave room for setup")

    def tightened(self, **overrides: float) -> "Bounds":
        return dataclasses.replace(self, **overrides)


HARD_BOUNDS = Bounds()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# Redaction and publication checks ---------------------------------------------------


class Redactor:
    """Replaces private identifiers and credentials, including encoded spellings."""

    def __init__(self, mapping: dict[str, str]):
        # Each character may appear literally, percent-encoded, as a JSON \\u escape or as an
        # XML character reference, in any mixture; letters match in either case.
        ordered = sorted((real for real in mapping if real), key=len, reverse=True)
        self.patterns = [(re.compile("".join(self._char(c) for c in real), re.IGNORECASE), mapping[real])
                         for real in ordered]
        self.byte_patterns = [(re.compile(pattern.pattern.encode("utf-8"), re.IGNORECASE),
                               placeholder.encode("utf-8")) for pattern, placeholder in self.patterns]

    @staticmethod
    def _char(c: str) -> str:
        if ord(c) > 0x7F:
            return re.escape(c)
        code = ord(c)
        forms = [re.escape(c), f"%{code:02x}", f"\\\\u{code:04x}", f"&#0*{code};", f"&#x0*{code:x};"]
        return "(?:" + "|".join(forms) + ")"

    def text(self, value: str) -> str:
        for pattern, placeholder in self.patterns:
            value = pattern.sub(lambda _: placeholder, value)
        return value

    def data(self, value: bytes) -> bytes:
        for pattern, placeholder in self.byte_patterns:
            value = pattern.sub(lambda _: placeholder, value)
        return value

    def exchange(self, raw: dict) -> dict:
        request, response = raw["request"], raw["response"]
        body = base64.b64decode(response["body_base64"])
        return {**raw,
                "request": {**request, "url": self.text(request["url"]),
                            "headers": [{"name": h["name"], "value": self.text(h["value"])}
                                        for h in request["headers"]]},
                "response": {**response,
                             "headers": [{"name": h["name"], "value": self.text(h["value"])}
                                         for h in response["headers"]],
                             "body_base64": base64.b64encode(self.data(body)).decode("ascii")}}


EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
PROJECT_ENTITY = re.compile(r"project-(?:owners|editors|viewers)-[0-9]+")
CREDENTIAL = re.compile(r"(?<![a-z])sig=|bearer |ya29\.|refresh_token|client_secret|"
                        r"(?<![a-z])(?:sv|se|sp|spr)=[^&\s]*&|arn:aws[a-z-]*:|"
                        r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|x-amz-security-token|AWS4-HMAC-SHA256", re.IGNORECASE)


def publishable_texts(value: object, key: str | None = None) -> Iterator[str]:
    """Every string in a capture, with base64 bodies decoded and percent escapes undone."""
    if isinstance(value, dict):
        for child_key, child in value.items():
            yield from publishable_texts(child_key)
            yield from publishable_texts(child, child_key)
    elif isinstance(value, list):
        for child in value:
            yield from publishable_texts(child, key)
    elif isinstance(value, str):
        texts = [value]
        if key in {"body_base64", "body_utf8"}:
            body = base64.b64decode(value).decode("utf-8", "replace") if key == "body_base64" else value
            texts = [body, html.unescape(body)]
            try:
                texts += list(publishable_texts(json.loads(body)))
            except ValueError:
                pass
        for text in texts:
            # Undo nested percent and entity escaping until the text stops changing.
            for _ in range(4):
                yield text
                decoded = html.unescape(unquote(text, errors="replace"))
                if decoded == text:
                    break
                text = decoded


def scan_private(document: object, identifiers: Iterable[str]) -> None:
    """Refuse publication if any identifier, credential, email or ACL entity remains."""
    texts = list(publishable_texts(document)) if not isinstance(document, (bytes, str)) else \
        [document.decode("utf-8", "replace") if isinstance(document, bytes) else document]
    lowered = [text.lower() for text in texts]
    for identifier in identifiers:
        if identifier and any(identifier.lower() in text for text in lowered):
            raise ValueError("a private identifier remained in publishable evidence")
    for text in texts:
        if CREDENTIAL.search(text) or EMAIL.search(text) or PROJECT_ENTITY.search(text):
            raise ValueError("credential, email or ACL entity material remained in publishable evidence")


# Campaign ledger --------------------------------------------------------------------


class CampaignLedger:
    """Campaign-wide accounting persisted as append-only, fsynced JSON lines.

    An exclusive lock keeps a second process from spending the same budget. Every attempt
    is written before it is sent, so a crash can only over-count. A crash-left window is
    charged through the later of its last line and any planned subprocess deadline; a torn
    final line is charged as one more attempt.
    """

    def __init__(self, path: Path, bounds: Bounds, clock: Callable[[], float] = time.time):
        self.path = path
        self.bounds = bounds
        self.clock = clock
        self.lock = threading.Lock()
        self.attempts = 0
        self.response_bytes = 0
        self.window_used = {WINDOW_ACTIVE: 0.0, WINDOW_CLEANUP: 0.0}
        self.open_window: tuple[str, float] | None = None
        self.by_phase: dict[str, int] = {}
        # Campaign-wide caps (attempts, windows, bytes) close the campaign; per-request caps make
        # only the run that hit them INCOMPLETE.
        self.exhausted: list[str] = []
        self.run_exhausted: list[str] = []
        self._seq = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = (path.parent / (path.name + ".lock")).open("w")
        try:
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self._lock_file.close()
            raise BoundExceeded("another process holds this campaign's ledger") from error
        self._load()
        self._file = path.open("a", encoding="utf-8")

    def _load(self) -> None:
        if not self.path.exists():
            return
        open_windows: dict[str, float] = {}
        last_time = 0.0
        raw = self.path.read_text(encoding="utf-8")
        if raw and not raw.endswith("\n"):
            # A torn final record: cut it so appends stay parseable and charge it as a send
            # through a durable record, so later restarts keep the charge.
            raw = raw[:raw.rfind("\n") + 1]
            with self.path.open("r+", encoding="utf-8") as handle:
                handle.truncate(len(raw.encode("utf-8")))
            charge = json.dumps({"kind": "torn_record_charged", "t": self.clock()}) + "\n"
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(charge)
                handle.flush()
                os.fsync(handle.fileno())
            raw += charge
        for line in raw.split("\n")[:-1]:
            entry = json.loads(line)
            last_time = max(last_time, entry.get("t", 0.0), entry.get("planned_until", 0.0))
            kind = entry["kind"]
            if kind == "send":
                self._count(entry["phase"], 1)
            elif kind == "torn_record_charged":
                self._count("unknown", 1)
            elif kind == "allowance":
                self._count(entry["phase"], entry["count"])
            elif kind == "settle":
                self._count(entry["phase"], -entry["refund"])
            elif kind == "bytes":
                self.response_bytes += entry["count"]
            elif kind == "exhausted" and entry["reason"] not in RUN_SCOPED_BOUNDS:
                self.exhausted.append(entry["reason"])
            elif kind == "window_open":
                for window, start in list(open_windows.items()):
                    self.window_used[window] += max(0.0, entry["t"] - start)
                    del open_windows[window]
                open_windows[entry["window"]] = entry["t"]
            elif kind == "window_close":
                start = open_windows.pop(entry["window"], None)
                if start is not None:
                    self.window_used[entry["window"]] += entry["t"] - start
            self._seq = max(self._seq, entry.get("seq", 0))
        for window, start in open_windows.items():
            # The crash happened after the last durable line, at most one heartbeat later.
            self.window_used[window] += max(0.0, last_time - start) + HEARTBEAT_SECONDS

    def _count(self, phase: str, count: int) -> None:
        self.attempts += count
        self.by_phase[phase] = self.by_phase.get(phase, 0) + count

    def _write(self, entry: dict) -> None:
        entry.setdefault("t", self.clock())
        self._file.write(json.dumps(entry, sort_keys=True) + "\n")
        self._file.flush()
        os.fsync(self._file.fileno())

    def close(self) -> None:
        if getattr(self, "_heartbeat", None) is not None:
            self._stop.set()
            self._heartbeat.join()
            self._heartbeat = None
        with self.lock:
            if self._file.closed:
                return
            if self.open_window is not None:
                self._close_window_locked()
            self._file.close()
            fcntl.flock(self._lock_file, fcntl.LOCK_UN)
            self._lock_file.close()

    def _refuse_locked(self, reason: str) -> BoundExceeded:
        scoped = self.run_exhausted if reason in RUN_SCOPED_BOUNDS else self.exhausted
        if reason not in scoped:
            scoped.append(reason)
            self._write({"kind": "exhausted", "reason": reason})
        return BoundExceeded(reason)

    def any_exhausted(self) -> list[str]:
        return self.exhausted + self.run_exhausted

    def mark_exhausted(self, reason: str) -> None:
        with self.lock:
            self._refuse_locked(reason)

    # Time windows -----------------------------------------------------------------

    def open(self, window: str) -> None:
        with self.lock:
            if self.open_window is not None:
                self._close_window_locked()
            self._write({"kind": "window_open", "window": window})
            self.open_window = (window, self.clock())
            if getattr(self, "_heartbeat", None) is None:
                # A crash leaves at most one heartbeat interval of window time uncharged.
                self._stop = threading.Event()
                self._heartbeat = threading.Thread(target=self._beat, daemon=True)
                self._heartbeat.start()

    def _beat(self) -> None:
        while not self._stop.wait(HEARTBEAT_SECONDS):
            with self.lock:
                if self._file.closed:
                    return
                if self.open_window is not None:
                    self._write({"kind": "heartbeat"})

    def _close_window_locked(self) -> None:
        window, start = self.open_window
        self._write({"kind": "window_close", "window": window})
        self.window_used[window] += self.clock() - start
        self.open_window = None

    def used_seconds(self, window: str) -> float:
        with self.lock:
            used = self.window_used[window]
            if self.open_window is not None and self.open_window[0] == window:
                used += self.clock() - self.open_window[1]
            return used

    def window_limit(self, window: str) -> float:
        return self.bounds.active_seconds if window == WINDOW_ACTIVE else self.bounds.cleanup_seconds

    def remaining_seconds(self, phase: str) -> float:
        window = WINDOW_ACTIVE if phase in ACTIVE_PHASES else WINDOW_CLEANUP
        if self.open_window is None or self.open_window[0] != window:
            raise BoundExceeded(f"{phase} attempted outside its {window} window")
        used = self.window_used[window] + self.clock() - self.open_window[1]
        return self.window_limit(window) - used

    # Attempts and bytes -----------------------------------------------------------

    def _ceiling(self, phase: str) -> int:
        return self.bounds.max_attempts - (self.bounds.cleanup_reserve if phase in ACTIVE_PHASES else 0)

    def _check_locked(self, phase: str, count: int) -> None:
        if self.remaining_seconds(phase) <= 0:
            raise self._refuse_locked(f"{phase} time window exhausted")
        if self.attempts + count > self._ceiling(phase):
            raise self._refuse_locked(f"{phase} attempt budget exhausted at {self.attempts} of "
                                      f"{self._ceiling(phase)} available to this phase")

    def acquire(self, phase: str, method: str, target: str) -> int:
        with self.lock:
            self._check_locked(phase, 1)
            self._seq += 1
            self._count(phase, 1)
            self._write({"kind": "send", "seq": self._seq, "phase": phase, "method": method,
                         "target": target})
            return self._seq

    def outcome(self, seq: int, status: int | None, error: str | None, elapsed: float) -> None:
        with self.lock:
            self._write({"kind": "outcome", "seq": seq, "status": status, "error": error,
                         "elapsed": round(elapsed, 4)})

    def charge_bytes(self, count: int, phase: str = CLEANUP) -> None:
        """Bytes already read are always recorded; reading stops once the phase's share is spent."""
        with self.lock:
            self.response_bytes += count
            self._write({"kind": "bytes", "count": count})
            total = self.bounds.max_total_response_bytes
            ceiling = total - (total // CLEANUP_BYTE_SHARE if phase in ACTIVE_PHASES else 0)
            if self.response_bytes > ceiling:
                raise self._refuse_locked(f"{phase} response-byte budget exhausted")

    def allowance(self, phase: str, wanted: int, planned_until: float) -> int:
        """Reserve up to `wanted` attempts for a subprocess; charged in full until settled."""
        with self.lock:
            granted = min(wanted, self._ceiling(phase) - self.attempts)
            if granted <= 0:
                raise self._refuse_locked(f"{phase} attempt budget exhausted")
            self._check_locked(phase, granted)
            self._seq += 1
            self._count(phase, granted)
            self._write({"kind": "allowance", "seq": self._seq, "phase": phase, "count": granted,
                         "planned_until": planned_until})
            return granted

    def settle(self, phase: str, granted: int, used: int) -> None:
        if not 0 <= used <= granted:
            raise ValueError("subprocess attempts outside its allowance")
        with self.lock:
            self._count(phase, used - granted)
            self._write({"kind": "settle", "phase": phase, "refund": granted - used})

    def refund_bytes(self, count: int) -> None:
        with self.lock:
            self.response_bytes -= count
            self._write({"kind": "bytes", "count": -count})

    def summary(self) -> dict:
        with self.lock:
            used = dict(self.window_used)
            if self.open_window is not None:
                used[self.open_window[0]] += self.clock() - self.open_window[1]
            return {"attempts": self.attempts, "attempts_by_phase": dict(sorted(self.by_phase.items())),
                    "response_bytes": self.response_bytes, "exhausted": list(self.exhausted),
                    "run_exhausted": list(self.run_exhausted),
                    "active_seconds": round(used[WINDOW_ACTIVE], 1),
                    "cleanup_seconds": round(used[WINDOW_CLEANUP], 1),
                    "bounds": dataclasses.asdict(self.bounds)}


# HTTP -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Request:
    """One provider request. `query` is the exact raw wire spelling without credentials."""

    method: str
    host: str
    path: str
    query: str = ""
    headers: tuple[tuple[str, str], ...] = ()
    body: bytes = b""
    scheme: str = "https"
    port: int | None = None

    def recorded_url(self) -> str:
        origin = f"{self.scheme}://{self.host}" + (f":{self.port}" if self.port else "")
        return origin + self.path + ("?" + self.query if self.query else "")

    def target(self) -> str:
        return self.path + ("?" + self.query if self.query else "")


@dataclass(frozen=True)
class Response:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    captured_at: str

    def header(self, name: str) -> str | None:
        values = [value for key, value in self.headers if key.lower() == name.lower()]
        return values[0] if len(values) == 1 else None


class Authorizer(Protocol):
    def headers(self, phase: str, request: "Request") -> list[tuple[str, str]]: ...
    def credential_query(self) -> str: ...


class NoAuthorizer:
    def headers(self, phase: str, request: "Request") -> list[tuple[str, str]]:
        return []

    def credential_query(self) -> str:
        return ""


class Connector(Protocol):
    def exchange(self, request: Request, credential_query: str, extra_headers: list[tuple[str, str]],
                 deadline: float, max_bytes: int,
                 on_bytes: Callable[[int], None]) -> tuple[int, list[tuple[str, str]], bytes]: ...


class HttpConnector:
    """Keep-alive HTTP/1.1 per thread, raw target spelling, and a watchdog that aborts the
    connection at the absolute request deadline, whatever blocking call is in progress."""

    def __init__(self) -> None:
        self.local = threading.local()
        self.tls = ssl.create_default_context()

    def _connection(self, request: Request, timeout: float) -> http.client.HTTPConnection:
        pool = getattr(self.local, "pool", None)
        if pool is None:
            pool = self.local.pool = {}
        key = (request.scheme, request.host, request.port)
        connection = pool.get(key)
        last_used = getattr(self.local, "last_used", {}).get(key, 0.0)
        if connection is not None and time.monotonic() - last_used > IDLE_RECONNECT_SECONDS:
            connection.close()
            connection = None
        if connection is None:
            connection = http.client.HTTPSConnection(request.host, request.port, timeout=timeout,
                                                     context=self.tls) \
                if request.scheme == "https" else http.client.HTTPConnection(request.host, request.port,
                                                                             timeout=timeout)
            pool[key] = connection
        connection.timeout = timeout
        if connection.sock is not None:
            connection.sock.settimeout(timeout)
        return connection

    def _drop(self, request: Request) -> None:
        connection = getattr(self.local, "pool", {}).pop((request.scheme, request.host, request.port), None)
        if connection is not None:
            connection.close()

    def exchange(self, request, credential_query, extra_headers, deadline, max_bytes, on_bytes):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TransportFailure("request deadline passed before send")
        target = request.path
        query = "&".join(part for part in (request.query, credential_query) if part)
        if query:
            target += "?" + query
        connection = self._connection(request, remaining)
        fired = threading.Event()

        def abort() -> None:
            fired.set()
            sock = connection.sock
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        watchdog = threading.Timer(remaining, abort)
        watchdog.daemon = True
        watchdog.start()
        try:
            if connection.sock is None:
                connection.connect()
            if fired.is_set() or time.monotonic() >= deadline:
                raise TransportFailure("request exceeded its total time cap")
            connection.putrequest(request.method, target, skip_accept_encoding=True)
            for name, value in [*request.headers, *extra_headers]:
                connection.putheader(name, value)
            if request.body or request.method in {"PUT", "POST"}:
                connection.putheader("Content-Length", str(len(request.body)))
            connection.endheaders(request.body or None)
            response = connection.getresponse()
            chunks = []
            total = 0
            while request.method != "HEAD":
                chunk = response.read1(65536)
                if not chunk:
                    break
                total += len(chunk)
                on_bytes(len(chunk))
                if total > max_bytes:
                    raise BoundExceeded(PER_RESPONSE_CAP)
                chunks.append(chunk)
            if fired.is_set() or time.monotonic() > deadline:
                raise TransportFailure("request exceeded its total time cap")
            if response.length:
                raise TransportFailure("response ended before its declared Content-Length")
            if response.will_close:
                self._drop(request)
            else:
                if not hasattr(self.local, "last_used"):
                    self.local.last_used = {}
                self.local.last_used[(request.scheme, request.host, request.port)] = time.monotonic()
            return response.status, list(response.getheaders()), b"".join(chunks)
        except BoundExceeded:
            self._drop(request)
            raise
        except (OSError, http.client.HTTPException, TransportFailure) as error:
            self._drop(request)
            if fired.is_set():
                raise TransportFailure("request exceeded its total time cap") from error
            if isinstance(error, TransportFailure):
                raise
            raise TransportFailure(type(error).__name__) from error
        finally:
            watchdog.cancel()


REQUEST_TIME_CAP = "request exceeded its total time cap"
PER_RESPONSE_CAP = "response exceeded the per-response byte cap"
RUN_SCOPED_BOUNDS = frozenset({REQUEST_TIME_CAP, PER_RESPONSE_CAP})


class HttpClient:
    """Every provider HTTP attempt in a campaign, including credential refreshes, goes
    through `send`: counted before it is sent, deadline-bound and byte-charged while read."""

    def __init__(self, ledger: CampaignLedger, connector: Connector, redactor: Redactor,
                 sleep: Callable[[float], None] = time.sleep):
        self.ledger = ledger
        self.connector = connector
        self.redactor = redactor
        self.authorizer: Authorizer = NoAuthorizer()
        self.permits = threading.BoundedSemaphore(ledger.bounds.concurrency)
        self.sleep = sleep

    def send(self, request: Request, phase: str, authorize: bool = True, nested: bool = False) -> Response:
        """`nested` marks a credential refresh made while its caller already holds a permit."""
        if nested:
            return self._send(request, phase, authorize)
        with self.permits:
            return self._send(request, phase, authorize)

    def _send(self, request: Request, phase: str, authorize: bool) -> Response:
        seq = self.ledger.acquire(phase, request.method, self.redactor.text(request.target()))
        started = time.monotonic()
        try:
            extra = self.authorizer.headers(phase, request) if authorize else []
            deadline = started + min(self.ledger.bounds.request_seconds, self.ledger.remaining_seconds(phase))
            credential = self.authorizer.credential_query() if authorize else ""
            status, headers, body = self.connector.exchange(
                request, credential, extra, deadline, self.ledger.bounds.max_response_bytes,
                lambda count: self.ledger.charge_bytes(count, phase))
        except BoundExceeded as error:
            self.ledger.outcome(seq, None, str(error), time.monotonic() - started)
            self.ledger.mark_exhausted(str(error))
            raise
        except TransportFailure as error:
            self.ledger.outcome(seq, None, self.redactor.text(str(error)), time.monotonic() - started)
            if str(error) == REQUEST_TIME_CAP:
                # Hitting the per-request cap is an exhausted bound, never retried into success.
                self.ledger.mark_exhausted(REQUEST_TIME_CAP)
                raise BoundExceeded(REQUEST_TIME_CAP) from error
            raise
        self.ledger.outcome(seq, status, None, time.monotonic() - started)
        return Response(status, tuple(headers), body, utc_now())

    def send_with_retry(self, request: Request, phase: str, attempts: int = 3) -> Response:
        """Bounded retry for provisioning and cleanup; semantic probes never retry."""
        delay = 0.5
        for attempt in range(1, attempts + 1):
            try:
                response = self.send(request, phase)
                if response.status not in RETRYABLE_STATUS or attempt == attempts:
                    return response
            except TransportFailure:
                if attempt == attempts:
                    raise
            self.sleep(delay)
            delay *= 2
        raise AssertionError("unreachable")


def raw_exchange(request: Request, response: Response, redactor: Redactor,
                 scrub: Callable[[bytes], bytes] = lambda body: body) -> dict:
    """The raw exchange shape evidence.py sanitizes, redacted before it is ever written."""
    response = dataclasses.replace(response, body=scrub(response.body))
    return redactor.exchange({
        "captured_at": response.captured_at,
        "request": {"method": request.method, "url": request.recorded_url(),
                    "headers": [{"name": n, "value": v} for n, v in request.headers]},
        "response": {"status": response.status,
                     "headers": [{"name": n, "value": v} for n, v in response.headers],
                     "body_base64": base64.b64encode(response.body).decode("ascii")}})


# Provider contract ----------------------------------------------------------------


@dataclass(frozen=True)
class Listed:
    """One listing entry: an object (name, size, updated, version) or a rolled-up prefix."""

    name: str
    prefix: bool = False
    size: int | None = None
    updated: str | None = None
    version: str | None = None
    # Provider metadata a replay fixture reproduces exactly (S3 ETag, storage class, checksum).
    attributes: tuple[tuple[str, str], ...] = ()


class Provider(Protocol):
    name: str
    # API versions every case runs under. Each version is evidence-gated separately.
    api_versions: tuple[str, ...]

    def redactions(self) -> dict[str, str]: ...
    def private_identifiers(self) -> list[str]: ...
    def authorizer(self, client: HttpClient) -> Authorizer: ...
    def corpus(self, run_prefix: str, profile: str) -> list[CorpusObject]: ...
    def cases(self, run_prefix: str, profile: str) -> list[Case]: ...
    def untested(self) -> list[Untested]: ...
    def prices(self) -> PriceTable: ...
    def upload(self, item: CorpusObject) -> Request: ...
    def created(self, response: Response) -> bool: ...
    def delete(self, name: str, version: str | None) -> Request | None: ...
    def deleted(self, response: Response) -> bool: ...
    def list_request(self, query: str, version: str | None, headers: tuple = ()) -> Request: ...
    def with_token(self, query: str, token: str) -> str: ...
    def page(self, body: bytes) -> tuple[list[Listed], str | None]: ...
    def inventory_query(self, prefix: str) -> str: ...
    def version_echoed(self, version: str, response: Response) -> bool: ...
    def version_unavailable(self, response: Response) -> bool: ...
    def manifest_fields(self) -> dict: ...
    def sdk_options(self, case: Case, version: str | None) -> dict: ...
    def sdk_environment(self) -> dict[str, str]: ...
    def sdk_identity(self) -> dict[str, str]: ...
    def sanitize_names(self) -> dict[str, str]: ...
    def canonical_name(self, name: str) -> str: ...
    def raw_wire(self, raw: dict, manifest_sha256: str, probe_id: str, capture_sha256: str) -> bytes | None: ...
    def scrub(self, body: bytes) -> bytes: ...


def cost_bound(prices: PriceTable, bounds: Bounds) -> dict:
    """Conservative charge ceiling at the full caps, before any request is sent."""
    attempts = bounds.max_attempts * prices.per_attempt_usd
    storage = bounds.max_payload_bytes / (1024 ** 3) * prices.storage_gib_month_usd
    egress = bounds.max_total_response_bytes / (1024 ** 3) * prices.egress_gib_usd
    return {"source": prices.source, "retrieved": prices.retrieved,
            "attempts_usd": round(attempts, 6), "storage_usd": round(storage, 6),
            "egress_usd": round(egress, 6), "total_usd": round(attempts + storage + egress, 6),
            "assumptions": "every attempt billed at the most expensive operation class; "
                           "full payload stored for a whole month; every permitted response "
                           "byte billed at the highest internet egress tier"}


def actual_cost(prices: PriceTable, summary: dict, payload_bytes: int) -> dict:
    attempts = summary["attempts"] * prices.per_attempt_usd
    storage = payload_bytes / (1024 ** 3) * prices.storage_gib_month_usd
    egress = summary["response_bytes"] / (1024 ** 3) * prices.egress_gib_usd
    return {"attempts_usd": round(attempts, 6), "storage_usd": round(storage, 6),
            "egress_usd": round(egress, 6), "total_usd": round(attempts + storage + egress, 6),
            "basis": "campaign-wide ledger totals at the conservative price table"}


def new_run_prefix(clock: Callable[[], float] = time.time) -> str:
    stamp = datetime.fromtimestamp(clock(), timezone.utc).strftime("r%Y%m%dt%H%M%Sz")
    prefix = f"{RUN_PREFIX_ROOT}{stamp}-{secrets.token_hex(4)}/"
    assert RUN_PREFIX.fullmatch(prefix)
    return prefix


def require_owned(name: str, run_prefix: str) -> None:
    if not RUN_PREFIX.fullmatch(run_prefix):
        raise ValueError("run prefix is not an isolated campaign prefix")
    if not name.startswith(run_prefix) or len(name) == len(run_prefix):
        raise ValueError("refusing a name outside this run's isolated prefix")


def build_plan(provider: Provider, bounds: Bounds, run_prefix: str, profile: str) -> dict:
    """Validate the corpus and case matrix against the caps; no credentials or network."""
    corpus = provider.corpus(run_prefix, profile)
    cases = provider.cases(run_prefix, profile)
    ids = [case.id for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("case IDs must be unique")
    for case in cases:
        if case.token and case.token.case_id and case.token.case_id not in ids:
            raise ValueError(f"case {case.id} takes its token from an unknown case")
    names = [item.name for item in corpus]
    if len(names) != len(set(names)):
        raise ValueError("corpus names must be unique")
    for name in names:
        require_owned(name, run_prefix)
    probe_text = "\n".join([run_prefix, *names, *(case.query for case in cases),
                            *(json.dumps(case.sdk or {}) for case in cases)]).lower()
    for identity in provider.redactions():
        if identity and identity.lower() in probe_text:
            raise ValueError("a private identifier occurs in a corpus name or probe; redaction would corrupt evidence")
    payload = sum(len(item.payload) for item in corpus)
    if len(corpus) > bounds.max_objects or payload > bounds.max_payload_bytes:
        raise BoundExceeded("corpus exceeds the object or payload cap")
    bound = cost_bound(provider.prices(), bounds)
    if bound["total_usd"] > bounds.max_cost_usd:
        raise BoundExceeded(f"cost bound {bound['total_usd']} exceeds {bounds.max_cost_usd}")
    executions = sum(max(1, len(case.versions)) for case in cases)
    return {"provider": provider.name, "profile": profile, "run_prefix": run_prefix,
            "objects": len(corpus), "payload_bytes": payload,
            "groups": dict(sorted({g: sum(1 for i in corpus if i.group == g)
                                   for g in {i.group for i in corpus}}.items())),
            "cases": [case_record(case) for case in cases], "executions": executions,
            "untested": [dataclasses.asdict(item) for item in provider.untested()],
            "corpus": [{"name": item.name, "size": len(item.payload), "group": item.group,
                        "expect_created": item.expect_created, "wire_name": item.wire_name,
                        "sha256": hashlib.sha256(item.payload).hexdigest()} for item in corpus],
            "cost_bound": bound, "bounds": dataclasses.asdict(bounds),
            "request_upper_estimate": 2 * len(corpus) + 8 * bounds.max_walk_pages
            + executions * bounds.max_walk_pages}


class OwnershipJournal:
    """Fsynced record of the run prefix claim, every create intent (written before the
    request), and every create outcome. Cleanup refuses names outside the claimed prefix."""

    def __init__(self, path: Path, run_prefix: str, redactor: Redactor):
        if not RUN_PREFIX.fullmatch(run_prefix):
            raise ValueError("run prefix is not an isolated campaign prefix")
        self.path = path
        self.run_prefix = run_prefix
        self.redactor = redactor
        self.lock = threading.Lock()
        if path.exists():
            data = path.read_bytes()
            if data and not data.endswith(b"\n"):
                # Cut a torn tail left by a crash; discovery at cleanup covers its object.
                with path.open("r+b") as handle:
                    handle.truncate(data.rfind(b"\n") + 1)

    def _append(self, entry: dict) -> None:
        with self.lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def claim(self) -> None:
        self._append({"event": "claim", "run_prefix": self.run_prefix})

    def intent(self, item: CorpusObject) -> None:
        require_owned(item.name, self.run_prefix)
        self._append({"event": "intent", "name": item.name, "wire_name": item.wire_name})

    def outcome(self, name: str, result: dict) -> None:
        self._append({"event": "outcome", "name": name, **{k: self.redactor.text(v) if isinstance(v, str)
                                                           else v for k, v in result.items()}})

    def entries(self) -> list[dict]:
        if not self.path.exists():
            return []
        raw = self.path.read_text(encoding="utf-8")
        # A torn final line is an intent or outcome whose write was interrupted; discovery
        # listing at cleanup finds the object if the request was sent at all.
        # Split on newline only: names may contain U+2028 and other characters splitlines() honors.
        lines = raw.split("\n")[:-1]
        entries = [json.loads(line) for line in lines]
        for entry in entries:
            if entry["event"] == "claim" and entry["run_prefix"] != self.run_prefix:
                raise ValueError("journal claims a different prefix")
            if "name" in entry:
                require_owned(entry["name"], self.run_prefix)
        return entries

    def claimed(self) -> bool:
        return any(entry["event"] == "claim" for entry in self.entries())

    def intents(self) -> set[str]:
        return {entry["name"] for entry in self.entries() if entry["event"] == "intent"}

    def collisions(self) -> set[str]:
        """Names whose conditional create was refused because an object already existed."""
        return {entry["name"] for entry in self.entries()
                if entry["event"] == "outcome" and entry.get("status") in (409, 412)}


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


def safe_file_id(case_id: str, version: str | None) -> str:
    return case_id + (f"~{version}" if version else "")


@dataclass(frozen=True)
class SdkLauncher:
    """Runs the official-SDK capture driver on the pinned JDK."""

    java_home: str
    classpath_file: Path
    main_class: str = "io.varve.swath.replay.provider.ProviderSdkCapture"

    @classmethod
    def from_environment(cls) -> "SdkLauncher":
        return cls(os.environ.get("SWATH_CONFORMANCE_JAVA_HOME", ""),
                   REPO_ROOT / "swath-replay/build/provider-capture/classpath.txt")

    def command(self, options: Path, output: Path, private_env: dict[str, str]) -> tuple[list[str], dict]:
        if not self.java_home:
            raise BoundExceeded("SWATH_CONFORMANCE_JAVA_HOME must name the pinned JDK")
        classpath = self.classpath_file.read_text(encoding="utf-8").strip()
        environment = {"JAVA_HOME": self.java_home, "PATH": os.environ.get("PATH", ""),
                       "HOME": os.environ.get("HOME", ""), **private_env}
        return ([f"{self.java_home}/bin/java", "-cp", classpath, self.main_class,
                 "--options", str(options), "--output", str(output)], environment)


class Campaign:
    def __init__(self, provider: Provider, campaign_dir: Path, bounds: Bounds,
                 connector: Connector | None = None, clock: Callable[[], float] = time.time,
                 sdk_runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
                 sdk: SdkLauncher | None = None, sleep: Callable[[float], None] = time.sleep):
        self.provider = provider
        self.campaign_dir = campaign_dir
        self.bounds = bounds
        self.clock = clock
        self.sdk_runner = sdk_runner
        self.sdk = sdk or SdkLauncher.from_environment()
        self.redactor = Redactor(provider.redactions())
        self.ledger = CampaignLedger(campaign_dir / "attempts.jsonl", bounds, clock)
        self.client = HttpClient(self.ledger, connector or HttpConnector(), self.redactor, sleep)

    def _authorize(self) -> None:
        self.client.authorizer = self.provider.authorizer(self.client)

    # Execution --------------------------------------------------------------------

    def run(self, profile: str, run_prefix: str | None = None) -> dict:
        run_prefix = run_prefix or new_run_prefix(self.clock)
        try:
            plan = build_plan(self.provider, self.bounds, run_prefix, profile)
        except (BoundExceeded, ValueError):
            self.ledger.close()
            raise
        run_dir = self.campaign_dir / "runs" / run_prefix.rstrip("/").rsplit("/", 1)[-1]
        run_dir.mkdir(parents=True, exist_ok=False)
        write_json(run_dir / "plan.json", plan)
        journal = OwnershipJournal(run_dir / "owned.jsonl", run_prefix, self.redactor)
        receipt = {"schema_version": "provider-campaign-receipt-v1", "provider": self.provider.name,
                   "profile": profile, "run_prefix": run_prefix, "started_at": utc_now(),
                   "incomplete_reasons": [], "creation": [], "cases": {}, "cleanup": None}
        corpus = self.provider.corpus(run_prefix, profile)
        try:
            self.ledger.open(WINDOW_ACTIVE)
            cleanup_left = self.ledger.window_limit(WINDOW_CLEANUP) - self.ledger.used_seconds(WINDOW_CLEANUP)
            if cleanup_left < CLEANUP_SECONDS_PER_OBJECT * len(corpus) + CLEANUP_LISTING_SECONDS:
                raise BoundExceeded("the remaining cleanup window cannot cover this corpus; nothing was created")
            self._authorize()
            self._claim(run_prefix, journal)
            self._provision(corpus, journal, receipt)
            manifest = self._inventory_manifest(run_prefix, corpus, receipt)
            write_json(run_dir / "manifest.json", manifest)
            self._probe(self.provider.cases(run_prefix, profile), run_dir, receipt)
        except BoundExceeded as error:
            receipt["incomplete_reasons"].append(str(error))
        except Exception as error:  # noqa: BLE001 - cleanup must still run
            receipt["incomplete_reasons"].append(
                f"runner error: {type(error).__name__}: {self.redactor.text(str(error))}")
        finally:
            try:
                self.ledger.open(WINDOW_CLEANUP)
                receipt["cleanup"] = self.cleanup(run_prefix, journal)
            finally:
                self._finish(receipt, corpus, run_dir / "receipt.json")
        return receipt

    def recover(self, run_dir: Path) -> dict:
        """Cleanup-only pass for an interrupted run, inside the cleanup window."""
        plan = json.loads((run_dir / "plan.json").read_text(encoding="utf-8"))
        journal = OwnershipJournal(run_dir / "owned.jsonl", plan["run_prefix"], self.redactor)
        receipt = {"schema_version": "provider-campaign-recovery-v1", "run_prefix": plan["run_prefix"],
                   "started_at": utc_now(), "incomplete_reasons": []}
        try:
            self.ledger.open(WINDOW_CLEANUP)
            self._authorize()
            receipt["cleanup"] = self.cleanup(plan["run_prefix"], journal)
        finally:
            self._finish(receipt, [], run_dir / "recovery-receipt.json")
        return receipt

    def _finish(self, receipt: dict, corpus: list[CorpusObject], path: Path) -> None:
        cleanup = receipt.get("cleanup") or {}
        if not cleanup.get("verified_empty"):
            receipt["incomplete_reasons"].append("cleanup did not verify an empty namespace")
        summary = self.ledger.summary()
        for window in (WINDOW_ACTIVE, WINDOW_CLEANUP):
            used = self.ledger.used_seconds(window)
            if used > self.ledger.window_limit(window):
                receipt["incomplete_reasons"].append(f"{window} window exceeded its cap: {used} s")
        for reason in summary["exhausted"] + summary["run_exhausted"]:
            if reason not in receipt["incomplete_reasons"]:
                receipt["incomplete_reasons"].append(f"bound exhausted: {reason}")
        receipt["status"] = INCOMPLETE if receipt["incomplete_reasons"] else COMPLETE
        receipt["finished_at"] = utc_now()
        receipt["ledger"] = summary
        receipt["estimated_cost"] = actual_cost(self.provider.prices(), summary,
                                                sum(len(item.payload) for item in corpus))
        receipt["cost_bound"] = cost_bound(self.provider.prices(), self.bounds)
        self.ledger.close()
        write_json(path, receipt)

    def _claim(self, run_prefix: str, journal: OwnershipJournal) -> None:
        walk = self.walk(self.provider.inventory_query(run_prefix), None, SETUP, retry=True)
        listed = [entry for page in walk["pages"] for entry in page["entries"]]
        if not walk["complete"] or listed:
            raise BoundExceeded("the run prefix is not provably empty; nothing was created")
        journal.claim()

    def _provision(self, corpus: list[CorpusObject], journal: OwnershipJournal, receipt: dict) -> None:
        def create(item: CorpusObject) -> dict | None:
            if self.ledger.any_exhausted():
                return None
            journal.intent(item)
            result = {"name": item.name, "expected_created": item.expect_created, "wire_name": item.wire_name}
            try:
                response = self.client.send_with_retry(self.provider.upload(item), SETUP)
            except TransportFailure as error:
                result.update(status=None, created=None, error=str(error))
            except BoundExceeded:
                result.update(status=None, created=None, error="bound exhausted")
            else:
                request_id = next((value for name, value in response.headers if name.lower() in
                                   {"x-ms-request-id", "x-amz-request-id", "x-guploader-uploadid"}), "")
                result.update(status=response.status, created=self.provider.created(response),
                              content_type=response.header("content-type"),
                              error_code=response.header("x-ms-error-code") or response.header("x-amz-error-code"),
                              request_id_sha256=hashlib.sha256(request_id.encode()).hexdigest() if request_id else None)
                if item.expect_created is not True or not result["created"]:
                    result["body"] = self.redactor.text(
                        self.provider.scrub(response.body).decode("utf-8", "replace")[:2000])
            journal.outcome(item.name, {k: v for k, v in result.items() if k != "name"})
            return result

        with ThreadPoolExecutor(self.bounds.concurrency) as pool:
            results = [result for result in pool.map(create, corpus) if result is not None]
        if self.ledger.any_exhausted():
            raise BoundExceeded(self.ledger.any_exhausted()[0])
        receipt["creation"] = [{k: self.redactor.text(v) if isinstance(v, str) else v for k, v in r.items()}
                               for r in results if r["expected_created"] is not True or not r["created"]]
        failed = [r["name"] for r in results if r["expected_created"] is True and not r["created"]]
        if failed:
            raise BoundExceeded(f"{len(failed)} expected corpus objects were not created")

    def _inventory_manifest(self, run_prefix: str, corpus: list[CorpusObject], receipt: dict) -> dict:
        walk = self.walk(self.provider.inventory_query(run_prefix), None, SETUP, retry=True)
        if not walk["complete"]:
            raise BoundExceeded("corpus inventory walk did not complete: " + walk["reason"])
        by_name = {item.name: item for item in corpus}
        by_canonical = {self.provider.canonical_name(item.name): item for item in corpus}
        objects, unattributed = [], []
        for entry in (e for page in walk["pages"] for e in page["entries"]):
            require_owned(entry.name, run_prefix)
            if entry.prefix:
                raise BoundExceeded("flat inventory listing returned a prefix entry")
            item = by_name.get(entry.name) or by_canonical.get(entry.name)
            if item is None or entry.size != len(item.payload):
                # Ours (inside the claimed prefix), e.g. a provider-rewritten exploratory name;
                # cleanup deletes it, but it cannot join the replay fixture.
                unattributed.append(entry.name)
                continue
            record = {"name": entry.name, "size": entry.size, "updated": entry.updated,
                      "sha256": hashlib.sha256(item.payload).hexdigest(), "group": item.group}
            if entry.name != item.name:
                record["created_as"] = item.name
            if entry.attributes:
                record["attributes"] = dict(entry.attributes)
            objects.append(record)
        expected = {i.name for i in corpus if i.expect_created is True}
        missing = expected - {o["name"] for o in objects}
        if missing:
            raise BoundExceeded(f"{len(missing)} created objects are missing from the inventory")
        receipt["inventory_pages"] = len(walk["pages"])
        receipt["unattributed"] = unattributed
        return {"provider": self.provider.name, **self.provider.manifest_fields(),
                "capture_date": datetime.now(timezone.utc).date().isoformat(),
                "run_prefix": run_prefix, "objects": objects}

    def walk(self, query: str, version: str | None, phase: str, headers: tuple = (),
             retry: bool = False) -> dict:
        """`retry` is for inventory, claim and cleanup listings; probe walks never retry."""
        pages = []
        seen: set[str] = set()
        token = None
        for _ in range(self.bounds.max_walk_pages):
            request = self.provider.list_request(
                self.provider.with_token(query, token) if token else query, version, headers)
            response = self.client.send_with_retry(request, phase) if retry else self.client.send(request, phase)
            page = {"exchange": raw_exchange(request, response, self.redactor, self.provider.scrub), "entries": [],
                    "status": response.status, "response": response}
            pages.append(page)
            if response.status != 200:
                return {"pages": pages, "complete": False, "reason": f"HTTP {response.status}"}
            page["entries"], token = self.provider.page(response.body)
            if not token:
                return {"pages": pages, "complete": True, "reason": None}
            if token in seen:
                return {"pages": pages, "complete": False, "reason": "repeated continuation token"}
            seen.add(token)
        return {"pages": pages, "complete": False, "reason": "walk page bound reached"}

    def _probe(self, cases: list[Case], run_dir: Path, receipt: dict) -> None:
        walks: dict[tuple[str, str | None], dict] = {}
        unavailable: set[str] = set()
        # Token probes reuse pages of earlier walks, so they run after every other case.
        ordered = [case for case in cases if case.kind != "token"] + \
            [case for case in cases if case.kind == "token"]
        for case in ordered:
            for version in case.versions or (None,):
                key = safe_file_id(case.id, version)
                if version in unavailable:
                    receipt["cases"][key] = {"complete": False,
                                             "reason": f"service version {version} not accepted"}
                    receipt["incomplete_reasons"].append(f"case {key}: service version {version} not accepted")
                    continue
                raw, result = self._run_case(case, version, run_dir, walks)
                if case.version_gate:
                    response = result.pop("response", None)
                    if response is not None and response.status == 200 \
                            and self.provider.version_echoed(version, response):
                        result["version_accepted"] = True
                    elif response is not None and self.provider.version_unavailable(response):
                        unavailable.add(version)
                        receipt.setdefault("unavailable_versions", []).append(version)
                    else:
                        result.update(complete=False, reason="version gate gave no definite answer")
                result.pop("response", None)
                receipt["cases"][key] = result
                if raw is not None:
                    write_json(run_dir / "raw" / f"{key}.json", raw)
                if not result["complete"]:
                    receipt["incomplete_reasons"].append(f"case {key}: {result.get('reason')}")

    def _run_case(self, case: Case, version: str | None, run_dir: Path, walks: dict) -> tuple[dict | None, dict]:
        phase = SDK if case.kind == "sdk_run" else PROBE
        try:
            if case.kind in {"exchange", "token"}:
                query = case.query
                if case.kind == "token":
                    query = self.provider.with_token(query, self._token(case, version, walks))
                request = self.provider.list_request(query, version, case.headers)
                response = self.client.send(request, phase)
                return raw_exchange(request, response, self.redactor, self.provider.scrub), \
                    {"status": response.status, "complete": True, "response": response}
            if case.kind == "walk":
                walk = self.walk(case.query, version, phase, case.headers)
                walks[(case.id, version)] = walk
                raw = {"steps": [{"phase": "walk", "exchange": page["exchange"]} for page in walk["pages"]]}
                # A walk that ends on an HTTP error is still a complete observation of that error.
                complete = walk["complete"] or walk["reason"].startswith("HTTP ")
                return raw, {"pages": len(walk["pages"]), "complete": complete,
                             "terminal": walk["complete"], "reason": walk["reason"]}
            return self._sdk(case, version, run_dir)
        except TransportFailure as error:
            return None, {"complete": False, "reason": f"transport: {self.redactor.text(str(error))}"}
        except LookupError as error:
            return None, {"complete": False, "reason": str(error)}

    def _token(self, case: Case, version: str | None, walks: dict) -> str:
        source = case.token
        if source.mutation == "tampered":
            return mutate_token("", "tampered")
        walk = walks.get((source.case_id, version))
        if walk is None or len(walk["pages"]) <= source.page or walk["pages"][source.page]["status"] != 200:
            raise LookupError(f"token source page {source.case_id}:{source.page} is unavailable")
        _, token = self.provider.page(walk["pages"][source.page]["response"].body)
        if not token:
            raise LookupError(f"token source page {source.case_id}:{source.page} is terminal")
        return mutate_token(token, source.mutation) if source.mutation else token

    def _sdk(self, case: Case, version: str | None, run_dir: Path) -> tuple[dict | None, dict]:
        remaining = min(self.ledger.remaining_seconds(SDK), SDK_MAX_SECONDS)
        if remaining <= 1:
            self.ledger.mark_exhausted("sdk time window exhausted")
            raise BoundExceeded("sdk time window exhausted")
        planned_until = self.clock() + remaining
        granted = self.ledger.allowance(SDK, case.sdk.get("max_attempts", 2 * (self.bounds.max_walk_pages + 4)),
                                        planned_until)
        byte_allowance = min(SDK_RESPONSE_BYTE_ALLOWANCE,
                             self.bounds.max_total_response_bytes - self.ledger.response_bytes)
        self.ledger.charge_bytes(byte_allowance, SDK)
        used, used_bytes = granted, byte_allowance
        key = safe_file_id(case.id, version)
        output = run_dir / "sdk" / f"{key}.fifo"
        options_file = run_dir / "sdk" / f"{key}.options.json"
        options = {**self.provider.sdk_options(case, version),
                   "maxAttempts": granted // SDK_RESEND_FACTOR,
                   "deadlineEpochMs": int(planned_until * 1000),
                   "requestTimeoutSeconds": int(self.bounds.request_seconds),
                   "maxPages": self.bounds.max_walk_pages,
                   "maxResponseBytes": self.bounds.max_response_bytes,
                   "maxTotalResponseBytes": byte_allowance}
        write_json(options_file, options)
        # The driver writes its raw result into a pipe, so unredacted bytes never reach a file.
        os.mkfifo(output, 0o600)
        received: list[bytes] = []
        reader = threading.Thread(target=lambda: received.append(output.read_bytes()), daemon=True)
        reader.start()
        try:
            command, environment = self.sdk.command(options_file, output, self.provider.sdk_environment())
            try:
                completed = self.sdk_runner(command, env=environment, capture_output=True, text=True,
                                            timeout=remaining, check=False)
            except subprocess.TimeoutExpired:
                return None, {"complete": False, "reason": "SDK subprocess reached the window deadline"}
            self._release(reader, output)
            (run_dir / "sdk" / f"{key}.log").write_text(
                self.redactor.text(completed.stdout + completed.stderr), encoding="utf-8")
            if not received or not received[0]:
                return None, {"complete": False, "reason": f"SDK exited {completed.returncode} without output"}
            result = json.loads(self.redactor.text(received[0].decode("utf-8")))
            reported = result.get("storage_attempts", -1) + result.get("auth_attempts", -1)
            steps = result.get("steps") or []
            statuses = [step.get("exchange", {}).get("response", {}).get("status") for step in steps]
            storage, auth = result.get("storage_attempts"), result.get("auth_attempts")
            valid = completed.returncode == 0 and result.get("status") == COMPLETE and steps \
                and result.get("sdk") == self.provider.sdk_identity() \
                and isinstance(storage, int) and isinstance(auth, int) and auth >= 0 \
                and len(steps) <= storage and storage + auth <= options["maxAttempts"] \
                and 0 < result.get("response_bytes", -1) <= byte_allowance and statuses[-1] == 200 \
                and self._terminal(steps[-1])
            if not valid:
                return None, {"complete": False, "reason": f"SDK result not accepted: exit {completed.returncode}, "
                                                           f"status {result.get('status')}, {result.get('reason')}"}
            used = min(granted, SDK_RESEND_FACTOR * reported)
            used_bytes = result["response_bytes"]
            raw = {"steps": [{"phase": step["phase"], "exchange": self._scrubbed(step["exchange"])}
                             for step in result["steps"]]}
            return raw, {"complete": True, "pages": len(result["steps"]), "sdk": result["sdk"],
                         "java_version": result.get("java_version"), "attempts": reported,
                         "charged_attempts": used, "response_bytes": used_bytes}
        finally:
            self._release(reader, output)
            output.unlink(missing_ok=True)
            self.ledger.settle(SDK, granted, used)
            self.ledger.refund_bytes(byte_allowance - used_bytes)

    @staticmethod
    def _release(reader: threading.Thread, fifo: Path) -> None:
        """Unblock a pipe reader whose writer never opened the pipe, then wait for it."""
        if reader.is_alive():
            fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK) if fifo.exists() else None
            if fd is not None:
                os.close(fd)
            reader.join(10)

    def _scrubbed(self, exchange: dict) -> dict:
        body = self.provider.scrub(base64.b64decode(exchange["response"]["body_base64"]))
        exchange = {**exchange, "response": {**exchange["response"],
                                             "body_base64": base64.b64encode(body).decode("ascii")}}
        return self.redactor.exchange(exchange)

    def _terminal(self, step: dict) -> bool:
        try:
            _, token = self.provider.page(base64.b64decode(step["exchange"]["response"]["body_base64"]))
        except (ValueError, KeyError, ElementTreeError):
            return False
        return token is None

    # Cleanup ----------------------------------------------------------------------

    def cleanup(self, run_prefix: str, journal: OwnershipJournal) -> dict:
        """Delete only names listed inside the claimed run prefix, conditionally on their
        listed version, then verify the whole namespace."""
        outcome = {"claimed": False, "deleted": 0, "absent": 0, "failed": [], "rounds": 0,
                   "unattributed": [], "leftovers": [], "foreign": [], "verified_empty": False, "reason": None}
        try:
            outcome["claimed"] = journal.claimed()
            intents = journal.intents()
            collisions = journal.collisions()
            outcome["skipped_collisions"] = [self.redactor.text(n) for n in sorted(collisions)]
            for _ in range(CLEANUP_ROUNDS if outcome["claimed"] else 0):
                discovery = self.walk(self.provider.inventory_query(run_prefix), None, CLEANUP, retry=True)
                if not discovery["complete"]:
                    outcome["reason"] = "discovery listing incomplete: " + discovery["reason"]
                    break
                listed = [e for page in discovery["pages"] for e in page["entries"] if e.name not in collisions]
                if not listed:
                    break
                outcome["rounds"] += 1
                for entry in listed:
                    require_owned(entry.name, run_prefix)
                    if entry.name not in intents and entry.name not in outcome["unattributed"]:
                        outcome["unattributed"].append(self.redactor.text(entry.name))
                with ThreadPoolExecutor(self.bounds.concurrency) as pool:
                    for name, state in pool.map(self._remove, listed):
                        if state in ("deleted", "absent"):
                            outcome[state] += 1
                        else:
                            outcome["failed"].append({"name": self.redactor.text(name), "state": state})
            verify = self.walk(self.provider.inventory_query(""), None, CLEANUP, retry=True)
            names = [entry.name for page in verify["pages"] for entry in page["entries"]]
            outcome["leftovers"] = [self.redactor.text(n) for n in names if n.startswith(run_prefix)]
            outcome["foreign"] = [self.redactor.text(n) for n in names if not n.startswith(run_prefix)]
            outcome["verified_empty"] = verify["complete"] and not names
            if not verify["complete"]:
                outcome["reason"] = "verification listing incomplete: " + verify["reason"]
        except Exception as error:  # noqa: BLE001 - the receipt must record why cleanup stopped
            outcome["reason"] = self.redactor.text(f"{type(error).__name__}: {error}")
        return outcome

    def _remove(self, entry: Listed) -> tuple[str, str]:
        request = self.provider.delete(entry.name, entry.version)
        if request is None:
            return entry.name, "not deleted: listing gave no version for a conditional delete"
        try:
            response = self.client.send_with_retry(request, CLEANUP, attempts=CLEANUP_DELETE_ATTEMPTS)
        except (TransportFailure, BoundExceeded) as error:
            return entry.name, f"error: {self.redactor.text(str(error))}"
        if self.provider.deleted(response):
            return entry.name, "deleted"
        if response.status == 404:
            return entry.name, "absent"
        return entry.name, f"HTTP {response.status}"


# Sanitization ---------------------------------------------------------------------


PORTABLE_SDK_OPTIONS = frozenset({"provider", "walk", "prefix", "delimiter", "startOffset", "endOffset",
                                  "startFrom", "startAfter", "encodingType", "fetchOwner", "region",
                                  "pageSize", "serviceVersion", "clientRequestIdPrefix",
                                  "retryAttempts", "maxPages", "maxEntries", "maxResponseBytes",
                                  "requestTimeoutSeconds"})


def creation_reason(body: str) -> str | None:
    """A provider's typed error code for a refused create, without its free-text message."""
    try:
        errors = json.loads(body).get("error", {}).get("errors", [])
        return errors[0].get("reason") if errors else None
    except (ValueError, AttributeError):
        match = re.search(r"<Code>([A-Za-z]+)</Code>", body)
        return match.group(1) if match else None


def sanitize_run(provider: Provider, run_dir: Path, out_dir: Path, compress_over: int = 256 * 1024) -> dict:
    """Write sanitized, checksummed evidence for every completed case of one run."""
    manifest = evidence.strict_json((run_dir / "manifest.json").read_text(encoding="utf-8"))
    plan = json.loads((run_dir / "plan.json").read_text(encoding="utf-8"))
    receipt = json.loads((run_dir / "receipt.json").read_text(encoding="utf-8"))
    identifiers = provider.private_identifiers()
    run_prefix = manifest["run_prefix"]
    index = {"schema_version": "provider-evidence-index-v1", "provider": provider.name,
             "run_prefix": run_prefix, "manifests": {}, "captures": {}, "cases": plan["cases"],
             "untested": plan["untested"], "creation": [], "sanitize_errors": {},
             "receipt": {k: receipt.get(k) for k in ("status", "incomplete_reasons", "ledger", "estimated_cost",
                                                     "cost_bound", "started_at", "finished_at",
                                                     "unavailable_versions", "unattributed")}}
    index["receipt"]["cleanup"] = {k: v for k, v in receipt["cleanup"].items()
                                   if k in {"claimed", "deleted", "absent", "rounds", "verified_empty", "reason"}}
    index["receipt"]["cleanup"]["leftover_count"] = len(receipt["cleanup"]["leftovers"])
    for result in receipt["creation"]:
        index["creation"].append({"name": result["name"], "wire_name": result.get("wire_name"),
                                  "status": result["status"], "created": result["created"],
                                  "expected_created": result["expected_created"],
                                  "content_type": result.get("content_type"),
                                  "error_code": result.get("error_code"),
                                  "request_id_sha256": result.get("request_id_sha256"),
                                  "error_reason": creation_reason(result.get("body", ""))})
    digests = {}
    for api_version in provider.api_versions:
        versioned = dict(manifest, api_version=api_version)
        evidence.validate_manifest(versioned)
        payload = evidence.canonical(versioned)
        scan_private(versioned, identifiers)
        name = f"manifest.{api_version}.json"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / name).write_bytes(payload)
        digests[api_version] = hashlib.sha256(payload).hexdigest()
        index["manifests"][api_version] = {"file": name, "sha256": digests[api_version]}
    for case in plan["cases"]:
        for version in case["versions"] or [None]:
            key = safe_file_id(case["id"], version)
            raw_path = run_dir / "raw" / f"{key}.json"
            if not raw_path.exists() or not receipt["cases"].get(key, {}).get("complete"):
                continue
            api_version = version or provider.api_versions[0]
            raw = json.loads(raw_path.read_text(encoding="utf-8"))
            kind = "exchange" if case["kind"] == "token" else case["kind"]
            if kind == "walk" and len(raw["steps"]) == 1:
                # A walk the provider finished (or refused) in one page is an exchange observation.
                raw, kind = raw["steps"][0]["exchange"], "exchange"
            try:
                if kind == "exchange":
                    safe = evidence.sanitize_exchange(raw, provider.name, provider.sanitize_names(),
                                                      digests[api_version], case["id"], api_version)
                else:
                    safe = evidence.sanitize_run(raw, provider.name, provider.sanitize_names(),
                                                 digests[api_version], case["id"], api_version, kind)
                scan_private(safe, identifiers)
            except (ValueError, KeyError) as error:
                index["sanitize_errors"][key] = f"{type(error).__name__}: {error}"
                continue
            payload = evidence.canonical(safe)
            stored = payload if len(payload) <= compress_over else evidence.deterministic_gzip(payload)
            file_name = f"captures/{key}.json" + ("" if stored is payload else ".gz")
            (out_dir / file_name).parent.mkdir(parents=True, exist_ok=True)
            (out_dir / file_name).write_bytes(stored)
            entry = {"file": file_name, "sha256": hashlib.sha256(payload).hexdigest(), "kind": kind}
            wire = provider.raw_wire(raw, digests[api_version], case["id"], entry["sha256"]) \
                if kind == "exchange" else None
            if wire is not None:
                scan_private(json.loads(wire), identifiers)
                wire_name = f"raw-wire/{key}.json"
                (out_dir / wire_name).parent.mkdir(parents=True, exist_ok=True)
                (out_dir / wire_name).write_bytes(wire)
                entry["raw_wire"] = {"file": wire_name, "sha256": hashlib.sha256(wire).hexdigest()}
            if case["kind"] == "sdk_run":
                options = json.loads((run_dir / "sdk" / f"{key}.options.json").read_text(encoding="utf-8"))
                # Namespace, credentials and budget fields are per-endpoint and stay private.
                entry["sdk_options"] = {k: v for k, v in options.items() if k in PORTABLE_SDK_OPTIONS}
            index["captures"][key] = entry
    scan_private(index, identifiers)
    payload = (json.dumps(index, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    (out_dir / "index.json").write_bytes(payload)
    return index


# Plan rendering and CLI -----------------------------------------------------------


def plan_markdown(plan: dict) -> str:
    def cell(value: str) -> str:
        return value.replace("|", "\\|").replace("\n", "\\n").replace("\r", "\\r")

    lines = [f"# {plan['provider']} case matrix ({plan['profile']})", "",
             f"Run prefix `{plan['run_prefix']}` (example); {plan['objects']} objects, "
             f"{plan['payload_bytes']} payload bytes; groups {plan['groups']}; "
             f"{len(plan['cases'])} cases, {plan['executions']} executions.",
             f"Cost bound at the full caps: US${plan['cost_bound']['total_usd']}.", "",
             "| Case | Kind | Versions | Behavior | Question | Raw query or SDK options |",
             "| --- | --- | --- | --- | --- | --- |"]
    for case in plan["cases"]:
        detail = json.dumps(case["sdk"], ensure_ascii=False) if "sdk" in case else case["query"]
        if "token" in case:
            token = case["token"]
            source = "tampered constant" if token["mutation"] == "tampered" else \
                f"token from {token['case_id']} page {token['page']}" + \
                (f", {token['mutation']}" if token["mutation"] else "")
            detail += f" + {source}"
        if case["headers"]:
            detail += " headers " + json.dumps(case["headers"])
        lines.append(f"| {case['id']} | {case['kind']} | {', '.join(case['versions']) or '—'} | "
                     f"{cell(case['title'])} | {cell(case['discriminates'])} | `{cell(detail)}` |")
    lines += ["", "## Corpus", "", "| Name (Python repr) | Group | Size | Expect created | Wire spelling |",
              "| --- | --- | --- | --- | --- |"]
    for item in plan["corpus"]:
        if item["group"] == "b" and not item["name"].endswith(("/0000", "/00000", "zzzz")):
            continue
        lines.append(f"| `{cell(ascii(item['name']))}` | {item['group']} | {item['size']} | "
                     f"{item['expect_created']} | {cell(item['wire_name'] or '')} |")
    lines += ["", "Bulk `b/` names are elided except the first and the extra one.", "",
              "## Explicitly untested", "", "| Topic | Reason |", "| --- | --- |"]
    lines += [f"| {cell(u['topic'])} | {cell(u['reason'])} |" for u in plan["untested"]]
    return "\n".join(lines) + "\n"


def bounds_for(provider: Provider, profile: str) -> Bounds:
    """Raise the cleanup reserve (never lower it) so two cleanup rounds over the whole
    corpus plus listings fit, while setup and probes keep enough attempts for the plan."""
    objects = len(provider.corpus(new_run_prefix(), profile))
    needed = CLEANUP_ROUNDS * objects + 4 * CLEANUP_ROUNDS * HARD_BOUNDS.max_walk_pages
    reserve = max(HARD_BOUNDS.cleanup_reserve, needed)
    bounds = HARD_BOUNDS.tightened(cleanup_reserve=reserve)
    if bounds.max_attempts - reserve < 2 * objects:
        raise BoundExceeded("the attempt cap cannot cover this corpus with a full cleanup reserve")
    return bounds


def load_provider(name: str) -> Provider:
    if name == "gcs":
        import gcs_live
        return gcs_live.GcsProvider.from_environment()
    if name == "azure":
        import azure_live
        return azure_live.AzureProvider.from_environment()
    if name == "s3":
        import s3_live
        return s3_live.S3Provider.from_environment()
    raise ValueError(f"unknown provider {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["plan", "run", "recover", "sanitize"])
    parser.add_argument("--provider", choices=["gcs", "azure", "s3"], required=True)
    parser.add_argument("--campaign-dir", type=Path, help="private directory with the ledger and raw runs")
    parser.add_argument("--profile", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--run", help="run directory name for recover or sanitize")
    parser.add_argument("--out", type=Path, help="publishable evidence directory for sanitize")
    parser.add_argument("--markdown", action="store_true", help="render plan as a case matrix")
    parser.add_argument("--confirm-live", action="store_true",
                        help="required for run and recover: sends authenticated provider requests")
    args = parser.parse_args()
    provider = load_provider(args.provider)
    if args.command == "plan":
        plan = build_plan(provider, HARD_BOUNDS, new_run_prefix(), args.profile)
        print(plan_markdown(plan) if args.markdown else json.dumps(plan, ensure_ascii=False, indent=2))
        return
    campaign_dir = args.campaign_dir / args.provider
    if args.command == "sanitize":
        index = sanitize_run(provider, campaign_dir / "runs" / args.run, args.out)
        print(json.dumps({"captures": len(index["captures"]), "sanitize_errors": index["sanitize_errors"]},
                         ensure_ascii=False, indent=2))
        return
    if not args.confirm_live:
        raise SystemExit("this command sends live provider requests; pass --confirm-live")
    campaign = Campaign(provider, campaign_dir, bounds_for(provider, args.profile))
    receipt = campaign.recover(campaign_dir / "runs" / args.run) if args.command == "recover" \
        else campaign.run(args.profile)
    print(json.dumps({k: receipt.get(k) for k in ("status", "incomplete_reasons", "ledger", "estimated_cost",
                                                  "cleanup")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
