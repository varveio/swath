# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Convert a provider-evidence corpus manifest into a CSV DuckDB loads.

Usage: python3 -I evidence_csv.py gcs|azure MANIFEST.json OUT.csv

Columns: name_hex (the UTF-8 bytes of the object name in hex: names carry
NUL, tab, U+2028 and other characters a CSV reader may change), size (an
integer) and updated_us (microseconds since the Unix epoch, UTC). GCS
`updated` is RFC 3339; Azure `updated` is RFC 1123, second precision. The
other manifest fields (group, sha256, created_as) are dropped. The manifest
is read as data only.
"""
import csv
import json
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def micros(provider, value):
    if provider == "azure":
        t = parsedate_to_datetime(value)
    else:
        t = datetime.fromisoformat(value.replace("Z", "+00:00"))
    d = t.astimezone(timezone.utc) - EPOCH
    return (d.days * 86400 + d.seconds) * 1_000_000 + d.microseconds


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in ("gcs", "azure"):
        sys.exit("usage: evidence_csv.py gcs|azure MANIFEST.json OUT.csv")
    provider, src, dst = sys.argv[1:4]
    with open(src, encoding="utf-8") as f:
        objects = json.load(f)["objects"]
    with open(dst, "w", newline="", encoding="ascii") as f:
        w = csv.writer(f)
        w.writerow(["name_hex", "size", "updated_us"])
        for o in objects:
            w.writerow([o["name"].encode("utf-8").hex(), int(o["size"]), micros(provider, o["updated"])])
    print(f"{len(objects)} rows -> {dst}")


main()
