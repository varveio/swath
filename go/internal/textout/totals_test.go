// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/totals"
)

// TestIEC: a table's size keeps at most three significant digits across
// the unit boundaries, moving to the next unit before a fourth digit.
func TestIEC(t *testing.T) {
	const k, m = 1024, 1024 * 1024
	for n, want := range map[int64]string{
		0: "0 B", 1023: "1023 B", k: "1.00 KiB", 10 * k: "10.0 KiB", 100 * k: "100 KiB",
		999 * k: "999 KiB", 999*k + k/2: "0.98 MiB", 1000 * k: "0.98 MiB", 1023 * k: "1.00 MiB", m: "1.00 MiB",
		9996 * m / 1000: "10.0 MiB", 9996 * m / 100: "100 MiB", 9994 * m / 1000: "9.99 MiB", 1 << 62: "4.00 EiB",
	} {
		if got := string(appendIEC(nil, &totals.Prefix{Bytes: n})); got != want {
			t.Errorf("%d: %q, want %q", n, got, want)
		}
	}
	if got := string(appendIEC(nil, &totals.Prefix{Bytes: 5, Unsized: 1})); got != "-" {
		t.Errorf("unsized: %q", got)
	}
}

// TestTotalsJSONL: every line is JSON; a prefix that is not UTF-8 is
// PrefixBase64 alone, its exact bytes; an unknown size and a missing time
// are absent, not zero.
func TestTotalsJSONL(t *testing.T) {
	rows := []totals.Prefix{
		{Prefix: "", Objects: 3, Bytes: 7, Newest: 1, HasNewest: true},
		{Prefix: "caf\xc3\xa9/\"q\"\t/", Objects: 1, Bytes: 2},
		{Prefix: "bad\xff/", Objects: 1, Unsized: 1},
	}
	var b bytes.Buffer
	if err := WriteTotals(&b, JSONL, TotalsLayout{}, rows); err != nil {
		t.Fatal(err)
	}
	var got []map[string]any
	for l := range strings.Lines(b.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("%q: %v", l, err)
		}
		got = append(got, m)
	}
	if len(got) != 3 || got[1][FieldPrefix] != rows[1].Prefix || got[1][FieldSize] != 2.0 || got[1][FieldLastModified] != nil {
		t.Fatalf("%v", got)
	}
	raw, err := base64.StdEncoding.DecodeString(got[2][FieldPrefixBase64].(string))
	if err != nil || string(raw) != rows[2].Prefix || got[2][FieldPrefix] != nil || got[2][FieldSize] != nil {
		t.Fatalf("%v (%v)", got[2], err)
	}
}
