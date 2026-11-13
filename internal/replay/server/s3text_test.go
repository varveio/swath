// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/internal/replay/store"
)

// TestControlCharacterKeysAreReferences serves a fixture whose keys hold
// C0 control characters: without encoding-type each is written as live
// S3 writes listing text (TestLiveControlCharactersAreReferences), a
// hexadecimal reference except TAB, LF and CR, DEL as it is, in Key,
// CommonPrefixes/Prefix and the echoed Prefix, StartAfter and Delimiter;
// with encoding-type=url as %XX.
func TestControlCharacterKeysAreReferences(t *testing.T) {
	dir := t.TempDir()
	var rows []legacytest.Row
	for i, k := range []string{"a\x01/k1", "a\x01/k2", "b\t\x1f\x7fk", "c\x0b\nk"} {
		rows = append(rows, legacytest.Object(k, i))
	}
	if err := legacytest.Write(filepath.Join(dir, "part-00001.parquet"),
		legacytest.Part{Rows: rows, Stamps: legacytest.Stamps(1, true)}); err != nil {
		t.Fatal(err)
	}
	st, _, err := store.Open(context.Background(), dir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := start(t, config(st))
	for target, want := range map[string][]string{
		"/bucket?list-type=2&delimiter=/": {"<Delimiter>/</Delimiter>",
			"<Key>b\t&#x1f;\x7fk</Key>", "<Key>c&#xb;\nk</Key>", "<CommonPrefixes><Prefix>a&#x1;/</Prefix></CommonPrefixes>"},
		"/bucket?list-type=2&prefix=a%01&start-after=a%01%2Fk1": {"<Prefix>a&#x1;</Prefix><StartAfter>a&#x1;/k1</StartAfter>",
			"<KeyCount>1</KeyCount>", "<Key>a&#x1;/k2</Key>"},
		"/bucket?list-type=2&delimiter=%1F": {"<Delimiter>&#x1f;</Delimiter>", "<CommonPrefixes><Prefix>b\t&#x1f;</Prefix></CommonPrefixes>"},
		"/bucket?list-type=2&delimiter=%1F&encoding-type=url": {"<Delimiter>%1F</Delimiter>", "<Key>a%01/k1</Key>",
			"<CommonPrefixes><Prefix>b%09%1F</Prefix></CommonPrefixes>", "<Key>c%0B%0Ak</Key>"},
	} {
		r := get(t, s, target)
		for _, w := range want {
			if r.status != 200 || !strings.Contains(r.body, w) {
				t.Errorf("%s: %d lacks %q:\n%q", target, r.status, w, r.body)
			}
		}
	}
}
