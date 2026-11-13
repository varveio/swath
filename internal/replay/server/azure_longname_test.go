// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/internal/replay/sortfixture"
	"github.com/varveio/swath/internal/replay/store"
)

// azureMaxNameBytes is the longest Azure name in UTF-8: 1,024 UTF-16 code
// units, each a three-byte scalar.
const azureMaxNameBytes = 3072

var azureNameRe = regexp.MustCompile(`<Name>([^<]*)</Name>`)
var azureNextMarkerRe = regexp.MustCompile(`<NextMarker>([^<]*)</NextMarker>`)

// TestAzureServesLongestNames builds a sorted fixture of 3,072-byte names
// with sort-fixture's key limit at 3,072 (the default 1,024 refuses them),
// serves it through the real store and the Azure route, and lists it with a
// startFrom of that length and with a page-size-one marker walk. The names
// include a U+E000 / U+10000 sibling pair under a 3,066-byte parent, which
// lists in the provider's order, not byte order.
func TestAzureServesLongestNames(t *testing.T) {
	parent := strings.Repeat("中", 1022)
	plain, high, supp := parent+"中中", parent+"中", parent+"\U00010000"
	ascii := "long/" + strings.Repeat("a", 1019)
	keys := []string{"a", ascii, plain, high, supp}
	for _, k := range []string{plain, high} {
		if len(k) != azureMaxNameBytes {
			t.Fatalf("%d bytes", len(k))
		}
	}
	slices.Sort(keys)
	var rows []legacytest.Row
	for i, k := range keys {
		rows = append(rows, legacytest.Object(k, i))
	}
	capture := filepath.Join(t.TempDir(), "capture")
	if err := os.MkdirAll(capture, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := legacytest.Write(filepath.Join(capture, "part-a.parquet"), legacytest.Part{Rows: rows}); err != nil {
		t.Fatal(err)
	}
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, sortfixture.ErrKeyTooLong) {
		t.Fatalf("the default key limit: %v", err)
	}
	out := filepath.Join(t.TempDir(), "fixture")
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: out, KeyLimit: azureMaxNameBytes}, nil); err != nil {
		t.Fatal(err)
	}
	st, meta, err := store.Open(context.Background(), out, store.Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := config(st)
	cfg.Protocols, cfg.AzureAccount, cfg.FixtureIdentity = []string{"s3", "azure"}, DefaultAzureAccount, meta.Identity
	s := start(t, cfg)

	list := func(query string) (names []string, next string) {
		t.Helper()
		target := "/replay/bucket?restype=container&comp=list"
		if query != "" {
			target += "&" + query
		}
		r := azureDo(t, s, "GET", target, "x-ms-version", "2026-10-06")
		if r.status != 200 {
			t.Fatalf("%.80s: %+v", query, r)
		}
		for _, m := range azureNameRe.FindAllStringSubmatch(r.body, -1) {
			names = append(names, m[1])
		}
		if m := azureNextMarkerRe.FindStringSubmatch(r.body); m != nil {
			next = m[1]
		}
		return names, next
	}
	want := []string{"a", ascii, plain, supp, high}
	if names, _ := list(""); !slices.Equal(names, want) {
		t.Fatalf("flat listing: %d names %q", len(names), names)
	}
	for start, want := range map[string][]string{plain: {plain, supp, high}, high: {high}, parent + "": {high}} {
		if names, _ := list("startFrom=" + url.QueryEscape(start)); !slices.Equal(names, want) {
			t.Errorf("startFrom of %d bytes: %d names", len(start), len(names))
		}
	}
	var walked []string
	marker := ""
	for range len(want) + 1 {
		q := "maxresults=1&startFrom=" + url.QueryEscape(plain)
		if marker != "" {
			q += "&marker=" + marker
		}
		names, next := list(q)
		walked = append(walked, names...)
		if marker = next; marker == "" {
			break
		}
	}
	if !slices.Equal(walked, want[2:]) {
		t.Fatalf("marker walk: %d names", len(walked))
	}
}
