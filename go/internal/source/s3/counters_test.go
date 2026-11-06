// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"os"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestCountersDoc keeps the S3 table in docs/internals/counters.md generated
// from Counters: run with UPDATE=1 to rewrite it.
func TestCountersDoc(t *testing.T) {
	const path, begin, end = "../../../docs/internals/counters.md", "<!-- s3:begin -->\n", "<!-- s3:end -->"
	doc, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(doc)
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		t.Fatal("counters.md lacks the s3 markers")
	}
	want := s[:i+len(begin)] + counters.Markdown(Counters) + s[j:]
	if want == s {
		return
	}
	if os.Getenv("UPDATE") != "" {
		if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("docs/internals/counters.md is stale; run UPDATE=1 go test ./internal/source/s3 -run TestCountersDoc")
}

// TestCountersMatchIDs: one client counter definition per ID; keys unique
// across the engine's and the client's.
func TestCountersMatchIDs(t *testing.T) {
	if len(clientCounters) != int(objhttp.NumCounters) {
		t.Fatalf("%d client definitions for %d IDs", len(clientCounters), objhttp.NumCounters)
	}
	seen := map[string]bool{}
	for _, d := range Counters {
		if seen[d.Key] {
			t.Fatalf("duplicate key %s", d.Key)
		}
		seen[d.Key] = true
	}
}
