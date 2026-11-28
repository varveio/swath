// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/docs"
	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3/client"
)

// Counters is the S3 tally in report order, assembled for documentation and ID checks:
// a versions listing's, whose definitions extend a current listing's.
var Counters = append(append([]counters.Def(nil), keyrange.Counters...), versionsCounters...)

// TestDocsCounters keeps the S3 table in docs/internals/counters.md generated
// from Counters: run with UPDATE=1 to rewrite it.
func TestDocsCounters(t *testing.T) {
	path, begin, end := docs.Path(t, "internals/counters.md"), "<!-- s3:begin -->\n", "<!-- s3:end -->"
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
	t.Fatal("docs/internals/counters.md is stale; run UPDATE=1 go test ./internal/source/s3 -run TestDocsCounters")
}

// TestCountersMatchIDs: one client counter definition per ID; keys unique
// across the engine's and the client's.
func TestCountersMatchIDs(t *testing.T) {
	if len(clientCounters) != cB2LongKeyRestarts+1 {
		t.Fatalf("%d client definitions for %d IDs", len(clientCounters), cB2LongKeyRestarts+1)
	}
	if len(versionsCounters) != cB2LatestCleared+1 || !slices.Equal(versionsCounters[:len(clientCounters)], clientCounters) {
		t.Fatalf("%d versions definitions for %d IDs, or not the client's first", len(versionsCounters), cB2LatestCleared+1)
	}
	seen := map[string]bool{}
	for _, d := range Counters {
		if seen[d.Key] {
			t.Fatalf("duplicate key %s", d.Key)
		}
		seen[d.Key] = true
	}
}

// refreshing is a stub client that reports credential refreshes.
type refreshing struct{ stub }

func (*refreshing) CredentialRefreshes() (int64, int64) { return 3, 1 }

// TestCredentialRefreshCounters: a client's credential refreshes reach the
// report's counters.
func TestCredentialRefreshCounters(t *testing.T) {
	page := &client.Page{Contents: []client.Object{{Key: "a"}}}
	_, st := list(t, Options{Bucket: "b", Client: &refreshing{stub{pages: []*client.Page{page}}}, Policy: keyrange.Policy{Sequential: true}}, 1)
	if st.Counters.Value("credentials.refreshes") != 3 || st.Counters.Value("credentials.expired_refreshes") != 1 {
		t.Fatalf("refreshes %d, expired %d", st.Counters.Value("credentials.refreshes"), st.Counters.Value("credentials.expired_refreshes"))
	}
}
