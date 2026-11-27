// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"os"
	"strings"
	"testing"

	"github.com/varveio/swath/docs"
	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3/client"
)

// Counters is the S3 tally in report order, assembled for documentation and ID checks.
var Counters = append(append([]counters.Def(nil), keyrange.Counters...), clientCounters...)

// TestDocsCounters keeps the S3 tables in docs/internals/counters.md
// generated from Counters (the range engine's listing) and treeCounters
// (the folder walk's): run with UPDATE=1 to rewrite them.
func TestDocsCounters(t *testing.T) {
	path := docs.Path(t, "internals/counters.md")
	doc, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(doc)
	for _, table := range []struct {
		name string
		defs []counters.Def
	}{{"s3", Counters}, {"s3-tree", treeCounters}} {
		begin, end := "<!-- "+table.name+":begin -->\n", "<!-- "+table.name+":end -->"
		i, j := strings.Index(s, begin), strings.Index(s, end)
		if i < 0 || j < i {
			t.Fatalf("counters.md lacks the %s markers", table.name)
		}
		s = s[:i+len(begin)] + counters.Markdown(table.defs) + s[j:]
	}
	if s == string(doc) {
		return
	}
	if os.Getenv("UPDATE") != "" {
		if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("docs/internals/counters.md is stale; run UPDATE=1 go test ./internal/source/s3 -run TestDocsCounters")
}

// TestCountersMatchIDs: one client counter definition per ID; keys unique
// across the engine's and the client's.
func TestCountersMatchIDs(t *testing.T) {
	if len(clientCounters) != cCredentialExpired+1 {
		t.Fatalf("%d client definitions for %d IDs", len(clientCounters), cCredentialExpired+1)
	}
	for _, defs := range [][]counters.Def{Counters, treeCounters} {
		seen := map[string]bool{}
		for _, d := range defs {
			if seen[d.Key] {
				t.Fatalf("duplicate key %s", d.Key)
			}
			seen[d.Key] = true
		}
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
