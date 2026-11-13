// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
)

// TestAzurePriorPages: a counted prior lists in the service's largest
// pages (client.MaxPageSize) unless --page-size sets one; a prior without
// counts, and a run without one, keep pages of defaultPageSize. The
// listing is exact either way.
func TestAzurePriorPages(t *testing.T) {
	var keys []string
	for i := range 30000 {
		keys = append(keys, fmt.Sprintf("d%02d/obj%06d", i%11, i))
	}
	slices.Sort(keys)
	prior := func(counted bool) *source.Prior {
		pr := &source.Prior{Source: "test", Counted: counted}
		for i := 99; i < len(keys); i += 100 {
			pr.Cuts = append(pr.Cuts, source.Cut{Key: keys[i], Rows: 100})
		}
		return pr
	}
	for _, c := range []struct {
		what  string
		prior *source.Prior
		page  int // --page-size (0: none)
		want  int
	}{
		{"counted", prior(true), 0, client.MaxPageSize},
		{"counted, --page-size", prior(true), 700, 700},
		{"cuts only", prior(false), 0, defaultPageSize},
		{"no prior", nil, 0, defaultPageSize},
	} {
		st := azblobtest.NewStore(keys)
		p, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: st, Workers: 4,
			Policy: keyrange.Policy{PageSize: c.page}})
		if err != nil {
			t.Fatal(err)
		}
		if c.prior != nil {
			if err := p.SeedFrom(c.prior); err != nil {
				t.Fatal(err)
			}
		}
		rows := &collect{}
		if err := runRows(t, context.Background(), p, 4, rows); err != nil {
			t.Fatalf("%s: %v", c.what, err)
		}
		if got := keysOf(rows.rows); !slices.Equal(got, keys) {
			t.Fatalf("%s: %d keys, want %d", c.what, len(got), len(keys))
		}
		for _, r := range st.Log() {
			if r.In.Delimiter == "" && r.In.MaxResults != c.want {
				t.Fatalf("%s: a request for %d, want %d", c.what, r.In.MaxResults, c.want)
			}
		}
	}
}
