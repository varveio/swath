// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// script is a provider that breaks its ordering contract on purpose: the
// root emits its rows, then a child, then more rows; the child's rows are
// given. Every range is ascending on its own.
type script struct {
	root, child, after []string
}

func (s script) Root() fr { return fr{-1} }

func (s script) Expand(_ context.Context, r fr, e source.Emitter[fr, tp]) error {
	rows := func(keys []string) error {
		if len(keys) == 0 {
			return nil
		}
		out := make([]schema.Row[tp], len(keys))
		for i, k := range keys {
			out[i].Key = k
		}
		return e.Rows(out)
	}
	if r.i == 0 {
		return rows(s.child)
	}
	if err := rows(s.root); err != nil {
		return err
	}
	if s.child != nil {
		if err := e.Child(fr{0}); err != nil {
			return err
		}
	}
	return rows(s.after)
}

// TestOrderGuard: the stream-wide guard refuses a key that descends or
// repeats across a range boundary (each range ascending on its own) and a
// key at or below a resume's last published key, before the sink sees it.
// Without the guard the order stage handed such keys on to any sink.
func TestOrderGuard(t *testing.T) {
	k := func(s string) *string { return &s }
	for _, tc := range []struct {
		name  string
		p     script
		after *string
		want  string // the error names the row
	}{
		{"descent", script{root: []string{"b", "c"}, child: []string{"a"}, after: []string{"d"}}, nil, `row 2 key "a" descends below the previous row "c"`},
		{"repeat", script{root: []string{"b", "c"}, child: []string{"c", "d"}, after: []string{"e"}}, nil, `row 2 key "c" repeats the previous row "c"`},
		{"after the child", script{root: []string{"b"}, child: []string{"c", "f"}, after: []string{"e"}}, nil, `row 3 key "e" descends below the previous row "f"`},
		{"resume seam", script{root: []string{"m", "n"}}, k("m"), `row 0 key "m" repeats the last published key (resume) "m"`},
		{"below the seam", script{root: []string{"a", "z"}}, k("m"), `row 0 key "a" descends below the last published key (resume) "m"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, workers := range []int{1, 4} {
				c := &collect{}
				_, err := Run[fr, tp](context.Background(), tc.p, c, &testSchema, Options{Workers: workers, Budget: 1 << 20, After: tc.after})
				var ie *dataset.InvariantError
				if !errors.As(err, &ie) {
					t.Fatalf("workers %d: err = %v, want an invariant error", workers, err)
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("workers %d: %v, want %q", workers, err, tc.want)
				}
				// The sink never sees the bad row (nor, here, its batch).
				if slices.Contains(c.keys, "a") || len(c.keys) > 3 {
					t.Fatalf("workers %d: sink got %q", workers, c.keys)
				}
			}
		})
	}
	// A valid tree passes, with the counts reconciled.
	tr := &tree{depth: 2, fanout: 3, rowsPer: 4}
	st, err := Run[rng, tp](context.Background(), tr, &collect{}, &testSchema, Options{Workers: 3, Budget: 5, After: k("")})
	if err != nil || st.Received != st.Rows || st.Rows != int64(len(tr.keys())) {
		t.Fatalf("valid tree: %+v %v", st, err)
	}
}
